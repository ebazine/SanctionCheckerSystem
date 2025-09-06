#!/usr/bin/env python3
"""
Data Status Service for monitoring sanctions list downloads and versions.
"""

import os
import json
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import requests
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from ..database.manager import DatabaseManager
from ..models.sanctioned_entity import SanctionedEntity
from ..models.custom_sanction_entity import CustomSanctionEntity
from ..services.custom_sanctions_service import CustomSanctionsService
from .data_downloader import DataDownloader
from .data_parser import DataParser
from .data_validator import DataValidator


@dataclass
class DataSourceStatus:
    """Status information for a sanctions data source."""
    source_name: str
    is_downloaded: bool
    last_download: Optional[datetime]
    last_check: Optional[datetime]
    version: Optional[str]
    file_size: int
    entity_count: int
    file_hash: Optional[str]
    needs_update: bool
    error_message: Optional[str]
    download_url: str


@dataclass
class DataStatistics:
    """Statistics for a sanctions data source."""
    source_name: str
    total_entities: int
    individuals: int
    organizations: int
    countries: Dict[str, int]  # country -> count
    entity_types: Dict[str, int]  # type -> count
    date_ranges: Dict[str, int]  # year -> count
    last_updated: Optional[datetime]


class DataStatusService:
    """Service for monitoring and managing sanctions data status."""
    
    def __init__(self, config, db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager
        
        # Initialize database if not already done
        if not self.db_manager.engine:
            self.db_manager.initialize_database()
        # Handle both string and Path objects for data_directory
        if hasattr(config, 'data_directory'):
            data_dir = config.data_directory
            if isinstance(data_dir, str):
                self.data_dir = Path(data_dir)
            else:
                self.data_dir = data_dir  # Already a Path object
        else:
            # Fallback to a default directory
            self.data_dir = Path.home() / ".sanctions_checker" / "data"
        
        self.data_dir.mkdir(exist_ok=True)
        
        # Status cache file
        self.status_file = self.data_dir / "data_status.json"
        
        # Data sources configuration (including custom sanctions)
        self.data_sources = {
            "EU": {
                "name": "European Union Consolidated List",
                "url": "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw",
                "filename": "eu_sanctions.xml",
                "parser_type": "xml",
                "source_type": "external"
            },
            "UN": {
                "name": "United Nations Security Council Sanctions List",
                "url": "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
                "filename": "un_sanctions.xml", 
                "parser_type": "xml",
                "source_type": "external"
            },
            "OFAC": {
                "name": "OFAC Specially Designated Nationals List",
                "url": "https://sanctionslistservice.ofac.treas.gov/api/download/sdn.xml",
                "filename": "ofac_sdn.xml",
                "parser_type": "xml",
                "source_type": "external"
            },
            "CUSTOM": {
                "name": "Custom Sanctions List",
                "url": None,  # No external URL - managed internally
                "filename": None,  # No file - stored in database
                "parser_type": "database",
                "source_type": "internal"
            }
        }
        
        self.downloader = DataDownloader(self.data_dir)
        self.parser = DataParser()
        self.validator = DataValidator(str(self.data_dir))
        
        # Initialize custom sanctions service
        try:
            self.custom_sanctions_service = CustomSanctionsService(self.db_manager)
        except Exception as e:
            logger.warning(f"Could not initialize custom sanctions service: {e}")
            self.custom_sanctions_service = None
    
    def get_all_status(self) -> Dict[str, DataSourceStatus]:
        """Get status for all configured data sources."""
        status_dict = {}
        
        for source_id, source_config in self.data_sources.items():
            status_dict[source_id] = self.get_source_status(source_id)
        
        return status_dict
    
    def get_source_status(self, source_id: str) -> DataSourceStatus:
        """Get detailed status for a specific data source."""
        if source_id not in self.data_sources:
            raise ValueError(f"Unknown data source: {source_id}")
        
        source_config = self.data_sources[source_id]
        
        # Handle custom sanctions differently
        if source_id == "CUSTOM":
            return self._get_custom_sanctions_status(source_config)
        
        file_path = self.data_dir / source_config["filename"]
        
        # Initialize status
        status = DataSourceStatus(
            source_name=source_config["name"],
            is_downloaded=file_path.exists(),
            last_download=None,
            last_check=datetime.now(),
            version=None,
            file_size=0,
            entity_count=0,
            file_hash=None,
            needs_update=True,
            error_message=None,
            download_url=source_config["url"]
        )
        
        # Load cached status if available
        cached_status = self._load_cached_status(source_id)
        if cached_status:
            status.last_download = cached_status.get("last_download")
            if status.last_download:
                status.last_download = datetime.fromisoformat(status.last_download)
            status.version = cached_status.get("version")
        
        # Generate version from download date if not available
        if not status.version and status.last_download:
            status.version = status.last_download.strftime("%Y-%m-%d %H:%M")
        
        # Check file if it exists
        if file_path.exists():
            try:
                stat = file_path.stat()
                status.file_size = stat.st_size
                status.last_download = datetime.fromtimestamp(stat.st_mtime)
                status.is_downloaded = True
                
                # Calculate file hash
                status.file_hash = self._calculate_file_hash(file_path)
                
                # Count entities in database
                status.entity_count = self._count_entities(source_id)
                
                # Check if update is needed (older than 24 hours)
                if status.last_download:
                    age = datetime.now() - status.last_download
                    status.needs_update = age > timedelta(hours=24)
                
            except Exception as e:
                status.error_message = f"Error reading file: {str(e)}"
        else:
            # Also check for timestamped files
            pattern = f"{source_id.lower()}_sanctions_*.xml"
            import glob
            matching_files = glob.glob(str(self.data_dir / pattern))
            if matching_files:
                # Use the most recent file
                latest_file = max(matching_files, key=os.path.getmtime)
                latest_path = Path(latest_file)
                
                stat = latest_path.stat()
                status.file_size = stat.st_size
                status.last_download = datetime.fromtimestamp(stat.st_mtime)
                status.is_downloaded = True
                status.file_hash = self._calculate_file_hash(latest_path)
                status.entity_count = self._count_entities(source_id)
                
                if status.last_download:
                    age = datetime.now() - status.last_download
                    status.needs_update = age > timedelta(hours=24)
        
        # Save status to cache
        self._save_cached_status(source_id, status)
        
        return status
    
    def _get_custom_sanctions_status(self, source_config: Dict) -> DataSourceStatus:
        """Get status for custom sanctions (database-based)."""
        status = DataSourceStatus(
            source_name=source_config["name"],
            is_downloaded=True,  # Always "downloaded" since it's in database
            last_download=None,
            last_check=datetime.now(),
            version=None,
            file_size=0,  # Not applicable for database
            entity_count=0,
            file_hash=None,  # Not applicable for database
            needs_update=False,  # Custom sanctions are always current
            error_message=None,
            download_url=None  # No external URL
        )
        
        if self.custom_sanctions_service:
            try:
                # Get custom sanctions statistics
                try:
                    stats = self.custom_sanctions_service.get_statistics()
                    status.entity_count = stats.get('total_entities', 0)
                except Exception as e:
                    logger.debug(f"Error getting statistics: {e}")
                    status.entity_count = 0
                
                # Get the most recent entity to determine last update
                try:
                    entities = self.custom_sanctions_service.list_sanction_entities(limit=1)
                    if entities:
                        # Use the most recent entity's creation/update time
                        latest_entity = entities[0]
                        try:
                            if hasattr(latest_entity, 'last_updated') and latest_entity.last_updated:
                                status.last_download = latest_entity.last_updated
                            elif hasattr(latest_entity, 'created_at') and latest_entity.created_at:
                                status.last_download = latest_entity.created_at
                        except (AttributeError, ValueError, TypeError) as e:
                            logger.debug(f"Error processing entity date: {e}")
                            # Continue without setting last_download
                except Exception as e:
                    logger.debug(f"Error listing entities: {e}")
                    # Continue without setting last_download
                
                # Set version based on entity count and last update
                try:
                    if status.last_download:
                        # Safely format the date
                        try:
                            if hasattr(status.last_download, 'strftime'):
                                date_str = status.last_download.strftime('%Y-%m-%d')
                                status.version = f"v{status.entity_count} ({date_str})"
                            else:
                                status.version = f"v{status.entity_count} ({str(status.last_download)[:10]})"
                        except Exception as e:
                            logger.debug(f"Error formatting date: {e}")
                            status.version = f"v{status.entity_count}"
                    else:
                        status.version = f"v{status.entity_count}"
                except Exception as e:
                    logger.debug(f"Error formatting version string: {e}")
                    status.version = f"v{status.entity_count}"
                    
            except Exception as e:
                status.error_message = f"Error accessing custom sanctions: {str(e)}"
                logger.debug(f"Error getting custom sanctions status: {e}")  # Changed to debug to reduce noise
        else:
            status.error_message = "Custom sanctions service not available"
        
        return status
    
    def check_for_updates(self, source_id: str) -> bool:
        """Check if a data source has updates available."""
        if source_id not in self.data_sources:
            return False
        
        # Custom sanctions are always current - no updates needed
        if source_id == "CUSTOM":
            return False
        
        try:
            source_config = self.data_sources[source_id]
            
            # Make HEAD request to check last-modified
            response = requests.head(source_config["url"], timeout=10)
            response.raise_for_status()
            
            # Get remote last-modified date
            last_modified = response.headers.get("Last-Modified")
            if last_modified:
                from email.utils import parsedate_to_datetime
                remote_date = parsedate_to_datetime(last_modified)
                
                # Compare with local file
                local_file = self.data_dir / source_config["filename"]
                if local_file.exists():
                    local_date = datetime.fromtimestamp(local_file.stat().st_mtime)
                    return remote_date > local_date
                else:
                    return True  # No local file, update needed
            
            # If no last-modified header, check based on age
            status = self.get_source_status(source_id)
            return status.needs_update
            
        except Exception as e:
            # Use graceful error handling instead of printing scary errors
            self._handle_update_check_error(source_id, e)
            return False
    
    def download_source(self, source_id: str, force: bool = False) -> bool:
        """Download and process a specific data source."""
        if source_id not in self.data_sources:
            return False
        
        # Custom sanctions don't need downloading - they're managed internally
        if source_id == "CUSTOM":
            logger.info("Custom sanctions are managed internally - no download needed")
            return True
        
        try:
            source_config = self.data_sources[source_id]
            
            # Check if update is needed
            if not force and not self.check_for_updates(source_id):
                return True  # Already up to date
            
            # Download the data using the downloader's graceful error handling
            try:
                # Use download_all_sources for a single source to get graceful error handling
                download_results = self.downloader.download_all_sources()
                download_result = download_results.get(source_id, {})
                
                if not download_result.get('success', False):
                    # Handle the error gracefully - the downloader already logged user-friendly messages
                    return False
                
                # Get the downloaded file path
                file_path = download_result.get('file_path')
                if not file_path:
                    return False
                    
            except Exception as e:
                # Use graceful error handling instead of technical errors
                self._handle_download_error(source_id, e)
                return False
            
            # Validate the downloaded file (basic validation - file exists and has content)
            if not file_path or not Path(file_path).exists():
                return False
            
            file_size = Path(file_path).stat().st_size
            if file_size == 0:
                return False
            
            # Parse and store in database
            source_config = self.data_sources[source_id]
            file_format = source_config.get("parser_type", "xml")
            entities = self.parser.parse_file(file_path, source_id, file_format)
            if entities:
                self._store_entities(source_id, entities)
            
            # Update cached status
            status = self.get_source_status(source_id)
            self._save_cached_status(source_id, status)
            
            return True
            
        except Exception as e:
            # Use graceful error handling instead of technical errors
            self._handle_download_error(source_id, e)
            return False
    
    def download_all_sources(self, force: bool = False) -> Dict[str, bool]:
        """Download all configured data sources."""
        results = {}
        
        for source_id in self.data_sources.keys():
            results[source_id] = self.download_source(source_id, force)
        
        return results
    
    def get_statistics(self, source_id: str) -> Optional[DataStatistics]:
        """Get detailed statistics for a data source."""
        if source_id not in self.data_sources:
            return None
        
        # Handle custom sanctions differently
        if source_id == "CUSTOM":
            return self._get_custom_sanctions_statistics()
        
        try:
            with self.db_manager.get_session() as session:
                # Get all entities for this source
                entities = session.query(SanctionedEntity).filter(
                    SanctionedEntity.source == source_id
                ).all()
                
                if not entities:
                    return DataStatistics(
                        source_name=self.data_sources[source_id]["name"],
                        total_entities=0,
                        individuals=0,
                        organizations=0,
                        countries={},
                        entity_types={},
                        date_ranges={},
                        last_updated=None
                    )
                
                # Calculate statistics
                total_entities = len(entities)
                individuals = sum(1 for e in entities if e.entity_type.upper() == "INDIVIDUAL")
                organizations = sum(1 for e in entities if e.entity_type.upper() == "ORGANIZATION")
                
                # Country statistics (extract from additional_info if available)
                countries = {}
                for entity in entities:
                    # Try to get country from additional_info or use source as fallback
                    country = None
                    if hasattr(entity, 'additional_info') and entity.additional_info:
                        country = entity.additional_info.get('country')
                    if not country and hasattr(entity, 'source'):
                        country = entity.source  # Use source as country grouping
                    if country:
                        countries[country] = countries.get(country, 0) + 1
                
                # Entity type statistics
                entity_types = {}
                for entity in entities:
                    if entity.entity_type:
                        entity_types[entity.entity_type] = entity_types.get(entity.entity_type, 0) + 1
                
                # Date range statistics (by year) - use created_at instead of date_added
                date_ranges = {}
                for entity in entities:
                    if hasattr(entity, 'created_at') and entity.created_at:
                        year = str(entity.created_at.year)
                        date_ranges[year] = date_ranges.get(year, 0) + 1
                
                # Get last update time
                last_updated = max((e.created_at for e in entities if hasattr(e, 'created_at') and e.created_at), default=None)
                
                return DataStatistics(
                    source_name=self.data_sources[source_id]["name"],
                    total_entities=total_entities,
                    individuals=individuals,
                    organizations=organizations,
                    countries=countries,
                    entity_types=entity_types,
                    date_ranges=date_ranges,
                    last_updated=last_updated
                )
                
        except Exception as e:
            print(f"Error getting statistics for {source_id}: {e}")
            return None
    
    def get_all_statistics(self) -> Dict[str, DataStatistics]:
        """Get statistics for all data sources."""
        stats = {}
        
        for source_id in self.data_sources.keys():
            stat = self.get_statistics(source_id)
            if stat:
                stats[source_id] = stat
        
        return stats
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA-256 hash of a file."""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _count_entities(self, source_id: str) -> int:
        """Count entities for a source in the database."""
        try:
            if source_id == "CUSTOM":
                # Count custom sanctions entities
                if self.custom_sanctions_service:
                    stats = self.custom_sanctions_service.get_statistics()
                    return stats.get('total_entities', 0)
                return 0
            else:
                # Count regular sanctioned entities
                with self.db_manager.get_session() as session:
                    return session.query(SanctionedEntity).filter(
                        SanctionedEntity.source == source_id
                    ).count()
        except Exception:
            return 0
    
    def _store_entities(self, source_id: str, entities: List[Dict]):
        """Store entities in the database, replacing existing ones for this source."""
        try:
            with self.db_manager.get_session() as session:
                # Delete existing entities for this source
                session.query(SanctionedEntity).filter(
                    SanctionedEntity.source == source_id
                ).delete()
                
                # Add new entities
                for entity_data in entities:
                    # Create SanctionedEntity from dictionary
                    entity = SanctionedEntity(
                        name=entity_data.get('name', '').strip(),
                        aliases=entity_data.get('aliases', []),
                        entity_type=entity_data.get('entity_type', 'UNKNOWN'),
                        sanctions_type=entity_data.get('sanctions_type', ''),
                        effective_date=entity_data.get('effective_date'),
                        source=source_id,
                        source_version=entity_data.get('source_version', '1.0'),
                        additional_info=entity_data.get('additional_info', {})
                    )
                    session.add(entity)
                
                session.commit()
                logger.info(f"Successfully stored {len(entities)} entities for {source_id}")
                
        except Exception as e:
            logger.error(f"Error storing entities for {source_id}: {e}")
            print(f"Error storing entities for {source_id}: {e}")
    
    def _load_cached_status(self, source_id: str) -> Optional[Dict]:
        """Load cached status from file."""
        try:
            if self.status_file.exists():
                with open(self.status_file, 'r') as f:
                    all_status = json.load(f)
                    return all_status.get(source_id)
        except Exception:
            pass
        return None
    
    def _save_cached_status(self, source_id: str, status: DataSourceStatus):
        """Save status to cache file."""
        try:
            # Load existing status
            all_status = {}
            if self.status_file.exists():
                with open(self.status_file, 'r') as f:
                    all_status = json.load(f)
            
            # Convert status to dict
            status_dict = asdict(status)
            # Convert datetime objects to ISO strings
            for key, value in status_dict.items():
                if isinstance(value, datetime):
                    status_dict[key] = value.isoformat()
            
            # Update and save
            all_status[source_id] = status_dict
            with open(self.status_file, 'w') as f:
                json.dump(all_status, f, indent=2)
                
        except Exception as e:
            print(f"Error saving cached status: {e}")
    
    def _handle_update_check_error(self, source_id: str, error: Exception):
        """Handle update check errors gracefully with user-friendly messages."""
        error_str = str(error).lower()
        
        if '404' in error_str or 'not found' in error_str:
            if source_id == 'UN':
                logger.warning(f"UN sanctions list is currently unavailable (404 error)")
                logger.info(f"Suggestion: The UN has changed their API. Visit https://www.un.org/securitycouncil/sanctions/ for manual updates.")
            elif source_id == 'OFAC':
                logger.warning(f"OFAC sanctions list endpoint not found (404 error)")
                logger.info(f"Suggestion: The OFAC API has changed. Visit https://home.treasury.gov/policy-issues/financial-sanctions/ for manual updates.")
            else:
                logger.warning(f"{source_id} sanctions list is currently unavailable (404 error)")
                
        elif '405' in error_str or 'method not allowed' in error_str:
            if source_id == 'OFAC':
                logger.warning(f"OFAC API method not allowed (405 error)")
                logger.info(f"Suggestion: The OFAC API has changed their access method. Visit https://home.treasury.gov/policy-issues/financial-sanctions/ for manual updates.")
            else:
                logger.warning(f"{source_id} API method not allowed (405 error)")
                
        elif '403' in error_str or 'forbidden' in error_str:
            if source_id == 'UN':
                logger.warning(f"UN blocks automated access to sanctions data (403 Forbidden)")
                logger.info(f"Suggestion: The UN now requires manual download. Visit https://www.un.org/securitycouncil/sanctions/ to download the consolidated list.")
            else:
                logger.warning(f"{source_id} blocks automated access (403 Forbidden)")
        else:
            logger.warning(f"{source_id} update check failed: Network or server error")
            logger.info(f"Suggestion: The {source_id} service may be temporarily unavailable. Try again later.")
    
    def _handle_download_error(self, source_id: str, error: Exception):
        """Handle download errors gracefully with user-friendly messages."""
        error_str = str(error).lower()
        
        if '404' in error_str or 'not found' in error_str:
            if source_id == 'UN':
                logger.warning(f"UN sanctions list download failed (404 error)")
                logger.info(f"Impact: UN sanctions screening will use cached data if available.")
                logger.info(f"User Action: Download the UN consolidated list XML file manually and import it through Settings → Data Management.")
            elif source_id == 'OFAC':
                logger.warning(f"OFAC sanctions list download failed (404 error)")
                logger.info(f"Impact: OFAC sanctions screening will use cached data if available.")
                logger.info(f"User Action: Download the OFAC SDN list manually and import it through Settings → Data Management.")
            else:
                logger.warning(f"{source_id} sanctions list download failed (404 error)")
                
        elif '405' in error_str or 'method not allowed' in error_str:
            if source_id == 'OFAC':
                logger.warning(f"OFAC download failed - API method not allowed (405 error)")
                logger.info(f"Impact: OFAC sanctions screening will use cached data if available.")
                logger.info(f"User Action: Download the OFAC SDN list manually and import it through Settings → Data Management.")
            else:
                logger.warning(f"{source_id} download failed - API method not allowed (405 error)")
                
        elif '403' in error_str or 'forbidden' in error_str:
            if source_id == 'UN':
                logger.warning(f"UN download failed - automated access blocked (403 Forbidden)")
                logger.info(f"Impact: UN sanctions screening will use cached data if available.")
                logger.info(f"User Action: Download the UN consolidated list XML file manually and import it through Settings → Data Management.")
            else:
                logger.warning(f"{source_id} download failed - automated access blocked (403 Forbidden)")
        else:
            logger.warning(f"{source_id} download failed: Network or server error")
            logger.info(f"Impact: {source_id} sanctions screening will use cached data if available.")
    
    def _get_custom_sanctions_statistics(self) -> Optional[DataStatistics]:
        """Get detailed statistics for custom sanctions."""
        if not self.custom_sanctions_service:
            return DataStatistics(
                source_name="Custom Sanctions List",
                total_entities=0,
                individuals=0,
                organizations=0,
                countries={},
                entity_types={},
                date_ranges={},
                last_updated=None
            )
        
        try:
            # Get basic statistics from custom sanctions service
            try:
                stats = self.custom_sanctions_service.get_statistics()
            except Exception as e:
                logger.debug(f"Error getting basic statistics: {e}")
                stats = {'total_entities': 0}
            
            # Get entities for detailed analysis
            try:
                entities = self.custom_sanctions_service.list_sanction_entities(limit=1000)  # Reasonable limit
            except Exception as e:
                logger.debug(f"Error listing entities: {e}")
                entities = []
            
            # Calculate detailed statistics
            individuals = 0
            organizations = 0
            countries = {}
            entity_types = {}
            date_ranges = {}
            last_updated = None
            
            for entity in entities:
                try:
                    # Count by subject type
                    if hasattr(entity, 'subject_type') and entity.subject_type:
                        try:
                            subject_type = entity.subject_type.value if hasattr(entity.subject_type, 'value') else str(entity.subject_type)
                            entity_types[subject_type] = entity_types.get(subject_type, 0) + 1
                            
                            if subject_type.lower() in ['individual', 'person']:
                                individuals += 1
                            elif subject_type.lower() in ['entity', 'organization', 'company']:
                                organizations += 1
                        except Exception as e:
                            logger.debug(f"Error processing subject type: {e}")
                    
                    # Extract countries from addresses or other fields
                    try:
                        if hasattr(entity, 'addresses') and entity.addresses:
                            for address in entity.addresses:
                                if hasattr(address, 'country') and address.country:
                                    country = address.country
                                    countries[country] = countries.get(country, 0) + 1
                    except Exception as e:
                        logger.debug(f"Error processing addresses: {e}")
                    
                    # Get sanctioning authority as a "country" grouping
                    try:
                        if hasattr(entity, 'sanctioning_authority') and entity.sanctioning_authority:
                            authority = entity.sanctioning_authority
                            countries[authority] = countries.get(authority, 0) + 1
                    except Exception as e:
                        logger.debug(f"Error processing sanctioning authority: {e}")
                    
                    # Date ranges by year - handle both datetime and date objects safely
                    try:
                        entity_date = None
                        if hasattr(entity, 'created_at') and entity.created_at:
                            entity_date = entity.created_at
                        elif hasattr(entity, 'listing_date') and entity.listing_date:
                            entity_date = entity.listing_date
                        
                        if entity_date:
                            try:
                                # Handle both datetime and date objects
                                if hasattr(entity_date, 'year'):
                                    year = str(entity_date.year)
                                    date_ranges[year] = date_ranges.get(year, 0) + 1
                                    
                                    # Track latest update - ensure we're comparing compatible types
                                    if not last_updated:
                                        last_updated = entity_date
                                    elif hasattr(entity_date, 'year') and hasattr(last_updated, 'year'):
                                        try:
                                            # Convert both to datetime for comparison if needed
                                            if hasattr(entity_date, 'date') and not hasattr(last_updated, 'date'):
                                                # entity_date is datetime, last_updated is date
                                                if entity_date.date() > last_updated:
                                                    last_updated = entity_date
                                            elif not hasattr(entity_date, 'date') and hasattr(last_updated, 'date'):
                                                # entity_date is date, last_updated is datetime
                                                if entity_date > last_updated.date():
                                                    last_updated = entity_date
                                            else:
                                                # Both are same type
                                                if entity_date > last_updated:
                                                    last_updated = entity_date
                                        except (TypeError, AttributeError, ValueError) as e:
                                            # If comparison fails, just use the newer one by year
                                            try:
                                                if entity_date.year > last_updated.year:
                                                    last_updated = entity_date
                                            except Exception:
                                                # If even year comparison fails, skip
                                                pass
                            except (AttributeError, ValueError, TypeError) as e:
                                # Skip invalid dates
                                logger.debug(f"Skipping invalid date for entity: {e}")
                                pass  # Continue processing other entities
                    except Exception as e:
                        logger.debug(f"Error processing entity dates: {e}")
                        
                except Exception as e:
                    logger.debug(f"Error processing entity: {e}")
                    continue  # Skip this entity and continue with the next
            
            return DataStatistics(
                source_name="Custom Sanctions List",
                total_entities=stats.get('total_entities', len(entities)),
                individuals=individuals,
                organizations=organizations,
                countries=countries,
                entity_types=entity_types,
                date_ranges=date_ranges,
                last_updated=last_updated
            )
            
        except Exception as e:
            logger.error(f"Error getting custom sanctions statistics: {e}")
            # Return empty statistics instead of None to prevent further errors
            return DataStatistics(
                source_name="Custom Sanctions List",
                total_entities=0,
                individuals=0,
                organizations=0,
                countries={},
                entity_types={},
                date_ranges={},
                last_updated=None
            )