"""
Data validation module for sanctions data integrity checking.
"""
import hashlib
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates sanctions data integrity and manages data versions."""
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize the data validator.
        
        Args:
            data_dir: Directory containing data files
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.metadata_file = self.data_dir / "metadata.json"
    
    def validate_download_integrity(self, file_path: str, expected_hash: str, 
                                  expected_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Validate the integrity of a downloaded file.
        
        Args:
            file_path: Path to the file to validate
            expected_hash: Expected SHA-256 hash
            expected_size: Expected file size in bytes (optional)
            
        Returns:
            Validation result dictionary
        """
        result = {
            'valid': False,
            'file_path': file_path,
            'expected_hash': expected_hash,
            'actual_hash': None,
            'expected_size': expected_size,
            'actual_size': None,
            'errors': [],
            'warnings': []
        }
        
        try:
            file_path_obj = Path(file_path)
            
            # Check if file exists
            if not file_path_obj.exists():
                result['errors'].append(f"File does not exist: {file_path}")
                return result
            
            # Get actual file size
            result['actual_size'] = file_path_obj.stat().st_size
            
            # Check file size if expected size is provided
            if expected_size is not None and result['actual_size'] != expected_size:
                result['errors'].append(
                    f"File size mismatch: expected {expected_size}, got {result['actual_size']}"
                )
            
            # Calculate actual hash
            with open(file_path, 'rb') as f:
                content = f.read()
                result['actual_hash'] = hashlib.sha256(content).hexdigest()
            
            # Verify hash
            if result['actual_hash'] != expected_hash:
                result['errors'].append(
                    f"Hash mismatch: expected {expected_hash}, got {result['actual_hash']}"
                )
            
            # Check if file is empty
            if result['actual_size'] == 0:
                result['errors'].append("File is empty")
            
            # Check if file is suspiciously small
            elif result['actual_size'] < 1024:  # Less than 1KB
                result['warnings'].append(f"File is very small ({result['actual_size']} bytes)")
            
            result['valid'] = len(result['errors']) == 0
            
        except Exception as e:
            result['errors'].append(f"Validation error: {str(e)}")
            logger.error(f"Error validating file {file_path}: {e}")
        
        return result
    
    def validate_parsed_entities(self, entities: List[Dict[str, Any]], 
                                source_name: str) -> Dict[str, Any]:
        """
        Validate parsed entity data for completeness and consistency.
        
        Args:
            entities: List of parsed entities
            source_name: Name of the data source
            
        Returns:
            Validation result dictionary
        """
        result = {
            'source': source_name,
            'total_entities': len(entities),
            'valid_entities': 0,
            'invalid_entities': 0,
            'duplicate_entities': 0,
            'errors': [],
            'warnings': [],
            'statistics': {
                'entity_types': {},
                'sanctions_types': {},
                'entities_with_aliases': 0,
                'entities_with_dates': 0
            }
        }
        
        if not entities:
            result['errors'].append("No entities found in parsed data")
            return result
        
        required_fields = ['source', 'entity_type', 'name', 'sanctions_type']
        seen_names = set()
        
        for i, entity in enumerate(entities):
            entity_valid = True
            entity_id = f"Entity {i+1}"
            
            # Check required fields
            for field in required_fields:
                if field not in entity:
                    result['errors'].append(f"{entity_id}: Missing field '{field}'")
                    entity_valid = False
                elif not entity[field] or (isinstance(entity[field], str) and not entity[field].strip()):
                    result['errors'].append(f"{entity_id}: Empty field '{field}'")
                    entity_valid = False
            
            if not entity_valid:
                result['invalid_entities'] += 1
                continue
            
            # Validate name
            name = entity['name'].strip()
            if len(name) < 2:
                result['warnings'].append(f"{entity_id}: Name too short: '{name}'")
            elif len(name) > 500:
                result['warnings'].append(f"{entity_id}: Name very long ({len(name)} chars): '{name[:50]}...'")
            
            # Check for duplicates
            name_lower = name.lower()
            if name_lower in seen_names:
                result['duplicate_entities'] += 1
                result['warnings'].append(f"{entity_id}: Duplicate name: '{name}'")
            else:
                seen_names.add(name_lower)
            
            # Validate entity type
            entity_type = entity['entity_type']
            if entity_type not in result['statistics']['entity_types']:
                result['statistics']['entity_types'][entity_type] = 0
            result['statistics']['entity_types'][entity_type] += 1
            
            # Validate sanctions type
            sanctions_type = entity['sanctions_type']
            if sanctions_type not in result['statistics']['sanctions_types']:
                result['statistics']['sanctions_types'][sanctions_type] = 0
            result['statistics']['sanctions_types'][sanctions_type] += 1
            
            # Check aliases
            if entity.get('aliases') and len(entity['aliases']) > 0:
                result['statistics']['entities_with_aliases'] += 1
                
                # Validate aliases
                for alias in entity['aliases']:
                    if not isinstance(alias, str) or not alias.strip():
                        result['warnings'].append(f"{entity_id}: Invalid alias: {alias}")
            
            # Check effective date
            if entity.get('effective_date'):
                result['statistics']['entities_with_dates'] += 1
                
                # Validate date
                if isinstance(entity['effective_date'], datetime):
                    # Check if date is in the future
                    if entity['effective_date'] > datetime.now():
                        result['warnings'].append(
                            f"{entity_id}: Effective date in future: {entity['effective_date']}"
                        )
                    # Check if date is very old (more than 50 years ago)
                    elif entity['effective_date'] < datetime.now() - timedelta(days=50*365):
                        result['warnings'].append(
                            f"{entity_id}: Very old effective date: {entity['effective_date']}"
                        )
            
            # Validate additional info
            if entity.get('additional_info') and not isinstance(entity['additional_info'], dict):
                result['warnings'].append(f"{entity_id}: additional_info should be a dictionary")
            
            result['valid_entities'] += 1
        
        # Calculate percentages
        if result['total_entities'] > 0:
            result['statistics']['valid_percentage'] = (result['valid_entities'] / result['total_entities']) * 100
            result['statistics']['duplicate_percentage'] = (result['duplicate_entities'] / result['total_entities']) * 100
        
        return result
    
    def check_data_freshness(self, source_name: str, max_age_hours: int = 24) -> Dict[str, Any]:
        """
        Check if data for a source is fresh enough.
        
        Args:
            source_name: Name of the data source
            max_age_hours: Maximum age in hours before data is considered stale
            
        Returns:
            Freshness check result
        """
        result = {
            'source': source_name,
            'is_fresh': False,
            'age_hours': None,
            'last_update': None,
            'max_age_hours': max_age_hours,
            'message': ''
        }
        
        try:
            metadata = self.load_metadata()
            
            if source_name not in metadata:
                result['message'] = f"No metadata found for source {source_name}"
                return result
            
            source_metadata = metadata[source_name]
            last_update_str = source_metadata.get('last_update')
            
            if not last_update_str:
                result['message'] = f"No last update timestamp for source {source_name}"
                return result
            
            # Parse timestamp
            last_update = datetime.fromisoformat(last_update_str.replace('Z', '+00:00'))
            result['last_update'] = last_update
            
            # Calculate age
            age = datetime.now() - last_update.replace(tzinfo=None)
            result['age_hours'] = age.total_seconds() / 3600
            
            # Check freshness
            result['is_fresh'] = result['age_hours'] <= max_age_hours
            
            if result['is_fresh']:
                result['message'] = f"Data is fresh ({result['age_hours']:.1f} hours old)"
            else:
                result['message'] = f"Data is stale ({result['age_hours']:.1f} hours old, max {max_age_hours})"
            
        except Exception as e:
            result['message'] = f"Error checking data freshness: {str(e)}"
            logger.error(f"Error checking data freshness for {source_name}: {e}")
        
        return result
    
    def save_metadata(self, source_name: str, metadata: Dict[str, Any]):
        """
        Save metadata for a data source.
        
        Args:
            source_name: Name of the data source
            metadata: Metadata dictionary to save
        """
        try:
            # Load existing metadata
            all_metadata = self.load_metadata()
            
            # Update metadata for this source
            all_metadata[source_name] = {
                **metadata,
                'last_update': datetime.now().isoformat(),
                'validator_version': '1.0'
            }
            
            # Save to file
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(all_metadata, f, indent=2, default=str)
            
            logger.info(f"Saved metadata for source {source_name}")
            
        except Exception as e:
            logger.error(f"Error saving metadata for {source_name}: {e}")
            raise
    
    def load_metadata(self) -> Dict[str, Any]:
        """
        Load metadata for all data sources.
        
        Returns:
            Dictionary containing metadata for all sources
        """
        try:
            if self.metadata_file.exists():
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {}
        except Exception as e:
            logger.error(f"Error loading metadata: {e}")
            return {}
    
    def validate_data_consistency(self, entities_by_source: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Validate consistency across multiple data sources.
        
        Args:
            entities_by_source: Dictionary mapping source names to entity lists
            
        Returns:
            Consistency validation result
        """
        result = {
            'total_sources': len(entities_by_source),
            'total_entities': sum(len(entities) for entities in entities_by_source.values()),
            'cross_source_duplicates': [],
            'source_statistics': {},
            'warnings': [],
            'errors': []
        }
        
        if not entities_by_source:
            result['errors'].append("No data sources provided")
            return result
        
        # Build name index across all sources
        name_to_sources = {}
        
        for source_name, entities in entities_by_source.items():
            result['source_statistics'][source_name] = {
                'entity_count': len(entities),
                'unique_names': set(),
                'entity_types': set(),
                'sanctions_types': set()
            }
            
            for entity in entities:
                name = entity.get('name', '').strip().lower()
                if name:
                    result['source_statistics'][source_name]['unique_names'].add(name)
                    
                    if name not in name_to_sources:
                        name_to_sources[name] = []
                    name_to_sources[name].append(source_name)
                
                # Collect statistics
                if entity.get('entity_type'):
                    result['source_statistics'][source_name]['entity_types'].add(entity['entity_type'])
                
                if entity.get('sanctions_type'):
                    result['source_statistics'][source_name]['sanctions_types'].add(entity['sanctions_type'])
        
        # Find cross-source duplicates
        for name, sources in name_to_sources.items():
            if len(sources) > 1:
                result['cross_source_duplicates'].append({
                    'name': name,
                    'sources': list(set(sources)),  # Remove duplicates
                    'count': len(sources)
                })
        
        # Convert sets to lists for JSON serialization
        for source_stats in result['source_statistics'].values():
            source_stats['unique_names'] = len(source_stats['unique_names'])
            source_stats['entity_types'] = list(source_stats['entity_types'])
            source_stats['sanctions_types'] = list(source_stats['sanctions_types'])
        
        # Generate warnings
        if len(result['cross_source_duplicates']) > 0:
            result['warnings'].append(
                f"Found {len(result['cross_source_duplicates'])} names appearing in multiple sources"
            )
        
        # Check for sources with very different entity counts
        entity_counts = [stats['entity_count'] for stats in result['source_statistics'].values()]
        if len(entity_counts) > 1:
            max_count = max(entity_counts)
            min_count = min(entity_counts)
            if max_count > min_count * 10:  # One source has 10x more entities
                result['warnings'].append(
                    f"Large discrepancy in entity counts: {min_count} to {max_count}"
                )
        
        return result
    
    def generate_validation_report(self, validation_results: List[Dict[str, Any]]) -> str:
        """
        Generate a human-readable validation report.
        
        Args:
            validation_results: List of validation result dictionaries
            
        Returns:
            Formatted validation report string
        """
        report_lines = []
        report_lines.append("=== SANCTIONS DATA VALIDATION REPORT ===")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        for result in validation_results:
            if 'source' in result:
                report_lines.append(f"Source: {result['source']}")
                report_lines.append("-" * 40)
                
                if 'total_entities' in result:
                    report_lines.append(f"Total entities: {result['total_entities']}")
                    report_lines.append(f"Valid entities: {result['valid_entities']}")
                    report_lines.append(f"Invalid entities: {result['invalid_entities']}")
                    
                    if result.get('duplicate_entities', 0) > 0:
                        report_lines.append(f"Duplicate entities: {result['duplicate_entities']}")
                
                if result.get('errors'):
                    report_lines.append(f"\nErrors ({len(result['errors'])}):")
                    for error in result['errors'][:10]:  # Show first 10 errors
                        report_lines.append(f"  - {error}")
                    if len(result['errors']) > 10:
                        report_lines.append(f"  ... and {len(result['errors']) - 10} more errors")
                
                if result.get('warnings'):
                    report_lines.append(f"\nWarnings ({len(result['warnings'])}):")
                    for warning in result['warnings'][:10]:  # Show first 10 warnings
                        report_lines.append(f"  - {warning}")
                    if len(result['warnings']) > 10:
                        report_lines.append(f"  ... and {len(result['warnings']) - 10} more warnings")
                
                report_lines.append("")
        
        return "\n".join(report_lines)