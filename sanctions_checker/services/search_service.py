"""
Search service that orchestrates multiple matching algorithms for sanctions screening.

This module provides the main search functionality that:
- Orchestrates multiple fuzzy matching algorithms
- Implements confidence scoring and result ranking
- Handles search result aggregation and deduplication
- Manages search history and audit trails
"""

import hashlib
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from .fuzzy_matcher import FuzzyMatcher, MatchResult
from .name_normalizer import NameNormalizer
from ..models import SanctionedEntity, SearchRecord, SearchResult
from ..models.custom_sanction_entity import CustomSanctionEntity
from ..database.manager import DatabaseManager
from ..utils.logger import get_logger, get_audit_logger, log_performance, LogContext
from ..utils.error_handler import (
    ValidationError, DatabaseError, ErrorContext, handle_exceptions
)
from ..utils.recovery import RecoveryManager, RetryConfig

logger = get_logger(__name__)
audit_logger = get_audit_logger()


@dataclass
class SearchConfiguration:
    """Configuration for search operations."""
    levenshtein_threshold: float = 0.8
    jaro_winkler_threshold: float = 0.85
    soundex_threshold: float = 1.0
    jaro_prefix_scale: float = 0.1
    minimum_overall_confidence: float = 0.4
    max_results: int = 100
    enable_alias_matching: bool = True
    enable_name_normalization: bool = True
    enable_custom_sanctions: bool = True


@dataclass
class EntityMatch:
    """Represents a match between a search query and a sanctioned entity."""
    entity: Any  # Can be SanctionedEntity or CustomSanctionEntity
    confidence_scores: Dict[str, float]
    match_details: Dict[str, Any]
    overall_confidence: float
    matched_name: str  # Which name (primary or alias) was matched
    source_type: str = "official"  # "official" or "custom"
    
    def __post_init__(self):
        """Calculate overall confidence if not provided."""
        if self.overall_confidence is None or self.overall_confidence == 0.0:
            self.overall_confidence = self._calculate_overall_confidence()
        
        # Determine source type based on entity type
        if hasattr(self.entity, '__tablename__') and self.entity.__tablename__ == 'custom_sanction_entities':
            self.source_type = "custom"
        else:
            self.source_type = "official"
    
    def _calculate_overall_confidence(self) -> float:
        """Calculate weighted overall confidence score."""
        if not self.confidence_scores:
            return 0.0
        
        # Weight algorithms based on their reliability and characteristics
        weights = {
            'levenshtein': 0.4,    # Good for typos and minor variations
            'jaro_winkler': 0.4,   # Good for transpositions and prefix matching
            'soundex': 0.2         # Good for phonetic similarity but less precise
        }
        
        total_score = 0.0
        total_weight = 0.0
        
        for algorithm, score in self.confidence_scores.items():
            weight = weights.get(algorithm, 0.33)  # Default equal weight
            total_score += score * weight
            total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 0.0


class SearchService:
    """
    Main search service that orchestrates sanctions screening operations.
    
    This service provides comprehensive search functionality including:
    - Multi-algorithm fuzzy matching
    - Confidence scoring and ranking
    - Result aggregation and deduplication
    - Search history management
    - Audit trail generation
    """
    
    def __init__(self, database_manager: DatabaseManager, config: Optional[SearchConfiguration] = None):
        """
        Initialize the search service.
        
        Args:
            database_manager: Database manager instance
            config: Search configuration (uses defaults if not provided)
        """
        self.db_manager = database_manager
        self.config = config or SearchConfiguration()
        
        # Load custom sanctions setting from app config if not explicitly provided in config
        if config is None:
            try:
                from ..config import Config
                app_config = Config()
                self.config.enable_custom_sanctions = app_config.get('matching.enable_custom_sanctions', True)
            except Exception as e:
                logger.debug(f"Could not load custom sanctions config: {e}")
                # Keep the default value from SearchConfiguration
        
        # Initialize components
        self.fuzzy_matcher = FuzzyMatcher(
            levenshtein_threshold=self.config.levenshtein_threshold,
            jaro_winkler_threshold=self.config.jaro_winkler_threshold,
            soundex_threshold=self.config.soundex_threshold,
            jaro_prefix_scale=self.config.jaro_prefix_scale
        )
        
        self.name_normalizer = NameNormalizer()
        
        # Initialize recovery manager
        self.recovery_manager = RecoveryManager()
        self.retry_config = RetryConfig(
            max_attempts=3,
            base_delay=0.5,
            max_delay=5.0,
            exponential_base=2.0,
            jitter=True
        )
        
        logger.info("SearchService initialized with configuration: %s", self.config)
    
    @log_performance(logger, "search_entities")
    def search_entities(self, 
                       query: str, 
                       entity_type: Optional[str] = None,
                       user_id: Optional[str] = None,
                       tags: Optional[List[str]] = None) -> Tuple[List[EntityMatch], str]:
        """
        Search for sanctioned entities matching the given query.
        
        Args:
            query: Search query string (name to search for)
            entity_type: Optional filter by entity type (INDIVIDUAL, COMPANY)
            user_id: Optional user identifier for audit trail
            tags: Optional list of tags for categorizing this search
            
        Returns:
            Tuple of (list of EntityMatch objects, search_record_id)
        """
        if not query or not query.strip():
            raise ValueError("Search query cannot be empty")
        
        query = query.strip()
        logger.info(f"Starting search for query: '{query}', entity_type: {entity_type}")
        
        session = self.db_manager.get_session()
        try:
            all_matches = []
            
            # Search official sanctions entities
            entities_query = session.query(SanctionedEntity)
            if entity_type:
                entities_query = entities_query.filter(SanctionedEntity.entity_type == entity_type.upper())
            
            official_entities = entities_query.all()
            logger.info(f"Found {len(official_entities)} official entities to search against")
            
            # Perform matching against official entities
            official_matches = self._match_against_entities(query, official_entities)
            all_matches.extend(official_matches)
            
            # Search custom sanctions entities if enabled
            if self.config.enable_custom_sanctions:
                logger.debug("Searching custom sanctions entities...")
                from sqlalchemy.orm import joinedload
                custom_entities_query = session.query(CustomSanctionEntity).options(
                    joinedload(CustomSanctionEntity.names),
                    joinedload(CustomSanctionEntity.individual_details),
                    joinedload(CustomSanctionEntity.entity_details)
                )
                
                # Filter by entity type if specified
                if entity_type:
                    # Map entity types to subject types
                    subject_type_mapping = {
                        'INDIVIDUAL': 'Individual',
                        'COMPANY': 'Entity',
                        'ENTITY': 'Entity'
                    }
                    subject_type = subject_type_mapping.get(entity_type.upper())
                    if subject_type:
                        from ..models.base import SubjectType
                        custom_entities_query = custom_entities_query.filter(
                            CustomSanctionEntity.subject_type == SubjectType(subject_type)
                        )
                
                # Only search active custom sanctions
                from ..models.base import RecordStatus
                custom_entities_query = custom_entities_query.filter(
                    CustomSanctionEntity.record_status == RecordStatus.ACTIVE
                )
                
                custom_entities = custom_entities_query.all()
                logger.info(f"Found {len(custom_entities)} custom entities to search against")
                
                # Perform matching against custom entities
                logger.debug("Starting custom entity matching...")
                custom_matches = self._match_against_custom_entities(query, custom_entities)
                logger.debug(f"Custom entity matching completed with {len(custom_matches)} matches")
                all_matches.extend(custom_matches)
            
            # Filter by minimum confidence and limit results
            filtered_matches = [
                match for match in all_matches 
                if match.overall_confidence >= self.config.minimum_overall_confidence
            ]
            
            # Sort by confidence (highest first) and limit results
            filtered_matches.sort(key=lambda x: x.overall_confidence, reverse=True)
            filtered_matches = filtered_matches[:self.config.max_results]
            
            # Create search record for audit trail
            search_record_id = self._create_search_record(
                session, query, filtered_matches, user_id, tags
            )
            
            # Expunge entities from session to avoid detached instance errors
            for match in filtered_matches:
                session.expunge(match.entity)
            
            session.commit()
            logger.info(f"Search completed. Found {len(filtered_matches)} matches above threshold "
                       f"({len([m for m in filtered_matches if m.source_type == 'official'])} official, "
                       f"{len([m for m in filtered_matches if m.source_type == 'custom'])} custom)")
            
            return filtered_matches, search_record_id
            
        except Exception as e:
            session.rollback()
            logger.error(f"Search failed: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def _match_against_entities(self, query: str, entities: List[SanctionedEntity]) -> List[EntityMatch]:
        """
        Match query against a list of sanctioned entities.
        
        Args:
            query: Search query string
            entities: List of sanctioned entities to match against
            
        Returns:
            List of EntityMatch objects
        """
        matches = []
        
        # Normalize query if enabled
        normalized_query = query
        if self.config.enable_name_normalization:
            # Try to determine if it's a company or individual name
            # Simple heuristic: if it contains common company suffixes, treat as company
            if self._looks_like_company_name(query):
                normalized_query = self.name_normalizer.normalize_company_name(query)
            else:
                normalized_query = self.name_normalizer.normalize_individual_name(query)
        
        logger.debug(f"Normalized query: '{query}' -> '{normalized_query}'")
        
        for entity in entities:
            # Get all names to match against (primary name + aliases)
            names_to_match = [entity.name]
            if self.config.enable_alias_matching and entity.aliases:
                names_to_match.extend(entity.aliases)
            
            best_match = None
            best_confidence = 0.0
            best_matched_name = None
            
            # Try matching against each name
            for name in names_to_match:
                # Normalize the entity name if enabled
                normalized_name = name
                if self.config.enable_name_normalization:
                    if entity.entity_type == 'COMPANY':
                        normalized_name = self.name_normalizer.normalize_company_name(name)
                    else:
                        normalized_name = self.name_normalizer.normalize_individual_name(name)
                
                # Perform fuzzy matching
                match_results = self.fuzzy_matcher.match_all(normalized_query, normalized_name)
                
                # Calculate overall confidence for this name
                confidence_scores = {alg: result.score for alg, result in match_results.items()}
                
                # Create temporary EntityMatch to calculate confidence
                temp_match = EntityMatch(
                    entity=entity,
                    confidence_scores=confidence_scores,
                    match_details={
                        'original_query': query,
                        'normalized_query': normalized_query,
                        'original_name': name,
                        'normalized_name': normalized_name,
                        'algorithm_results': {alg: result.__dict__ for alg, result in match_results.items()}
                    },
                    overall_confidence=0.0,  # Will be calculated in __post_init__
                    matched_name=name
                )
                
                # Keep the best match for this entity
                if temp_match.overall_confidence > best_confidence:
                    best_confidence = temp_match.overall_confidence
                    best_match = temp_match
                    best_matched_name = name
            
            # Add the best match if it meets minimum threshold
            if best_match and best_confidence >= self.config.minimum_overall_confidence:
                matches.append(best_match)
        
        return matches
    
    def _match_against_custom_entities(self, query: str, entities: List[CustomSanctionEntity]) -> List[EntityMatch]:
        """
        Match query against a list of custom sanctioned entities.
        
        Args:
            query: Search query string
            entities: List of custom sanctioned entities to match against
            
        Returns:
            List of EntityMatch objects
        """
        matches = []
        
        # Normalize query if enabled
        normalized_query = query
        if self.config.enable_name_normalization:
            if self._looks_like_company_name(query):
                normalized_query = self.name_normalizer.normalize_company_name(query)
            else:
                normalized_query = self.name_normalizer.normalize_individual_name(query)
        
        logger.debug(f"Normalized query for custom entities: '{query}' -> '{normalized_query}'")
        
        for i, entity in enumerate(entities):
            if i % 10 == 0:  # Log progress every 10 entities
                logger.debug(f"Processing custom entity {i+1}/{len(entities)}")
            # Get all names to match against
            names_to_match = []
            if entity.names:
                names_to_match = [name.full_name for name in entity.names]
            
            if not names_to_match:
                continue  # Skip entities without names
            
            best_match = None
            best_confidence = 0.0
            best_matched_name = None
            
            # Try matching against each name
            for name in names_to_match:
                # Normalize the entity name if enabled
                normalized_name = name
                if self.config.enable_name_normalization:
                    if entity.subject_type.value == 'Entity':
                        normalized_name = self.name_normalizer.normalize_company_name(name)
                    else:
                        normalized_name = self.name_normalizer.normalize_individual_name(name)
                
                # Perform fuzzy matching
                match_results = self.fuzzy_matcher.match_all(normalized_query, normalized_name)
                
                # Calculate overall confidence for this name
                confidence_scores = {alg: result.score for alg, result in match_results.items()}
                
                # Create temporary EntityMatch to calculate confidence
                temp_match = EntityMatch(
                    entity=entity,
                    confidence_scores=confidence_scores,
                    match_details={
                        'original_query': query,
                        'normalized_query': normalized_query,
                        'original_name': name,
                        'normalized_name': normalized_name,
                        'algorithm_results': {alg: result.__dict__ for alg, result in match_results.items()},
                        'entity_type': 'custom'
                    },
                    overall_confidence=0.0,  # Will be calculated in __post_init__
                    matched_name=name,
                    source_type="custom"
                )
                
                # Keep the best match for this entity
                if temp_match.overall_confidence > best_confidence:
                    best_confidence = temp_match.overall_confidence
                    best_match = temp_match
                    best_matched_name = name
            
            # Add the best match if it meets minimum threshold
            if best_match and best_confidence >= self.config.minimum_overall_confidence:
                matches.append(best_match)
        
        return matches
    
    def _looks_like_company_name(self, name: str) -> bool:
        """
        Simple heuristic to determine if a name looks like a company name.
        
        Args:
            name: Name to analyze
            
        Returns:
            True if it looks like a company name
        """
        company_indicators = [
            'inc', 'corp', 'ltd', 'llc', 'company', 'co', 'gmbh', 'ag', 'sa', 'ab',
            'corporation', 'incorporated', 'limited', 'group', 'holdings', 'enterprises'
        ]
        
        name_lower = name.lower()
        return any(indicator in name_lower for indicator in company_indicators)
    
    def _create_search_record(self, 
                             session: Session, 
                             query: str, 
                             matches: List[EntityMatch],
                             user_id: Optional[str],
                             tags: Optional[List[str]] = None) -> str:
        """
        Create a search record for audit trail purposes.
        
        Args:
            session: Database session
            query: Original search query
            matches: List of matches found
            user_id: Optional user identifier
            tags: Optional list of tags for categorizing this search
            
        Returns:
            Search record ID
        """
        # Get sanctions list versions (simplified - in real implementation would track actual versions)
        sanctions_list_versions = self._get_sanctions_list_versions(session)
        
        # Create timestamp for consistency
        search_timestamp = datetime.utcnow()
        
        # Generate verification hash with the same timestamp
        verification_hash = self._generate_verification_hash(
            query, matches, sanctions_list_versions, search_timestamp
        )
        
        # Create search record
        search_record = SearchRecord(
            search_query=query,
            search_timestamp=search_timestamp,
            user_id=user_id,
            tags=tags or [],
            sanctions_list_versions=sanctions_list_versions,
            verification_hash=verification_hash,
            search_parameters={
                'levenshtein_threshold': self.config.levenshtein_threshold,
                'jaro_winkler_threshold': self.config.jaro_winkler_threshold,
                'soundex_threshold': self.config.soundex_threshold,
                'minimum_overall_confidence': self.config.minimum_overall_confidence,
                'max_results': self.config.max_results,
                'enable_alias_matching': self.config.enable_alias_matching,
                'enable_name_normalization': self.config.enable_name_normalization,
                'enable_custom_sanctions': self.config.enable_custom_sanctions
            }
        )
        
        session.add(search_record)
        session.flush()  # Get the ID
        
        # Create search results
        for match in matches:
            search_result = SearchResult(
                search_record_id=search_record.id,
                entity_id=match.entity.id,
                confidence_scores=match.confidence_scores,
                match_details=match.match_details,
                overall_confidence=match.overall_confidence
            )
            session.add(search_result)
        
        return search_record.id
    
    def _get_sanctions_list_versions(self, session: Session) -> Dict[str, str]:
        """
        Get current versions of sanctions lists in the database.
        
        Args:
            session: Database session
            
        Returns:
            Dictionary mapping source to version
        """
        # Query distinct sources and their latest versions
        results = session.query(
            SanctionedEntity.source,
            SanctionedEntity.source_version
        ).distinct().all()
        
        # Group by source and get the latest version (simplified approach)
        versions = {}
        for source, version in results:
            if source not in versions or version > versions[source]:
                versions[source] = version
        
        return versions
    
    def _generate_verification_hash(self, 
                                   query: str, 
                                   matches: List[EntityMatch],
                                   sanctions_list_versions: Dict[str, str],
                                   timestamp: Optional[datetime] = None) -> str:
        """
        Generate a cryptographic hash for search verification.
        
        Args:
            query: Search query
            matches: List of matches
            sanctions_list_versions: Versions of sanctions lists used
            timestamp: Optional timestamp to use (defaults to current time)
            
        Returns:
            SHA-256 hash string
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
            
        # Create a deterministic string representation of the search
        hash_data = {
            'query': query,
            'timestamp': timestamp.isoformat(),
            'sanctions_versions': sanctions_list_versions,
            'matches': [
                {
                    'entity_id': match.entity.id,
                    'confidence': round(match.overall_confidence, 6),
                    'matched_name': match.matched_name
                }
                for match in matches
            ]
        }
        
        # Convert to string and hash
        hash_string = str(sorted(hash_data.items()))
        return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()
    
    def get_search_history(self, 
                          user_id: Optional[str] = None,
                          limit: int = 100,
                          offset: int = 0) -> List[SearchRecord]:
        """
        Retrieve search history records.
        
        Args:
            user_id: Optional filter by user ID
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of SearchRecord objects
        """
        session = self.db_manager.get_session()
        try:
            from sqlalchemy.orm import joinedload
            query = session.query(SearchRecord).options(
                joinedload(SearchRecord.results)
            )
            
            if user_id:
                query = query.filter(SearchRecord.user_id == user_id)
            
            query = query.order_by(SearchRecord.search_timestamp.desc())
            query = query.offset(offset).limit(limit)
            
            records = query.all()
            
            # Expunge records to avoid detached instance errors
            for record in records:
                # Access the results to load them before expunging
                _ = len(record.results)  # Force loading
                session.expunge(record)
            
            return records
            
        finally:
            self.db_manager.close_session(session)
    
    def get_search_record_with_results(self, search_record_id: str) -> Optional[SearchRecord]:
        """
        Get a search record with all its results.
        
        Args:
            search_record_id: ID of the search record
            
        Returns:
            SearchRecord with results, or None if not found
        """
        session = self.db_manager.get_session()
        try:
            # Eagerly load the results to avoid detached instance errors
            from sqlalchemy.orm import joinedload
            return session.query(SearchRecord).options(
                joinedload(SearchRecord.results).joinedload(SearchResult.entity)
            ).filter(SearchRecord.id == search_record_id).first()
            
        finally:
            self.db_manager.close_session(session)
    
    def verify_search_hash(self, search_record_id: str) -> bool:
        """
        Verify the integrity of a search record using its hash.
        
        Args:
            search_record_id: ID of the search record to verify
            
        Returns:
            True if hash verification passes, False otherwise
        """
        session = self.db_manager.get_session()
        try:
            from sqlalchemy.orm import joinedload
            search_record = session.query(SearchRecord).options(
                joinedload(SearchRecord.results)
            ).filter(SearchRecord.id == search_record_id).first()
            
            if not search_record:
                return False
            
            # Create a deterministic string representation for verification
            # This should match the format used in _generate_verification_hash
            hash_data = {
                'query': search_record.search_query,
                'timestamp': search_record.search_timestamp.isoformat(),
                'sanctions_versions': search_record.sanctions_list_versions,
                'matches': [
                    {
                        'entity_id': result.entity_id,
                        'confidence': round(result.overall_confidence, 6),
                        'matched_name': result.match_details.get('original_name', '')
                    }
                    for result in search_record.results
                ]
            }
            
            # Convert to string and hash
            hash_string = str(sorted(hash_data.items()))
            import hashlib
            expected_hash = hashlib.sha256(hash_string.encode('utf-8')).hexdigest()
            
            return expected_hash == search_record.verification_hash
            
        finally:
            self.db_manager.close_session(session)
    
    def update_search_configuration(self, new_config: SearchConfiguration):
        """
        Update the search configuration.
        
        Args:
            new_config: New search configuration
        """
        self.config = new_config
        
        # Update fuzzy matcher thresholds
        self.fuzzy_matcher.update_thresholds(
            levenshtein_threshold=new_config.levenshtein_threshold,
            jaro_winkler_threshold=new_config.jaro_winkler_threshold,
            soundex_threshold=new_config.soundex_threshold
        )
        
        logger.info("Search configuration updated: %s", new_config)
    
    def get_entity_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the sanctioned entities database.
        
        Returns:
            Dictionary with statistics
        """
        session = self.db_manager.get_session()
        try:
            from sqlalchemy import func
            stats = {}
            
            # Official entities statistics
            stats['total_official_entities'] = session.query(SanctionedEntity).count()
            
            # Entities by type
            entity_types = session.query(
                SanctionedEntity.entity_type,
                func.count(SanctionedEntity.id).label('count')
            ).group_by(SanctionedEntity.entity_type).all()
            
            stats['official_entities_by_type'] = {et[0]: et[1] for et in entity_types}
            
            # Entities by source
            sources = session.query(
                SanctionedEntity.source,
                func.count(SanctionedEntity.id).label('count')
            ).group_by(SanctionedEntity.source).all()
            
            stats['entities_by_source'] = {s[0]: s[1] for s in sources}
            
            # Custom entities statistics
            try:
                stats['total_custom_entities'] = session.query(CustomSanctionEntity).count()
                
                # Custom entities by subject type
                from ..models.base import RecordStatus
                custom_types = session.query(
                    CustomSanctionEntity.subject_type,
                    func.count(CustomSanctionEntity.id).label('count')
                ).filter(
                    CustomSanctionEntity.record_status == RecordStatus.ACTIVE
                ).group_by(CustomSanctionEntity.subject_type).all()
                
                stats['custom_entities_by_type'] = {ct[0].value: ct[1] for ct in custom_types}
                
                # Custom entities by status
                custom_statuses = session.query(
                    CustomSanctionEntity.record_status,
                    func.count(CustomSanctionEntity.id).label('count')
                ).group_by(CustomSanctionEntity.record_status).all()
                
                stats['custom_entities_by_status'] = {cs[0].value: cs[1] for cs in custom_statuses}
                
            except Exception as e:
                logger.debug(f"Could not get custom entities statistics: {e}")
                stats['total_custom_entities'] = 0
                stats['custom_entities_by_type'] = {}
                stats['custom_entities_by_status'] = {}
            
            # Combined totals
            stats['total_entities'] = stats['total_official_entities'] + stats['total_custom_entities']
            
            # Search statistics
            stats['total_searches'] = session.query(SearchRecord).count()
            stats['total_search_results'] = session.query(SearchResult).count()
            
            return stats
            
        finally:
            self.db_manager.close_session(session)