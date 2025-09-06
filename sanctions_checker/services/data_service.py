"""
DataService for managing sanctions data lifecycle, search history, and audit trails.
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_, or_, desc, func, text

from ..database.manager import DatabaseManager
from ..models import SanctionedEntity, SearchRecord, SearchResult
from ..config import Config

logger = logging.getLogger(__name__)


class DataService:
    """
    Service layer for managing sanctions data lifecycle, search history, and audit trails.
    
    This service provides high-level operations for:
    - Managing sanctions data (CRUD operations, bulk operations)
    - Search history storage and retrieval
    - Data versioning and audit trails
    - Database maintenance and cleanup
    """
    
    def __init__(self, database_manager: DatabaseManager = None):
        """
        Initialize the DataService.
        
        Args:
            database_manager: Optional DatabaseManager instance. If not provided, creates a new one.
        """
        self.db_manager = database_manager or DatabaseManager()
        if not self.db_manager.engine:
            self.db_manager.initialize_database()
        self.config = Config()
    
    # ==================== Sanctions Data Management ====================
    
    def store_sanctions_data(self, entities: List[Dict[str, Any]], source: str, source_version: str) -> Tuple[int, int, int]:
        """
        Store or update sanctions data from a specific source.
        
        Args:
            entities: List of entity dictionaries to store
            source: Source of the data (e.g., 'EU', 'UN', 'OFAC')
            source_version: Version identifier for the source data
            
        Returns:
            Tuple of (created_count, updated_count, skipped_count)
        """
        session = self.db_manager.get_session()
        created_count = updated_count = skipped_count = 0
        
        try:
            # Get existing entities from this source
            existing_entities = {
                entity.name: entity 
                for entity in session.query(SanctionedEntity).filter_by(source=source).all()
            }
            
            for entity_data in entities:
                try:
                    entity_name = entity_data.get('name', '').strip()
                    if not entity_name:
                        skipped_count += 1
                        continue
                    
                    if entity_name in existing_entities:
                        # Update existing entity
                        existing_entity = existing_entities[entity_name]
                        if existing_entity.source_version != source_version:
                            self._update_entity_from_dict(existing_entity, entity_data, source_version)
                            updated_count += 1
                        else:
                            skipped_count += 1
                    else:
                        # Create new entity
                        new_entity = self._create_entity_from_dict(entity_data, source, source_version)
                        session.add(new_entity)
                        created_count += 1
                        
                except Exception as e:
                    logger.error(f"Error processing entity {entity_data.get('name', 'unknown')}: {e}")
                    skipped_count += 1
                    continue
            
            session.commit()
            logger.info(f"Stored sanctions data from {source}: {created_count} created, {updated_count} updated, {skipped_count} skipped")
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error storing sanctions data: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
        
        return created_count, updated_count, skipped_count
    
    def get_sanctions_data(self, source: str = None, entity_type: str = None, 
                          limit: int = None, offset: int = 0) -> List[SanctionedEntity]:
        """
        Retrieve sanctions data with optional filtering.
        
        Args:
            source: Optional source filter
            entity_type: Optional entity type filter
            limit: Optional limit on number of results
            offset: Optional offset for pagination
            
        Returns:
            List of SanctionedEntity objects
        """
        session = self.db_manager.get_session()
        
        try:
            query = session.query(SanctionedEntity)
            
            if source:
                query = query.filter(SanctionedEntity.source == source)
            if entity_type:
                query = query.filter(SanctionedEntity.entity_type == entity_type)
            
            query = query.order_by(SanctionedEntity.name)
            
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)
            
            return query.all()
            
        finally:
            self.db_manager.close_session(session)
    
    def get_sanctions_data_versions(self) -> Dict[str, str]:
        """
        Get the current version information for all sanctions data sources.
        
        Returns:
            Dictionary mapping source names to their current versions
        """
        session = self.db_manager.get_session()
        
        try:
            # Get the latest version for each source
            result = session.query(
                SanctionedEntity.source,
                func.max(SanctionedEntity.updated_at).label('latest_update')
            ).group_by(SanctionedEntity.source).all()
            
            versions = {}
            for source, latest_update in result:
                # Get the source_version for the most recently updated entity from this source
                latest_entity = session.query(SanctionedEntity).filter(
                    and_(
                        SanctionedEntity.source == source,
                        SanctionedEntity.updated_at == latest_update
                    )
                ).first()
                
                if latest_entity:
                    versions[source] = latest_entity.source_version
            
            return versions
            
        finally:
            self.db_manager.close_session(session)
    
    def delete_sanctions_data(self, source: str, source_version: str = None) -> int:
        """
        Delete sanctions data from a specific source and optionally version.
        
        Args:
            source: Source to delete data from
            source_version: Optional specific version to delete
            
        Returns:
            Number of entities deleted
        """
        session = self.db_manager.get_session()
        
        try:
            query = session.query(SanctionedEntity).filter(SanctionedEntity.source == source)
            
            if source_version:
                query = query.filter(SanctionedEntity.source_version == source_version)
            
            entities_to_delete = query.all()
            count = len(entities_to_delete)
            
            for entity in entities_to_delete:
                session.delete(entity)
            
            session.commit()
            logger.info(f"Deleted {count} entities from source {source}" + 
                       (f" version {source_version}" if source_version else ""))
            
            return count
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error deleting sanctions data: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    # ==================== Search History Management ====================
    
    def store_search_record(self, search_query: str, user_id: str = None, 
                           sanctions_list_versions: Dict[str, str] = None,
                           search_parameters: Dict[str, Any] = None,
                           verification_hash: str = None) -> str:
        """
        Store a search record in the database.
        
        Args:
            search_query: The search query string
            user_id: Optional user identifier
            sanctions_list_versions: Dictionary of source versions used
            search_parameters: Search configuration parameters
            verification_hash: Cryptographic hash for verification
            
        Returns:
            ID of the created search record
        """
        session = self.db_manager.get_session()
        
        try:
            search_record = SearchRecord(
                search_query=search_query,
                user_id=user_id,
                sanctions_list_versions=sanctions_list_versions or {},
                search_parameters=search_parameters or {},
                verification_hash=verification_hash or ""
            )
            
            session.add(search_record)
            session.commit()
            
            logger.debug(f"Stored search record: {search_record.id}")
            return search_record.id
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error storing search record: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def store_search_results(self, search_record_id: str, results: List[Dict[str, Any]]) -> int:
        """
        Store search results for a given search record.
        
        Args:
            search_record_id: ID of the search record
            results: List of search result dictionaries
            
        Returns:
            Number of results stored
        """
        session = self.db_manager.get_session()
        
        try:
            stored_count = 0
            
            for result_data in results:
                search_result = SearchResult(
                    search_record_id=search_record_id,
                    entity_id=result_data.get('entity_id'),
                    confidence_scores=result_data.get('confidence_scores', {}),
                    match_details=result_data.get('match_details', {}),
                    overall_confidence=result_data.get('overall_confidence', 0.0)
                )
                
                session.add(search_result)
                stored_count += 1
            
            session.commit()
            logger.debug(f"Stored {stored_count} search results for record {search_record_id}")
            
            return stored_count
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error storing search results: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def get_search_history(self, user_id: str = None, limit: int = 100, 
                          offset: int = 0, start_date: datetime = None, 
                          end_date: datetime = None) -> List[SearchRecord]:
        """
        Retrieve search history with optional filtering.
        
        Args:
            user_id: Optional user ID filter
            limit: Maximum number of records to return
            offset: Offset for pagination
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of SearchRecord objects with results loaded
        """
        session = self.db_manager.get_session()
        
        try:
            query = session.query(SearchRecord)
            
            if user_id:
                query = query.filter(SearchRecord.user_id == user_id)
            
            if start_date:
                query = query.filter(SearchRecord.search_timestamp >= start_date)
            
            if end_date:
                query = query.filter(SearchRecord.search_timestamp <= end_date)
            
            query = query.order_by(desc(SearchRecord.search_timestamp))
            
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)
            
            records = query.all()
            
            # Force load the results to avoid DetachedInstanceError
            for record in records:
                _ = len(record.results)  # This triggers the lazy loading
            
            return records
            
        finally:
            self.db_manager.close_session(session)
    
    def get_search_record_with_results(self, search_record_id: str) -> Optional[SearchRecord]:
        """
        Get a search record with all its results.
        
        Args:
            search_record_id: ID of the search record
            
        Returns:
            SearchRecord object with results, or None if not found
        """
        session = self.db_manager.get_session()
        
        try:
            record = session.query(SearchRecord).filter(
                SearchRecord.id == search_record_id
            ).first()
            
            if record:
                # Force load the results to avoid DetachedInstanceError
                _ = len(record.results)
                for result in record.results:
                    _ = result.entity  # Force load entity as well
            
            return record
            
        finally:
            self.db_manager.close_session(session)
    
    def get_search_statistics(self, user_id: str = None, days: int = 30) -> Dict[str, Any]:
        """
        Get search statistics for the specified period.
        
        Args:
            user_id: Optional user ID filter
            days: Number of days to look back
            
        Returns:
            Dictionary containing search statistics
        """
        session = self.db_manager.get_session()
        
        try:
            start_date = datetime.now() - timedelta(days=days)
            
            query = session.query(SearchRecord).filter(
                SearchRecord.search_timestamp >= start_date
            )
            
            if user_id:
                query = query.filter(SearchRecord.user_id == user_id)
            
            search_records = query.all()
            
            # Calculate statistics
            total_searches = len(search_records)
            total_results = sum(len(record.results) for record in search_records)
            
            high_confidence_matches = 0
            medium_confidence_matches = 0
            low_confidence_matches = 0
            
            for record in search_records:
                for result in record.results:
                    if result.overall_confidence >= 0.8:
                        high_confidence_matches += 1
                    elif result.overall_confidence >= 0.6:
                        medium_confidence_matches += 1
                    else:
                        low_confidence_matches += 1
            
            # Get unique users count (if not filtering by user)
            unique_users = 0
            if not user_id:
                unique_users = session.query(SearchRecord.user_id).filter(
                    and_(
                        SearchRecord.search_timestamp >= start_date,
                        SearchRecord.user_id.isnot(None)
                    )
                ).distinct().count()
            
            return {
                'period_days': days,
                'total_searches': total_searches,
                'total_results': total_results,
                'unique_users': unique_users,
                'high_confidence_matches': high_confidence_matches,
                'medium_confidence_matches': medium_confidence_matches,
                'low_confidence_matches': low_confidence_matches,
                'average_results_per_search': total_results / total_searches if total_searches > 0 else 0
            }
            
        finally:
            self.db_manager.close_session(session)
    
    # ==================== Data Versioning and Audit Trail ====================
    
    def get_data_version_history(self, source: str = None) -> List[Dict[str, Any]]:
        """
        Get version history for sanctions data sources.
        
        Args:
            source: Optional source filter
            
        Returns:
            List of version history entries
        """
        session = self.db_manager.get_session()
        
        try:
            query = session.query(
                SanctionedEntity.source,
                SanctionedEntity.source_version,
                func.min(SanctionedEntity.created_at).label('first_seen'),
                func.max(SanctionedEntity.updated_at).label('last_updated'),
                func.count(SanctionedEntity.id).label('entity_count')
            ).group_by(SanctionedEntity.source, SanctionedEntity.source_version)
            
            if source:
                query = query.filter(SanctionedEntity.source == source)
            
            query = query.order_by(desc('last_updated'))
            
            results = []
            for row in query.all():
                results.append({
                    'source': row.source,
                    'version': row.source_version,
                    'first_seen': row.first_seen.isoformat() if row.first_seen else None,
                    'last_updated': row.last_updated.isoformat() if row.last_updated else None,
                    'entity_count': row.entity_count
                })
            
            return results
            
        finally:
            self.db_manager.close_session(session)
    
    def create_audit_trail_entry(self, operation: str, table_name: str, record_id: str,
                                old_values: Dict[str, Any] = None, new_values: Dict[str, Any] = None,
                                user_id: str = None) -> None:
        """
        Create an audit trail entry for data changes.
        
        Args:
            operation: Type of operation (INSERT, UPDATE, DELETE)
            table_name: Name of the table affected
            record_id: ID of the affected record
            old_values: Previous values (for UPDATE/DELETE)
            new_values: New values (for INSERT/UPDATE)
            user_id: Optional user who performed the operation
        """
        # Note: This is a placeholder for audit trail functionality
        # In a production system, you might want to create a separate audit_trail table
        logger.info(f"Audit trail: {operation} on {table_name} record {record_id} by user {user_id}")
        
        # For now, we'll just log the operation
        # In the future, this could be extended to store in a dedicated audit table
    
    # ==================== Database Maintenance and Cleanup ====================
    
    def cleanup_old_search_records(self, retention_days: int = None) -> int:
        """
        Clean up old search records based on retention policy.
        
        Args:
            retention_days: Number of days to retain records. If None, uses config.
            
        Returns:
            Number of records deleted
        """
        if retention_days is None:
            retention_days = self.config.get('database', {}).get('search_retention_days', 90)
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        session = self.db_manager.get_session()
        
        try:
            # Get records to delete
            records_to_delete = session.query(SearchRecord).filter(
                SearchRecord.search_timestamp < cutoff_date
            ).all()
            
            count = len(records_to_delete)
            
            # Delete records (results will be deleted due to cascade)
            for record in records_to_delete:
                session.delete(record)
            
            session.commit()
            logger.info(f"Cleaned up {count} old search records (older than {retention_days} days)")
            
            return count
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error cleaning up old search records: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def cleanup_orphaned_entities(self) -> int:
        """
        Clean up orphaned sanctioned entities (entities with no valid source version).
        
        Returns:
            Number of entities cleaned up
        """
        session = self.db_manager.get_session()
        
        try:
            # This is a placeholder for more sophisticated orphan detection
            # For now, we'll just count entities with empty source_version
            orphaned_entities = session.query(SanctionedEntity).filter(
                or_(
                    SanctionedEntity.source_version == '',
                    SanctionedEntity.source_version.is_(None)
                )
            ).all()
            
            count = len(orphaned_entities)
            
            for entity in orphaned_entities:
                session.delete(entity)
            
            session.commit()
            logger.info(f"Cleaned up {count} orphaned entities")
            
            return count
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error cleaning up orphaned entities: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def optimize_database(self) -> Dict[str, Any]:
        """
        Perform database optimization operations.
        
        Returns:
            Dictionary with optimization results
        """
        session = self.db_manager.get_session()
        results = {}
        
        try:
            # For SQLite, run VACUUM and ANALYZE
            if "sqlite" in self.db_manager.database_url:
                session.execute(text("VACUUM"))
                session.execute(text("ANALYZE"))
                results['vacuum'] = True
                results['analyze'] = True
                logger.info("Database optimization completed (VACUUM and ANALYZE)")
            else:
                results['message'] = "Optimization not implemented for this database type"
            
            session.commit()
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error optimizing database: {e}")
            results['error'] = str(e)
        finally:
            self.db_manager.close_session(session)
        
        return results
    
    def get_database_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive database statistics.
        
        Returns:
            Dictionary containing database statistics
        """
        session = self.db_manager.get_session()
        
        try:
            stats = {}
            
            # Entity counts by source
            entity_counts = session.query(
                SanctionedEntity.source,
                func.count(SanctionedEntity.id).label('count')
            ).group_by(SanctionedEntity.source).all()
            
            stats['entities_by_source'] = {source: count for source, count in entity_counts}
            stats['total_entities'] = sum(count for _, count in entity_counts)
            
            # Entity counts by type
            type_counts = session.query(
                SanctionedEntity.entity_type,
                func.count(SanctionedEntity.id).label('count')
            ).group_by(SanctionedEntity.entity_type).all()
            
            stats['entities_by_type'] = {entity_type: count for entity_type, count in type_counts}
            
            # Search record counts
            stats['total_search_records'] = session.query(SearchRecord).count()
            stats['total_search_results'] = session.query(SearchResult).count()
            
            # Recent activity (last 30 days)
            thirty_days_ago = datetime.now() - timedelta(days=30)
            stats['recent_searches'] = session.query(SearchRecord).filter(
                SearchRecord.search_timestamp >= thirty_days_ago
            ).count()
            
            # Database size (for SQLite)
            if "sqlite" in self.db_manager.database_url:
                db_path = self.db_manager.database_url.replace("sqlite:///", "")
                try:
                    import os
                    stats['database_size_bytes'] = os.path.getsize(db_path)
                    stats['database_size_mb'] = round(stats['database_size_bytes'] / (1024 * 1024), 2)
                except:
                    stats['database_size_bytes'] = None
            
            return stats
            
        finally:
            self.db_manager.close_session(session)
    
    # ==================== Helper Methods ====================
    
    def _create_entity_from_dict(self, entity_data: Dict[str, Any], source: str, source_version: str) -> SanctionedEntity:
        """Create a SanctionedEntity from dictionary data."""
        return SanctionedEntity(
            name=entity_data.get('name', '').strip(),
            aliases=entity_data.get('aliases', []),
            entity_type=entity_data.get('entity_type', 'UNKNOWN'),
            sanctions_type=entity_data.get('sanctions_type', ''),
            effective_date=entity_data.get('effective_date'),
            source=source,
            source_version=source_version,
            additional_info=entity_data.get('additional_info', {})
        )
    
    def _update_entity_from_dict(self, entity: SanctionedEntity, entity_data: Dict[str, Any], source_version: str) -> None:
        """Update an existing SanctionedEntity from dictionary data."""
        entity.aliases = entity_data.get('aliases', entity.aliases)
        entity.entity_type = entity_data.get('entity_type', entity.entity_type)
        entity.sanctions_type = entity_data.get('sanctions_type', entity.sanctions_type)
        entity.effective_date = entity_data.get('effective_date', entity.effective_date)
        entity.source_version = source_version
        entity.additional_info = entity_data.get('additional_info', entity.additional_info)
    
    def close(self):
        """Close the database connection."""
        if self.db_manager:
            self.db_manager.close()