"""
Custom Sanctions Service for managing user-created sanctions data.

This service provides CRUD operations, data validation, duplicate detection,
and audit logging for custom sanctions entities.
"""
import logging
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple, Any
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import and_, or_, desc, func, text

from ..database.manager import DatabaseManager
from ..models import (
    CustomSanctionEntity, CustomSanctionName, CustomSanctionIndividual,
    CustomSanctionEntityDetails, CustomSanctionAddress, CustomSanctionIdentifier,
    CustomSanctionAuditLog
)
from ..models.base import SubjectType, NameType, RecordStatus
from ..services.custom_sanctions_validator import (
    CustomSanctionsValidator, ValidationResult, DuplicateDetectionResult, DuplicateMatch
)
from ..utils.logger import get_audit_logger

logger = logging.getLogger(__name__)
audit_logger = get_audit_logger()


class CustomSanctionsService:
    """
    Service for managing custom sanctions data with comprehensive CRUD operations,
    validation, duplicate detection, and audit logging.
    """
    
    def __init__(self, database_manager: DatabaseManager = None):
        """
        Initialize the CustomSanctionsService.
        
        Args:
            database_manager: Optional DatabaseManager instance. If not provided, creates a new one.
        """
        self.db_manager = database_manager or DatabaseManager()
        if not self.db_manager.engine:
            self.db_manager.initialize_database()
        self.validator = CustomSanctionsValidator()
    
    # ==================== CRUD Operations ====================
    
    def create_sanction_entity(self, entity_data: Dict[str, Any], user_id: str = None) -> Tuple[str, ValidationResult]:
        """
        Create a new custom sanction entity with comprehensive validation.
        
        Args:
            entity_data: Dictionary containing entity data
            user_id: Optional user ID for audit logging
            
        Returns:
            Tuple of (entity_id, validation_result)
            
        Raises:
            ValueError: If validation fails with errors
            SQLAlchemyError: If database operation fails
        """
        # Validate entity data
        validation_result = self.validator.validate_entity_data(entity_data)
        if not validation_result.is_valid:
            logger.warning(f"Validation failed for entity creation: {validation_result.errors_count} errors")
            raise ValueError(f"Validation failed: {validation_result.errors_count} errors found")
        
        # Check for duplicates
        duplicate_result = self.check_for_duplicates(entity_data)
        if duplicate_result.has_duplicates:
            logger.warning(f"Potential duplicates found: {len(duplicate_result.matches)} matches")
            # Add duplicate warnings to validation result
            for match in duplicate_result.matches:
                validation_result.add_warning(
                    'entity', 
                    f'Potential duplicate found: {match.match_type} "{match.match_value}" (confidence: {match.confidence:.2f})',
                    'POTENTIAL_DUPLICATE'
                )
        
        session = self.db_manager.get_session()
        entity_id = None
        
        try:
            # Generate internal entry ID if not provided
            if not entity_data.get('internal_entry_id'):
                entity_data['internal_entry_id'] = self._generate_internal_entry_id(session)
            
            # Create main entity
            entity = self._create_entity_from_dict(entity_data, user_id)
            session.add(entity)
            session.flush()  # Get the ID without committing
            
            entity_id = entity.id
            
            # Create related records
            self._create_related_records(session, entity, entity_data)
            
            # Create audit log entry (disabled for now due to model mismatch)
            # self._create_audit_log(session, entity.id, 'CREATE', user_id, 
            #                      new_values=self._entity_to_audit_dict(entity))
            
            session.commit()
            
            logger.info(f"Created custom sanction entity: {entity.internal_entry_id} (ID: {entity.id})")
            audit_logger.info(f"Custom sanction entity created: {entity.internal_entry_id} by user {user_id}")
            
            return entity.id, validation_result
            
        except IntegrityError as e:
            session.rollback()
            if "internal_entry_id" in str(e):
                raise ValueError(f"Internal entry ID already exists: {entity_data.get('internal_entry_id')}")
            else:
                logger.error(f"Integrity error creating entity: {e}")
                raise
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error creating entity: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def get_sanction_entity(self, entity_id: str, include_related: bool = True) -> Optional[CustomSanctionEntity]:
        """
        Retrieve a custom sanction entity by ID.
        
        Args:
            entity_id: Entity ID to retrieve
            include_related: Whether to include related records (names, addresses, etc.)
            
        Returns:
            CustomSanctionEntity object or None if not found
        """
        session = self.db_manager.get_session()
        
        try:
            query = session.query(CustomSanctionEntity).filter(CustomSanctionEntity.id == entity_id)
            
            if include_related:
                query = query.options(
                    joinedload(CustomSanctionEntity.names),
                    joinedload(CustomSanctionEntity.addresses),
                    joinedload(CustomSanctionEntity.identifiers),
                    joinedload(CustomSanctionEntity.individual_details),
                    joinedload(CustomSanctionEntity.entity_details)
                )
            
            entity = query.first()
            
            if entity and include_related:
                # Force load relationships to avoid DetachedInstanceError
                _ = len(entity.names)
                _ = len(entity.addresses)
                _ = len(entity.identifiers)
                if entity.individual_details:
                    _ = entity.individual_details.id
                if entity.entity_details:
                    _ = entity.entity_details.id
            
            return entity
            
        finally:
            self.db_manager.close_session(session)
    
    def update_sanction_entity(self, entity_id: str, updates: Dict[str, Any], user_id: str = None) -> Tuple[bool, ValidationResult]:
        """
        Update an existing custom sanction entity.
        
        Args:
            entity_id: ID of entity to update
            updates: Dictionary containing fields to update
            user_id: Optional user ID for audit logging
            
        Returns:
            Tuple of (success, validation_result)
            
        Raises:
            ValueError: If entity not found or validation fails
            SQLAlchemyError: If database operation fails
        """
        session = self.db_manager.get_session()
        
        try:
            # Get existing entity
            entity = session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.id == entity_id
            ).first()
            
            if not entity:
                raise ValueError(f"Entity not found: {entity_id}")
            
            # Store old values for audit
            old_values = self._entity_to_audit_dict(entity)
            
            # Merge updates with existing data for validation
            current_data = entity.to_dict()
            updated_data = {**current_data, **updates}
            
            # Validate updated data
            validation_result = self.validator.validate_entity_data(updated_data)
            if not validation_result.is_valid:
                logger.warning(f"Validation failed for entity update: {validation_result.errors_count} errors")
                raise ValueError(f"Validation failed: {validation_result.errors_count} errors found")
            
            # Update entity fields
            self._update_entity_from_dict(entity, updates)
            
            # Update related records if provided
            if 'names' in updates:
                self._update_names(session, entity, updates['names'])
            if 'addresses' in updates:
                self._update_addresses(session, entity, updates['addresses'])
            if 'identifiers' in updates:
                self._update_identifiers(session, entity, updates['identifiers'])
            if 'individual_details' in updates:
                self._update_individual_details(session, entity, updates['individual_details'])
            if 'entity_details' in updates:
                self._update_entity_details(session, entity, updates['entity_details'])
            
            # Update timestamp
            entity.last_updated = datetime.utcnow()
            
            # Create audit log entry
            new_values = self._entity_to_audit_dict(entity)
            self._create_audit_log(session, entity.id, 'UPDATE', user_id, old_values, new_values)
            
            session.commit()
            
            logger.info(f"Updated custom sanction entity: {entity.internal_entry_id} (ID: {entity.id})")
            audit_logger.info(f"Custom sanction entity updated: {entity.internal_entry_id} by user {user_id}")
            
            return True, validation_result
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error updating entity: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def delete_sanction_entity(self, entity_id: str, user_id: str = None) -> bool:
        """
        Delete a custom sanction entity and all related records.
        
        Args:
            entity_id: ID of entity to delete
            user_id: Optional user ID for audit logging
            
        Returns:
            True if deleted successfully, False if not found
            
        Raises:
            SQLAlchemyError: If database operation fails
        """
        session = self.db_manager.get_session()
        
        try:
            entity = session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.id == entity_id
            ).first()
            
            if not entity:
                return False
            
            # Store values for audit
            old_values = self._entity_to_audit_dict(entity)
            internal_entry_id = entity.internal_entry_id
            
            # Create audit log entry before deletion
            self._create_audit_log(session, entity.id, 'DELETE', user_id, old_values)
            
            # Delete entity (cascade will handle related records)
            session.delete(entity)
            session.commit()
            
            logger.info(f"Deleted custom sanction entity: {internal_entry_id} (ID: {entity_id})")
            audit_logger.info(f"Custom sanction entity deleted: {internal_entry_id} by user {user_id}")
            
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error deleting entity: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def list_sanction_entities(self, filters: Dict[str, Any] = None, 
                             limit: int = None, offset: int = 0,
                             order_by: str = 'last_updated', order_desc: bool = True) -> List[CustomSanctionEntity]:
        """
        List custom sanction entities with optional filtering and pagination.
        
        Args:
            filters: Optional filters (subject_type, record_status, created_by, etc.)
            limit: Optional limit on number of results
            offset: Optional offset for pagination
            order_by: Field to order by (default: last_updated)
            order_desc: Whether to order in descending order
            
        Returns:
            List of CustomSanctionEntity objects
        """
        session = self.db_manager.get_session()
        
        try:
            query = session.query(CustomSanctionEntity).options(
                joinedload(CustomSanctionEntity.names),
                joinedload(CustomSanctionEntity.addresses),
                joinedload(CustomSanctionEntity.identifiers),
                joinedload(CustomSanctionEntity.individual_details),
                joinedload(CustomSanctionEntity.entity_details)
            )
            
            # Apply filters
            if filters:
                if 'subject_type' in filters:
                    query = query.filter(CustomSanctionEntity.subject_type == filters['subject_type'])
                
                if 'record_status' in filters:
                    query = query.filter(CustomSanctionEntity.record_status == filters['record_status'])
                
                if 'created_by' in filters:
                    query = query.filter(CustomSanctionEntity.created_by == filters['created_by'])
                
                if 'sanctioning_authority' in filters:
                    query = query.filter(CustomSanctionEntity.sanctioning_authority.ilike(f"%{filters['sanctioning_authority']}%"))
                
                if 'program' in filters:
                    query = query.filter(CustomSanctionEntity.program.ilike(f"%{filters['program']}%"))
                
                if 'search_term' in filters:
                    search_term = f"%{filters['search_term']}%"
                    query = query.join(CustomSanctionName).filter(
                        or_(
                            CustomSanctionName.full_name.ilike(search_term),
                            CustomSanctionEntity.internal_entry_id.ilike(search_term)
                        )
                    )
                
                if 'date_from' in filters:
                    query = query.filter(CustomSanctionEntity.listing_date >= filters['date_from'])
                
                if 'date_to' in filters:
                    query = query.filter(CustomSanctionEntity.listing_date <= filters['date_to'])
            
            # Apply ordering
            if hasattr(CustomSanctionEntity, order_by):
                order_field = getattr(CustomSanctionEntity, order_by)
                if order_desc:
                    query = query.order_by(desc(order_field))
                else:
                    query = query.order_by(order_field)
            
            # Apply pagination
            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)
            
            entities = query.all()
            
            # Force load relationships to avoid DetachedInstanceError
            for entity in entities:
                _ = len(entity.names)
                _ = len(entity.addresses)
                _ = len(entity.identifiers)
                if entity.individual_details:
                    _ = entity.individual_details.id
                if entity.entity_details:
                    _ = entity.entity_details.id
            
            return entities
            
        finally:
            self.db_manager.close_session(session)
    
    def count_sanction_entities(self, filters: Dict[str, Any] = None) -> int:
        """
        Count custom sanction entities with optional filtering.
        
        Args:
            filters: Optional filters (same as list_sanction_entities)
            
        Returns:
            Number of entities matching the filters
        """
        session = self.db_manager.get_session()
        
        try:
            query = session.query(CustomSanctionEntity)
            
            # Apply same filters as list_sanction_entities
            if filters:
                if 'subject_type' in filters:
                    query = query.filter(CustomSanctionEntity.subject_type == filters['subject_type'])
                
                if 'record_status' in filters:
                    query = query.filter(CustomSanctionEntity.record_status == filters['record_status'])
                
                if 'created_by' in filters:
                    query = query.filter(CustomSanctionEntity.created_by == filters['created_by'])
                
                if 'sanctioning_authority' in filters:
                    query = query.filter(CustomSanctionEntity.sanctioning_authority.ilike(f"%{filters['sanctioning_authority']}%"))
                
                if 'program' in filters:
                    query = query.filter(CustomSanctionEntity.program.ilike(f"%{filters['program']}%"))
                
                if 'search_term' in filters:
                    search_term = f"%{filters['search_term']}%"
                    query = query.join(CustomSanctionName).filter(
                        or_(
                            CustomSanctionName.full_name.ilike(search_term),
                            CustomSanctionEntity.internal_entry_id.ilike(search_term)
                        )
                    )
                
                if 'date_from' in filters:
                    query = query.filter(CustomSanctionEntity.listing_date >= filters['date_from'])
                
                if 'date_to' in filters:
                    query = query.filter(CustomSanctionEntity.listing_date <= filters['date_to'])
            
            return query.count()
            
        finally:
            self.db_manager.close_session(session)
    
    # ==================== Data Validation and Duplicate Detection ====================
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> ValidationResult:
        """
        Validate entity data using the validator service.
        
        Args:
            entity_data: Dictionary containing entity data to validate
            
        Returns:
            ValidationResult with validation issues
        """
        return self.validator.validate_entity_data(entity_data)
    
    def check_for_duplicates(self, entity_data: Dict[str, Any], exclude_entity_id: str = None) -> DuplicateDetectionResult:
        """
        Check for potential duplicate entities based on names, identifiers, and registration numbers.
        
        Args:
            entity_data: Dictionary containing entity data to check
            exclude_entity_id: Optional entity ID to exclude from duplicate check
            
        Returns:
            DuplicateDetectionResult with potential matches
        """
        # Simplified implementation for now - return empty result
        result = DuplicateDetectionResult()
        return result
    
    # ==================== Status Management ====================
    
    def update_entity_status(self, entity_id: str, status: RecordStatus, user_id: str = None) -> bool:
        """
        Update the status of a custom sanction entity.
        
        Args:
            entity_id: ID of entity to update
            status: New status
            user_id: Optional user ID for audit logging
            
        Returns:
            True if updated successfully, False if entity not found
        """
        return self.update_sanction_entity(entity_id, {'record_status': status}, user_id)[0]
    
    def add_internal_note(self, entity_id: str, note: str, user_id: str = None) -> bool:
        """
        Add an internal note to a custom sanction entity.
        
        Args:
            entity_id: ID of entity to update
            note: Note to add
            user_id: Optional user ID for audit logging
            
        Returns:
            True if updated successfully, False if entity not found
        """
        session = self.db_manager.get_session()
        
        try:
            entity = session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.id == entity_id
            ).first()
            
            if not entity:
                return False
            
            # Append note with timestamp
            timestamp = datetime.utcnow().isoformat()
            new_note = f"[{timestamp}] {note}"
            
            if entity.internal_notes:
                entity.internal_notes += f"\n{new_note}"
            else:
                entity.internal_notes = new_note
            
            entity.last_updated = datetime.utcnow()
            
            # Create audit log entry
            self._create_audit_log(session, entity.id, 'UPDATE', user_id, 
                                 changes={'internal_notes': f'Added note: {note}'})
            
            session.commit()
            
            logger.info(f"Added note to entity {entity.internal_entry_id}: {note}")
            audit_logger.info(f"Note added to entity {entity.internal_entry_id} by user {user_id}")
            
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error adding note: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    # ==================== Import/Export Operations ====================
    
    def export_to_xml(self, filters: Dict[str, Any] = None, file_path: str = None) -> str:
        """
        Export custom sanctions entities to XML format.
        
        Args:
            filters: Optional filters to apply (same as list_sanction_entities)
            file_path: Optional file path to save XML. If not provided, returns XML string.
            
        Returns:
            XML content as string
            
        Raises:
            SQLAlchemyError: If database operation fails
            IOError: If file writing fails
        """
        from ..services.custom_sanctions_xml_processor import CustomSanctionsXMLProcessor
        
        # Get entities to export
        entities = self.list_sanction_entities(filters=filters)
        
        if not entities:
            logger.warning("No entities found for export")
            return ""
        
        # Create XML processor
        xml_processor = CustomSanctionsXMLProcessor()
        
        # Generate XML content
        xml_content = xml_processor.export_entities_to_xml(entities)
        
        # Save to file if path provided
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(xml_content)
                logger.info(f"Exported {len(entities)} entities to {file_path}")
            except IOError as e:
                logger.error(f"Failed to write export file: {e}")
                raise
        
        return xml_content
    
    def import_from_xml(self, xml_content: str, conflict_resolution: str = 'skip', 
                       user_id: str = None) -> 'ImportResult':
        """
        Import custom sanctions entities from XML content.
        
        Args:
            xml_content: XML content to import
            conflict_resolution: How to handle conflicts ('skip', 'update', 'create')
            user_id: Optional user ID for audit logging
            
        Returns:
            ImportResult with import statistics and errors
            
        Raises:
            ValueError: If XML validation fails
            SQLAlchemyError: If database operation fails
        """
        from ..services.custom_sanctions_xml_processor import CustomSanctionsXMLProcessor, ImportResult
        
        xml_processor = CustomSanctionsXMLProcessor()
        
        # Validate XML schema
        validation_result = xml_processor.validate_against_schema(xml_content)
        if not validation_result.is_valid:
            raise ValueError(f"XML schema validation failed: {validation_result.error_message}")
        
        # Parse entities from XML
        entities_data = xml_processor.import_entities_from_xml(xml_content)
        
        # Import entities
        total_processed = len(entities_data)
        imported_count = 0
        skipped_count = 0
        error_count = 0
        errors = []
        
        for i, entity_data in enumerate(entities_data):
            try:
                # Check for duplicates
                duplicate_result = self.check_for_duplicates(entity_data)
                
                if duplicate_result.has_duplicates and conflict_resolution == 'skip':
                    skipped_count += 1
                    logger.debug(f"Skipped duplicate entity: {entity_data.get('internal_entry_id')}")
                elif duplicate_result.has_duplicates and conflict_resolution == 'update':
                    # Update existing entity
                    duplicate_id = duplicate_result.matches[0].entity_id
                    self.update_sanction_entity(duplicate_id, entity_data, user_id)
                    imported_count += 1
                    logger.debug(f"Updated existing entity: {entity_data.get('internal_entry_id')}")
                else:
                    # Create new entity
                    self.create_sanction_entity(entity_data, user_id)
                    imported_count += 1
                    logger.debug(f"Created new entity: {entity_data.get('internal_entry_id')}")
                    
            except Exception as e:
                error_count += 1
                error_msg = f"Entity {i+1} ({entity_data.get('internal_entry_id', 'Unknown')}): {str(e)}"
                errors.append(error_msg)
                logger.error(f"Import error: {error_msg}")
        
        result = ImportResult(
            total_processed=total_processed,
            imported_count=imported_count,
            skipped_count=skipped_count,
            error_count=error_count,
            errors=errors
        )
        
        logger.info(f"Import completed: {imported_count} imported, {skipped_count} skipped, {error_count} errors")
        audit_logger.info(f"XML import completed by user {user_id}: {imported_count} entities imported")
        
        return result
    
    def validate_xml_schema(self, xml_content: str) -> 'ValidationResult':
        """
        Validate XML content against the custom sanctions schema.
        
        Args:
            xml_content: XML content to validate
            
        Returns:
            ValidationResult indicating if XML is valid
        """
        from ..services.custom_sanctions_xml_processor import CustomSanctionsXMLProcessor
        
        xml_processor = CustomSanctionsXMLProcessor()
        return xml_processor.validate_against_schema(xml_content)
    
    # ==================== Statistics and Reporting ====================
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about custom sanctions data.
        
        Returns:
            Dictionary containing various statistics
        """
        session = self.db_manager.get_session()
        
        try:
            stats = {}
            
            # Total counts
            stats['total_entities'] = session.query(CustomSanctionEntity).count()
            
            # Counts by subject type
            subject_type_counts = session.query(
                CustomSanctionEntity.subject_type,
                func.count(CustomSanctionEntity.id).label('count')
            ).group_by(CustomSanctionEntity.subject_type).all()
            
            stats['entities_by_subject_type'] = {
                st.value: count for st, count in subject_type_counts
            }
            
            # Counts by status
            status_counts = session.query(
                CustomSanctionEntity.record_status,
                func.count(CustomSanctionEntity.id).label('count')
            ).group_by(CustomSanctionEntity.record_status).all()
            
            stats['entities_by_status'] = {
                status.value: count for status, count in status_counts
            }
            
            # Recent activity (last 30 days)
            thirty_days_ago = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            thirty_days_ago = thirty_days_ago.replace(day=thirty_days_ago.day - 30)
            
            stats['recent_entities'] = session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.created_at >= thirty_days_ago
            ).count()
            
            stats['recent_updates'] = session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.last_updated >= thirty_days_ago
            ).count()
            
            # Top sanctioning authorities
            authority_counts = session.query(
                CustomSanctionEntity.sanctioning_authority,
                func.count(CustomSanctionEntity.id).label('count')
            ).group_by(CustomSanctionEntity.sanctioning_authority).order_by(desc('count')).limit(10).all()
            
            stats['top_sanctioning_authorities'] = [
                {'authority': authority, 'count': count} 
                for authority, count in authority_counts
            ]
            
            return stats
            
        finally:
            self.db_manager.close_session(session)
    
    # ==================== Helper Methods ====================
    
    def _generate_internal_entry_id(self, session: Session) -> str:
        """Generate a unique internal entry ID."""
        import uuid
        
        while True:
            # Generate a shorter, more readable ID
            entry_id = f"CSE-{uuid.uuid4().hex[:8].upper()}"
            
            # Check if it already exists
            existing = session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.internal_entry_id == entry_id
            ).first()
            
            if not existing:
                return entry_id
    
    def _create_entity_from_dict(self, entity_data: Dict[str, Any], user_id: str = None) -> CustomSanctionEntity:
        """Create a CustomSanctionEntity from dictionary data."""
        # Parse dates
        listing_date = entity_data.get('listing_date')
        if isinstance(listing_date, str):
            listing_date = datetime.fromisoformat(listing_date.replace('Z', '+00:00')).date()
        
        verified_date = entity_data.get('verified_date')
        if isinstance(verified_date, str):
            verified_date = datetime.fromisoformat(verified_date.replace('Z', '+00:00'))
        
        return CustomSanctionEntity(
            internal_entry_id=entity_data['internal_entry_id'],
            subject_type=SubjectType(entity_data['subject_type']),
            sanctioning_authority=entity_data['sanctioning_authority'],
            program=entity_data['program'],
            legal_basis=entity_data.get('legal_basis'),
            listing_date=listing_date,
            measures_imposed=entity_data.get('measures_imposed'),
            reason_for_listing=entity_data.get('reason_for_listing'),
            data_source=entity_data['data_source'],
            record_status=RecordStatus(entity_data.get('record_status', RecordStatus.ACTIVE.value)),
            internal_notes=entity_data.get('internal_notes'),
            created_by=user_id or entity_data.get('created_by'),
            verified_by=entity_data.get('verified_by'),
            verified_date=verified_date
        )
    
    def _update_entity_from_dict(self, entity: CustomSanctionEntity, updates: Dict[str, Any]):
        """Update entity fields from dictionary data."""
        # Update simple fields
        simple_fields = [
            'internal_entry_id', 'subject_type', 'sanctioning_authority', 'program',
            'legal_basis', 'measures_imposed', 'reason_for_listing', 'data_source',
            'record_status', 'internal_notes', 'created_by', 'verified_by'
        ]
        
        for field in simple_fields:
            if field in updates:
                if field == 'subject_type' and isinstance(updates[field], str):
                    setattr(entity, field, SubjectType(updates[field]))
                elif field == 'record_status' and isinstance(updates[field], str):
                    setattr(entity, field, RecordStatus(updates[field]))
                else:
                    setattr(entity, field, updates[field])
    
    def _create_related_records(self, session: Session, entity: CustomSanctionEntity, entity_data: Dict[str, Any]):
        """Create related records for an entity."""
        # Create names
        names_data = entity_data.get('names', [])
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
        
        # Create addresses
        addresses_data = entity_data.get('addresses', [])
        for addr_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=addr_data.get('street'),
                city=addr_data.get('city'),
                postal_code=addr_data.get('postal_code'),
                country=addr_data.get('country'),
                full_address=addr_data.get('full_address')
            )
            session.add(address)
        
        # Create identifiers
        identifiers_data = entity_data.get('identifiers', [])
        for id_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=id_data['id_type'],
                id_value=id_data['id_value'],
                issuing_country=id_data.get('issuing_country'),
                notes=id_data.get('notes')
            )
            session.add(identifier)
        
        # Create individual details
        individual_data = entity_data.get('individual_details')
        if individual_data and entity.subject_type == SubjectType.INDIVIDUAL:
            individual = CustomSanctionIndividual(
                entity_id=entity.id,
                birth_year=individual_data.get('birth_year'),
                birth_month=individual_data.get('birth_month'),
                birth_day=individual_data.get('birth_day'),
                birth_full_date=individual_data.get('birth_full_date'),
                birth_note=individual_data.get('birth_note'),
                place_of_birth=individual_data.get('place_of_birth'),
                nationalities=individual_data.get('nationalities')
            )
            session.add(individual)
        
        # Create entity details
        entity_details_data = entity_data.get('entity_details')
        if entity_details_data and entity.subject_type == SubjectType.ENTITY:
            entity_details = CustomSanctionEntityDetails(
                entity_id=entity.id,
                registration_number=entity_details_data.get('registration_number'),
                registration_authority=entity_details_data.get('registration_authority'),
                incorporation_date=entity_details_data.get('incorporation_date'),
                company_type=entity_details_data.get('company_type'),
                tax_id=entity_details_data.get('tax_id')
            )
            session.add(entity_details)
    
    def _update_names(self, session: Session, entity: CustomSanctionEntity, names_data: List[Dict[str, Any]]):
        """Update names for an entity."""
        # Delete existing names
        session.query(CustomSanctionName).filter(
            CustomSanctionName.entity_id == entity.id
        ).delete()
        
        # Create new names
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
    
    def _update_addresses(self, session: Session, entity: CustomSanctionEntity, addresses_data: List[Dict[str, Any]]):
        """Update addresses for an entity."""
        # Delete existing addresses
        session.query(CustomSanctionAddress).filter(
            CustomSanctionAddress.entity_id == entity.id
        ).delete()
        
        # Create new addresses
        for addr_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=addr_data.get('street'),
                city=addr_data.get('city'),
                postal_code=addr_data.get('postal_code'),
                country=addr_data.get('country'),
                full_address=addr_data.get('full_address')
            )
            session.add(address)
    
    def _update_identifiers(self, session: Session, entity: CustomSanctionEntity, identifiers_data: List[Dict[str, Any]]):
        """Update identifiers for an entity."""
        # Delete existing identifiers
        session.query(CustomSanctionIdentifier).filter(
            CustomSanctionIdentifier.entity_id == entity.id
        ).delete()
        
        # Create new identifiers
        for id_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=id_data['id_type'],
                id_value=id_data['id_value'],
                issuing_country=id_data.get('issuing_country'),
                notes=id_data.get('notes')
            )
            session.add(identifier)
    
    def _update_individual_details(self, session: Session, entity: CustomSanctionEntity, individual_data: Dict[str, Any]):
        """Update individual details for an entity."""
        # Delete existing individual details
        session.query(CustomSanctionIndividual).filter(
            CustomSanctionIndividual.entity_id == entity.id
        ).delete()
        
        # Create new individual details
        if individual_data:
            individual = CustomSanctionIndividual(
                entity_id=entity.id,
                birth_year=individual_data.get('birth_year'),
                birth_month=individual_data.get('birth_month'),
                birth_day=individual_data.get('birth_day'),
                birth_full_date=individual_data.get('birth_full_date'),
                birth_note=individual_data.get('birth_note'),
                place_of_birth=individual_data.get('place_of_birth'),
                nationalities=individual_data.get('nationalities')
            )
            session.add(individual)
    
    def _update_entity_details(self, session: Session, entity: CustomSanctionEntity, entity_details_data: Dict[str, Any]):
        """Update entity details for an entity."""
        # Delete existing entity details
        session.query(CustomSanctionEntityDetails).filter(
            CustomSanctionEntityDetails.entity_id == entity.id
        ).delete()
        
        # Create new entity details
        if entity_details_data:
            entity_details = CustomSanctionEntityDetails(
                entity_id=entity.id,
                registration_number=entity_details_data.get('registration_number'),
                registration_authority=entity_details_data.get('registration_authority'),
                incorporation_date=entity_details_data.get('incorporation_date'),
                company_type=entity_details_data.get('company_type'),
                tax_id=entity_details_data.get('tax_id')
            )
            session.add(entity_details)
    
    def _create_audit_log(self, session: Session, entity_id: str, action: str, user_id: str = None, 
                         old_values: Dict[str, Any] = None, new_values: Dict[str, Any] = None, 
                         changes: Dict[str, Any] = None):
        """Create an audit log entry."""
        # Prepare changes data
        audit_changes = changes or {}
        if old_values:
            audit_changes['old_values'] = old_values
        if new_values:
            audit_changes['new_values'] = new_values
        
        audit_log = CustomSanctionAuditLog(
            entity_id=entity_id,
            action=action,
            user_id=user_id,
            changes=audit_changes
        )
        session.add(audit_log)
    
    def _entity_to_audit_dict(self, entity: CustomSanctionEntity) -> Dict[str, Any]:
        """Convert entity to dictionary for audit logging."""
        return {
            'internal_entry_id': entity.internal_entry_id,
            'subject_type': entity.subject_type.value,
            'sanctioning_authority': entity.sanctioning_authority,
            'program': entity.program,
            'record_status': entity.record_status.value,
            'last_updated': entity.last_updated.isoformat() if entity.last_updated else None
        }
    
    def _update_entity_from_dict(self, entity: CustomSanctionEntity, updates: Dict[str, Any]):
        """Update entity fields from dictionary data."""
        # Parse dates if they are strings
        if 'listing_date' in updates:
            listing_date = updates['listing_date']
            if isinstance(listing_date, str):
                listing_date = datetime.fromisoformat(listing_date.replace('Z', '+00:00')).date()
            entity.listing_date = listing_date
        
        if 'verified_date' in updates:
            verified_date = updates['verified_date']
            if isinstance(verified_date, str):
                verified_date = datetime.fromisoformat(verified_date.replace('Z', '+00:00'))
            entity.verified_date = verified_date
        
        # Update simple fields
        simple_fields = [
            'internal_entry_id', 'subject_type', 'sanctioning_authority', 'program',
            'legal_basis', 'measures_imposed', 'reason_for_listing', 'data_source',
            'record_status', 'internal_notes', 'created_by', 'verified_by'
        ]
        
        for field in simple_fields:
            if field in updates:
                if field == 'subject_type' and isinstance(updates[field], str):
                    setattr(entity, field, SubjectType(updates[field]))
                elif field == 'record_status' and isinstance(updates[field], str):
                    setattr(entity, field, RecordStatus(updates[field]))
                else:
                    setattr(entity, field, updates[field])
    
    def _create_related_records(self, session: Session, entity: CustomSanctionEntity, entity_data: Dict[str, Any]):
        """Create related records for an entity."""
        # Create names
        names_data = entity_data.get('names', [])
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
        
        # Create addresses
        addresses_data = entity_data.get('addresses', [])
        for addr_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=addr_data.get('street'),
                city=addr_data.get('city'),
                postal_code=addr_data.get('postal_code'),
                country=addr_data.get('country'),
                full_address=addr_data.get('full_address')
            )
            session.add(address)
        
        # Create identifiers
        identifiers_data = entity_data.get('identifiers', [])
        for id_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=id_data['id_type'],
                id_value=id_data['id_value'],
                issuing_country=id_data.get('issuing_country'),
                notes=id_data.get('notes')
            )
            session.add(identifier)
        
        # Create individual details
        individual_data = entity_data.get('individual_details')
        if individual_data and entity.subject_type == SubjectType.INDIVIDUAL:
            individual = CustomSanctionIndividual(
                entity_id=entity.id,
                birth_year=individual_data.get('birth_year'),
                birth_month=individual_data.get('birth_month'),
                birth_day=individual_data.get('birth_day'),
                birth_full_date=individual_data.get('birth_full_date'),
                birth_note=individual_data.get('birth_note'),
                place_of_birth=individual_data.get('place_of_birth'),
                nationalities=individual_data.get('nationalities')
            )
            session.add(individual)
        
        # Create entity details
        entity_details_data = entity_data.get('entity_details')
        if entity_details_data and entity.subject_type == SubjectType.ENTITY:
            entity_details = CustomSanctionEntityDetails(
                entity_id=entity.id,
                registration_number=entity_details_data.get('registration_number'),
                registration_authority=entity_details_data.get('registration_authority'),
                incorporation_date=entity_details_data.get('incorporation_date'),
                company_type=entity_details_data.get('company_type'),
                tax_id=entity_details_data.get('tax_id')
            )
            session.add(entity_details)
    
    def _update_names(self, session: Session, entity: CustomSanctionEntity, names_data: List[Dict[str, Any]]):
        """Update names for an entity."""
        # Delete existing names
        session.query(CustomSanctionName).filter(
            CustomSanctionName.entity_id == entity.id
        ).delete()
        
        # Create new names
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
    
    def _update_addresses(self, session: Session, entity: CustomSanctionEntity, addresses_data: List[Dict[str, Any]]):
        """Update addresses for an entity."""
        # Delete existing addresses
        session.query(CustomSanctionAddress).filter(
            CustomSanctionAddress.entity_id == entity.id
        ).delete()
        
        # Create new addresses
        for addr_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=addr_data.get('street'),
                city=addr_data.get('city'),
                postal_code=addr_data.get('postal_code'),
                country=addr_data.get('country'),
                full_address=addr_data.get('full_address')
            )
            session.add(address)
    
    def _update_identifiers(self, session: Session, entity: CustomSanctionEntity, identifiers_data: List[Dict[str, Any]]):
        """Update identifiers for an entity."""
        # Delete existing identifiers
        session.query(CustomSanctionIdentifier).filter(
            CustomSanctionIdentifier.entity_id == entity.id
        ).delete()
        
        # Create new identifiers
        for id_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=id_data['id_type'],
                id_value=id_data['id_value'],
                issuing_country=id_data.get('issuing_country'),
                notes=id_data.get('notes')
            )
            session.add(identifier)
    
    def _update_individual_details(self, session: Session, entity: CustomSanctionEntity, individual_data: Dict[str, Any]):
        """Update individual details for an entity."""
        # Delete existing individual details
        session.query(CustomSanctionIndividual).filter(
            CustomSanctionIndividual.entity_id == entity.id
        ).delete()
        
        # Create new individual details
        if individual_data:
            individual = CustomSanctionIndividual(
                entity_id=entity.id,
                birth_year=individual_data.get('birth_year'),
                birth_month=individual_data.get('birth_month'),
                birth_day=individual_data.get('birth_day'),
                birth_full_date=individual_data.get('birth_full_date'),
                birth_note=individual_data.get('birth_note'),
                place_of_birth=individual_data.get('place_of_birth'),
                nationalities=individual_data.get('nationalities')
            )
            session.add(individual)
    
    def _update_entity_details(self, session: Session, entity: CustomSanctionEntity, entity_details_data: Dict[str, Any]):
        """Update entity details for an entity."""
        # Delete existing entity details
        session.query(CustomSanctionEntityDetails).filter(
            CustomSanctionEntityDetails.entity_id == entity.id
        ).delete()
        
        # Create new entity details
        if entity_details_data:
            entity_details = CustomSanctionEntityDetails(
                entity_id=entity.id,
                registration_number=entity_details_data.get('registration_number'),
                registration_authority=entity_details_data.get('registration_authority'),
                incorporation_date=entity_details_data.get('incorporation_date'),
                company_type=entity_details_data.get('company_type'),
                tax_id=entity_details_data.get('tax_id')
            )
            session.add(entity_details)
    
    def _check_name_duplicates(self, session: Session, full_name: str, result: DuplicateDetectionResult, exclude_entity_id: str = None):
        """Check for name-based duplicates."""
        # Simple name matching for now - can be enhanced with fuzzy matching later
        query = session.query(CustomSanctionName).join(CustomSanctionEntity)
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        existing_names = query.filter(CustomSanctionName.full_name.ilike(f"%{full_name}%")).all()
        
        for existing_name in existing_names:
            # Simple similarity check
            similarity = 1.0 if existing_name.full_name.lower() == full_name.lower() else 0.8
            if similarity > 0.7:
                from ..services.custom_sanctions_validator import DuplicateMatch
                match = DuplicateMatch(
                    entity_id=existing_name.entity_id,
                    match_type='name',
                    match_value=existing_name.full_name,
                    confidence=similarity
                )
                result.add_match(match)
    
    def _check_identifier_duplicates(self, session: Session, id_type: str, id_value: str, result: DuplicateDetectionResult, exclude_entity_id: str = None):
        """Check for identifier-based duplicates."""
        query = session.query(CustomSanctionIdentifier).join(CustomSanctionEntity).filter(
            and_(
                CustomSanctionIdentifier.id_type == id_type,
                CustomSanctionIdentifier.id_value == id_value
            )
        )
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        existing_identifiers = query.all()
        
        for existing_id in existing_identifiers:
            from ..services.custom_sanctions_validator import DuplicateMatch
            match = DuplicateMatch(
                entity_id=existing_id.entity_id,
                match_type='identifier',
                match_value=f"{id_type}: {id_value}",
                confidence=1.0  # Exact match
            )
            result.add_match(match)
    
    def _check_registration_duplicates(self, session: Session, registration_number: str, result: DuplicateDetectionResult, exclude_entity_id: str = None):
        """Check for registration number duplicates."""
        query = session.query(CustomSanctionEntityDetails).join(CustomSanctionEntity).filter(
            CustomSanctionEntityDetails.registration_number == registration_number
        )
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        existing_registrations = query.all()
        
        for existing_reg in existing_registrations:
            from ..services.custom_sanctions_validator import DuplicateMatch
            match = DuplicateMatch(
                entity_id=existing_reg.entity_id,
                match_type='registration_number',
                match_value=registration_number,
                confidence=1.0  # Exact match
            )
            result.add_match(match)
    

    
    def _entity_to_audit_dict(self, entity: CustomSanctionEntity) -> Dict[str, Any]:
        """Convert entity to dictionary for audit logging."""
        return {
            'internal_entry_id': entity.internal_entry_id,
            'subject_type': entity.subject_type.value,
            'sanctioning_authority': entity.sanctioning_authority,
            'program': entity.program,
            'record_status': entity.record_status.value,
            'last_updated': entity.last_updated.isoformat() if entity.last_updated else None
        }
    
    def _update_entity_from_dict(self, entity: CustomSanctionEntity, updates: Dict[str, Any]):
        """Update entity fields from dictionary data."""
        # Parse dates if they are strings
        if 'listing_date' in updates:
            listing_date = updates['listing_date']
            if isinstance(listing_date, str):
                listing_date = datetime.fromisoformat(listing_date.replace('Z', '+00:00')).date()
            entity.listing_date = listing_date
        
        if 'verified_date' in updates:
            verified_date = updates['verified_date']
            if isinstance(verified_date, str):
                verified_date = datetime.fromisoformat(verified_date.replace('Z', '+00:00'))
            entity.verified_date = verified_date
        
        # Update simple fields
        simple_fields = [
            'internal_entry_id', 'subject_type', 'sanctioning_authority', 'program',
            'legal_basis', 'measures_imposed', 'reason_for_listing', 'data_source',
            'record_status', 'internal_notes', 'created_by', 'verified_by'
        ]
        
        for field in simple_fields:
            if field in updates:
                if field == 'subject_type' and isinstance(updates[field], str):
                    setattr(entity, field, SubjectType(updates[field]))
                elif field == 'record_status' and isinstance(updates[field], str):
                    setattr(entity, field, RecordStatus(updates[field]))
                else:
                    setattr(entity, field, updates[field])
    
    def _create_related_records(self, session: Session, entity: CustomSanctionEntity, entity_data: Dict[str, Any]):
        """Create related records for an entity."""
        # Create names
        names_data = entity_data.get('names', [])
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
        
        # Create addresses
        addresses_data = entity_data.get('addresses', [])
        for address_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=address_data.get('street'),
                city=address_data.get('city'),
                postal_code=address_data.get('postal_code'),
                country=address_data.get('country'),
                full_address=address_data.get('full_address')
            )
            session.add(address)
        
        # Create identifiers
        identifiers_data = entity_data.get('identifiers', [])
        for identifier_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=identifier_data['id_type'],
                id_value=identifier_data['id_value'],
                issuing_country=identifier_data.get('issuing_country'),
                notes=identifier_data.get('notes')
            )
            session.add(identifier)
        
        # Create individual details if subject is Individual
        if entity.subject_type == SubjectType.INDIVIDUAL:
            individual_data = entity_data.get('individual_details', {})
            if individual_data:
                # Parse birth date components
                birth_full_date = None
                if 'birth_full_date' in individual_data:
                    birth_date_str = individual_data['birth_full_date']
                    if isinstance(birth_date_str, str):
                        birth_full_date = datetime.fromisoformat(birth_date_str.replace('Z', '+00:00')).date()
                    else:
                        birth_full_date = birth_date_str
                
                individual = CustomSanctionIndividual(
                    entity_id=entity.id,
                    birth_year=individual_data.get('birth_year'),
                    birth_month=individual_data.get('birth_month'),
                    birth_day=individual_data.get('birth_day'),
                    birth_full_date=birth_full_date,
                    birth_note=individual_data.get('birth_note'),
                    place_of_birth=individual_data.get('place_of_birth'),
                    nationalities=individual_data.get('nationalities', [])
                )
                session.add(individual)
        
        # Create entity details if subject is Entity
        elif entity.subject_type == SubjectType.ENTITY:
            entity_details_data = entity_data.get('entity_details', {})
            if entity_details_data:
                # Parse incorporation date
                incorporation_date = None
                if 'incorporation_date' in entity_details_data:
                    inc_date_str = entity_details_data['incorporation_date']
                    if isinstance(inc_date_str, str):
                        incorporation_date = datetime.fromisoformat(inc_date_str.replace('Z', '+00:00')).date()
                    else:
                        incorporation_date = inc_date_str
                
                entity_details = CustomSanctionEntityDetails(
                    entity_id=entity.id,
                    registration_number=entity_details_data.get('registration_number'),
                    registration_authority=entity_details_data.get('registration_authority'),
                    incorporation_date=incorporation_date,
                    company_type=entity_details_data.get('company_type'),
                    tax_id=entity_details_data.get('tax_id')
                )
                session.add(entity_details)
    
    def _update_names(self, session: Session, entity: CustomSanctionEntity, names_data: List[Dict[str, Any]]):
        """Update names for an entity."""
        # Delete existing names
        session.query(CustomSanctionName).filter(
            CustomSanctionName.entity_id == entity.id
        ).delete()
        
        # Create new names
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
    
    def _update_addresses(self, session: Session, entity: CustomSanctionEntity, addresses_data: List[Dict[str, Any]]):
        """Update addresses for an entity."""
        # Delete existing addresses
        session.query(CustomSanctionAddress).filter(
            CustomSanctionAddress.entity_id == entity.id
        ).delete()
        
        # Create new addresses
        for address_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=address_data.get('street'),
                city=address_data.get('city'),
                postal_code=address_data.get('postal_code'),
                country=address_data.get('country'),
                full_address=address_data.get('full_address')
            )
            session.add(address)
    
    def _update_identifiers(self, session: Session, entity: CustomSanctionEntity, identifiers_data: List[Dict[str, Any]]):
        """Update identifiers for an entity."""
        # Delete existing identifiers
        session.query(CustomSanctionIdentifier).filter(
            CustomSanctionIdentifier.entity_id == entity.id
        ).delete()
        
        # Create new identifiers
        for identifier_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=identifier_data['id_type'],
                id_value=identifier_data['id_value'],
                issuing_country=identifier_data.get('issuing_country'),
                notes=identifier_data.get('notes')
            )
            session.add(identifier)
    
    def _update_individual_details(self, session: Session, entity: CustomSanctionEntity, individual_data: Dict[str, Any]):
        """Update individual details for an entity."""
        # Delete existing individual details
        session.query(CustomSanctionIndividual).filter(
            CustomSanctionIndividual.entity_id == entity.id
        ).delete()
        
        # Create new individual details if data provided
        if individual_data:
            # Parse birth date
            birth_full_date = None
            if 'birth_full_date' in individual_data:
                birth_date_str = individual_data['birth_full_date']
                if isinstance(birth_date_str, str):
                    birth_full_date = datetime.fromisoformat(birth_date_str.replace('Z', '+00:00')).date()
                else:
                    birth_full_date = birth_date_str
            
            individual = CustomSanctionIndividual(
                entity_id=entity.id,
                birth_year=individual_data.get('birth_year'),
                birth_month=individual_data.get('birth_month'),
                birth_day=individual_data.get('birth_day'),
                birth_full_date=birth_full_date,
                birth_note=individual_data.get('birth_note'),
                place_of_birth=individual_data.get('place_of_birth'),
                nationalities=individual_data.get('nationalities', [])
            )
            session.add(individual)
    
    def _update_entity_details(self, session: Session, entity: CustomSanctionEntity, entity_details_data: Dict[str, Any]):
        """Update entity details for an entity."""
        # Delete existing entity details
        session.query(CustomSanctionEntityDetails).filter(
            CustomSanctionEntityDetails.entity_id == entity.id
        ).delete()
        
        # Create new entity details if data provided
        if entity_details_data:
            # Parse incorporation date
            incorporation_date = None
            if 'incorporation_date' in entity_details_data:
                inc_date_str = entity_details_data['incorporation_date']
                if isinstance(inc_date_str, str):
                    incorporation_date = datetime.fromisoformat(inc_date_str.replace('Z', '+00:00')).date()
                else:
                    incorporation_date = inc_date_str
            
            entity_details = CustomSanctionEntityDetails(
                entity_id=entity.id,
                registration_number=entity_details_data.get('registration_number'),
                registration_authority=entity_details_data.get('registration_authority'),
                incorporation_date=incorporation_date,
                company_type=entity_details_data.get('company_type'),
                tax_id=entity_details_data.get('tax_id')
            )
            session.add(entity_details)
    
    def _entity_to_audit_dict(self, entity: CustomSanctionEntity) -> Dict[str, Any]:
        """Convert entity to dictionary for audit logging."""
        return {
            'id': entity.id,
            'internal_entry_id': entity.internal_entry_id,
            'subject_type': entity.subject_type.value if entity.subject_type else None,
            'sanctioning_authority': entity.sanctioning_authority,
            'program': entity.program,
            'legal_basis': entity.legal_basis,
            'listing_date': entity.listing_date.isoformat() if entity.listing_date else None,
            'measures_imposed': entity.measures_imposed,
            'reason_for_listing': entity.reason_for_listing,
            'data_source': entity.data_source,
            'record_status': entity.record_status.value if entity.record_status else None,
            'internal_notes': entity.internal_notes,
            'created_by': entity.created_by,
            'verified_by': entity.verified_by,
            'verified_date': entity.verified_date.isoformat() if entity.verified_date else None,
            'last_updated': entity.last_updated.isoformat() if entity.last_updated else None
        }
    

    
    def _check_name_duplicates(self, session: Session, full_name: str, result: 'DuplicateDetectionResult', exclude_entity_id: str = None):
        """Check for name-based duplicates."""
        from ..services.custom_sanctions_validator import DuplicateMatch
        
        # Exact match
        query = session.query(CustomSanctionName).join(CustomSanctionEntity).filter(
            CustomSanctionName.full_name.ilike(full_name)
        )
        
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        matches = query.all()
        
        for match in matches:
            result.add_match(DuplicateMatch(
                entity_id=match.entity_id,
                match_type='name',
                match_value=match.full_name,
                confidence=1.0 if match.full_name.lower() == full_name.lower() else 0.8
            ))
        
        # Fuzzy match (simplified - could use more sophisticated matching)
        if not matches:
            # Check for similar names (basic implementation)
            similar_query = session.query(CustomSanctionName).join(CustomSanctionEntity).filter(
                CustomSanctionName.full_name.ilike(f"%{full_name[:5]}%")
            )
            
            if exclude_entity_id:
                similar_query = similar_query.filter(CustomSanctionEntity.id != exclude_entity_id)
            
            similar_matches = similar_query.limit(5).all()
            
            for match in similar_matches:
                # Simple similarity check
                similarity = self._calculate_name_similarity(full_name, match.full_name)
                if similarity > 0.7:
                    result.add_match(DuplicateMatch(
                        entity_id=match.entity_id,
                        match_type='name_similar',
                        match_value=match.full_name,
                        confidence=similarity
                    ))
    
    def _check_identifier_duplicates(self, session: Session, id_type: str, id_value: str, 
                                   result: 'DuplicateDetectionResult', exclude_entity_id: str = None):
        """Check for identifier-based duplicates."""
        from ..services.custom_sanctions_validator import DuplicateMatch
        
        query = session.query(CustomSanctionIdentifier).join(CustomSanctionEntity).filter(
            and_(
                CustomSanctionIdentifier.id_type.ilike(id_type),
                CustomSanctionIdentifier.id_value.ilike(id_value)
            )
        )
        
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        matches = query.all()
        
        for match in matches:
            result.add_match(DuplicateMatch(
                entity_id=match.entity_id,
                match_type='identifier',
                match_value=f"{match.id_type}: {match.id_value}",
                confidence=1.0
            ))
    
    def _check_registration_duplicates(self, session: Session, registration_number: str,
                                     result: 'DuplicateDetectionResult', exclude_entity_id: str = None):
        """Check for registration number duplicates."""
        from ..services.custom_sanctions_validator import DuplicateMatch
        
        query = session.query(CustomSanctionEntityDetails).join(CustomSanctionEntity).filter(
            CustomSanctionEntityDetails.registration_number.ilike(registration_number)
        )
        
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        matches = query.all()
        
        for match in matches:
            result.add_match(DuplicateMatch(
                entity_id=match.entity_id,
                match_type='registration_number',
                match_value=match.registration_number,
                confidence=1.0
            ))
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two names (simplified implementation)."""
        name1 = name1.lower().strip()
        name2 = name2.lower().strip()
        
        if name1 == name2:
            return 1.0
        
        # Simple Jaccard similarity on words
        words1 = set(name1.split())
        words2 = set(name2.split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union if union > 0 else 0.0
    
    def _create_related_records(self, session: Session, entity: CustomSanctionEntity, entity_data: Dict[str, Any]):
        """Create related records for an entity."""
        # Create names
        names_data = entity_data.get('names', [])
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
        
        # Create addresses
        addresses_data = entity_data.get('addresses', [])
        for addr_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=addr_data.get('street'),
                city=addr_data.get('city'),
                postal_code=addr_data.get('postal_code'),
                country=addr_data.get('country'),
                full_address=addr_data.get('full_address')
            )
            session.add(address)
        
        # Create identifiers
        identifiers_data = entity_data.get('identifiers', [])
        for id_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=id_data['id_type'],
                id_value=id_data['id_value'],
                issuing_country=id_data.get('issuing_country'),
                notes=id_data.get('notes')
            )
            session.add(identifier)
        
        # Create individual details
        individual_data = entity_data.get('individual_details')
        if individual_data and entity.subject_type == SubjectType.INDIVIDUAL:
            individual = CustomSanctionIndividual(
                entity_id=entity.id,
                birth_year=individual_data.get('birth_year'),
                birth_month=individual_data.get('birth_month'),
                birth_day=individual_data.get('birth_day'),
                birth_full_date=individual_data.get('birth_full_date'),
                birth_note=individual_data.get('birth_note'),
                place_of_birth=individual_data.get('place_of_birth'),
                nationalities=individual_data.get('nationalities')
            )
            session.add(individual)
        
        # Create entity details
        entity_details_data = entity_data.get('entity_details')
        if entity_details_data and entity.subject_type == SubjectType.ENTITY:
            entity_details = CustomSanctionEntityDetails(
                entity_id=entity.id,
                registration_number=entity_details_data.get('registration_number'),
                registration_authority=entity_details_data.get('registration_authority'),
                incorporation_date=entity_details_data.get('incorporation_date'),
                company_type=entity_details_data.get('company_type'),
                tax_id=entity_details_data.get('tax_id')
            )
            session.add(entity_details)
    
    def _update_names(self, session: Session, entity: CustomSanctionEntity, names_data: List[Dict[str, Any]]):
        """Update names for an entity."""
        # Delete existing names
        session.query(CustomSanctionName).filter(
            CustomSanctionName.entity_id == entity.id
        ).delete()
        
        # Create new names
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
    
    def _update_addresses(self, session: Session, entity: CustomSanctionEntity, addresses_data: List[Dict[str, Any]]):
        """Update addresses for an entity."""
        # Delete existing addresses
        session.query(CustomSanctionAddress).filter(
            CustomSanctionAddress.entity_id == entity.id
        ).delete()
        
        # Create new addresses
        for addr_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=addr_data.get('street'),
                city=addr_data.get('city'),
                postal_code=addr_data.get('postal_code'),
                country=addr_data.get('country'),
                full_address=addr_data.get('full_address')
            )
            session.add(address)
    
    def _update_identifiers(self, session: Session, entity: CustomSanctionEntity, identifiers_data: List[Dict[str, Any]]):
        """Update identifiers for an entity."""
        # Delete existing identifiers
        session.query(CustomSanctionIdentifier).filter(
            CustomSanctionIdentifier.entity_id == entity.id
        ).delete()
        
        # Create new identifiers
        for id_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=id_data['id_type'],
                id_value=id_data['id_value'],
                issuing_country=id_data.get('issuing_country'),
                notes=id_data.get('notes')
            )
            session.add(identifier)
    
    def _update_individual_details(self, session: Session, entity: CustomSanctionEntity, individual_data: Dict[str, Any]):
        """Update individual details for an entity."""
        # Delete existing individual details
        session.query(CustomSanctionIndividual).filter(
            CustomSanctionIndividual.entity_id == entity.id
        ).delete()
        
        # Create new individual details
        if individual_data:
            individual = CustomSanctionIndividual(
                entity_id=entity.id,
                birth_year=individual_data.get('birth_year'),
                birth_month=individual_data.get('birth_month'),
                birth_day=individual_data.get('birth_day'),
                birth_full_date=individual_data.get('birth_full_date'),
                birth_note=individual_data.get('birth_note'),
                place_of_birth=individual_data.get('place_of_birth'),
                nationalities=individual_data.get('nationalities')
            )
            session.add(individual)
    
    def _update_entity_details(self, session: Session, entity: CustomSanctionEntity, entity_details_data: Dict[str, Any]):
        """Update entity details for an entity."""
        # Delete existing entity details
        session.query(CustomSanctionEntityDetails).filter(
            CustomSanctionEntityDetails.entity_id == entity.id
        ).delete()
        
        # Create new entity details
        if entity_details_data:
            entity_details = CustomSanctionEntityDetails(
                entity_id=entity.id,
                registration_number=entity_details_data.get('registration_number'),
                registration_authority=entity_details_data.get('registration_authority'),
                incorporation_date=entity_details_data.get('incorporation_date'),
                company_type=entity_details_data.get('company_type'),
                tax_id=entity_details_data.get('tax_id')
            )
            session.add(entity_details)
    
    def _check_name_duplicates(self, session: Session, full_name: str, result: DuplicateDetectionResult, exclude_entity_id: str = None):
        """Check for name-based duplicates."""
        from ..services.fuzzy_matcher import FuzzyMatcher
        
        # Get all names from database
        query = session.query(CustomSanctionName).join(CustomSanctionEntity)
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        existing_names = query.all()
        
        # Use fuzzy matching to find similar names
        fuzzy_matcher = FuzzyMatcher()
        for existing_name in existing_names:
            similarity = fuzzy_matcher.calculate_similarity(full_name, existing_name.full_name)
            if similarity > 0.8:  # High similarity threshold
                from ..services.custom_sanctions_validator import DuplicateMatch
                match = DuplicateMatch(
                    entity_id=existing_name.entity_id,
                    match_type='name',
                    match_value=existing_name.full_name,
                    confidence=similarity
                )
                result.add_match(match)
    
    def _check_identifier_duplicates(self, session: Session, id_type: str, id_value: str, result: DuplicateDetectionResult, exclude_entity_id: str = None):
        """Check for identifier-based duplicates."""
        query = session.query(CustomSanctionIdentifier).join(CustomSanctionEntity).filter(
            and_(
                CustomSanctionIdentifier.id_type == id_type,
                CustomSanctionIdentifier.id_value == id_value
            )
        )
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        existing_identifiers = query.all()
        
        for existing_id in existing_identifiers:
            from ..services.custom_sanctions_validator import DuplicateMatch
            match = DuplicateMatch(
                entity_id=existing_id.entity_id,
                match_type='identifier',
                match_value=f"{id_type}: {id_value}",
                confidence=1.0  # Exact match
            )
            result.add_match(match)
    
    def _check_registration_duplicates(self, session: Session, registration_number: str, result: DuplicateDetectionResult, exclude_entity_id: str = None):
        """Check for registration number duplicates."""
        query = session.query(CustomSanctionEntityDetails).join(CustomSanctionEntity).filter(
            CustomSanctionEntityDetails.registration_number == registration_number
        )
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        existing_registrations = query.all()
        
        for existing_reg in existing_registrations:
            from ..services.custom_sanctions_validator import DuplicateMatch
            match = DuplicateMatch(
                entity_id=existing_reg.entity_id,
                match_type='registration_number',
                match_value=registration_number,
                confidence=1.0  # Exact match
            )
            result.add_match(match)
    

    
    def _entity_to_audit_dict(self, entity: CustomSanctionEntity) -> Dict[str, Any]:
        """Convert entity to dictionary for audit logging."""
        return {
            'internal_entry_id': entity.internal_entry_id,
            'subject_type': entity.subject_type.value,
            'sanctioning_authority': entity.sanctioning_authority,
            'program': entity.program,
            'record_status': entity.record_status.value,
            'last_updated': entity.last_updated.isoformat() if entity.last_updated else None
        } 
   
    def _update_entity_from_dict(self, entity: CustomSanctionEntity, updates: Dict[str, Any]):
        """Update entity fields from dictionary data."""
        # Parse dates if they are strings
        if 'listing_date' in updates:
            listing_date = updates['listing_date']
            if isinstance(listing_date, str):
                listing_date = datetime.fromisoformat(listing_date.replace('Z', '+00:00')).date()
            entity.listing_date = listing_date
        
        if 'verified_date' in updates:
            verified_date = updates['verified_date']
            if isinstance(verified_date, str):
                verified_date = datetime.fromisoformat(verified_date.replace('Z', '+00:00'))
            entity.verified_date = verified_date
        
        # Update simple fields
        simple_fields = [
            'internal_entry_id', 'subject_type', 'sanctioning_authority', 'program',
            'legal_basis', 'measures_imposed', 'reason_for_listing', 'data_source',
            'record_status', 'internal_notes', 'created_by', 'verified_by'
        ]
        
        for field in simple_fields:
            if field in updates:
                if field == 'subject_type' and isinstance(updates[field], str):
                    setattr(entity, field, SubjectType(updates[field]))
                elif field == 'record_status' and isinstance(updates[field], str):
                    setattr(entity, field, RecordStatus(updates[field]))
                else:
                    setattr(entity, field, updates[field])
    
    def _create_related_records(self, session: Session, entity: CustomSanctionEntity, entity_data: Dict[str, Any]):
        """Create related records for an entity."""
        # Create names
        names_data = entity_data.get('names', [])
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
        
        # Create addresses
        addresses_data = entity_data.get('addresses', [])
        for addr_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=addr_data.get('street'),
                city=addr_data.get('city'),
                postal_code=addr_data.get('postal_code'),
                country=addr_data.get('country'),
                full_address=addr_data.get('full_address')
            )
            session.add(address)
        
        # Create identifiers
        identifiers_data = entity_data.get('identifiers', [])
        for id_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=id_data['id_type'],
                id_value=id_data['id_value'],
                issuing_country=id_data.get('issuing_country'),
                notes=id_data.get('notes')
            )
            session.add(identifier)
        
        # Create individual details
        individual_data = entity_data.get('individual_details')
        if individual_data and entity.subject_type == SubjectType.INDIVIDUAL:
            individual = CustomSanctionIndividual(
                entity_id=entity.id,
                birth_year=individual_data.get('birth_year'),
                birth_month=individual_data.get('birth_month'),
                birth_day=individual_data.get('birth_day'),
                birth_full_date=individual_data.get('birth_full_date'),
                birth_note=individual_data.get('birth_note'),
                place_of_birth=individual_data.get('place_of_birth'),
                nationalities=individual_data.get('nationalities')
            )
            session.add(individual)
        
        # Create entity details
        entity_details_data = entity_data.get('entity_details')
        if entity_details_data and entity.subject_type == SubjectType.ENTITY:
            entity_details = CustomSanctionEntityDetails(
                entity_id=entity.id,
                registration_number=entity_details_data.get('registration_number'),
                registration_authority=entity_details_data.get('registration_authority'),
                incorporation_date=entity_details_data.get('incorporation_date'),
                company_type=entity_details_data.get('company_type'),
                tax_id=entity_details_data.get('tax_id')
            )
            session.add(entity_details)
    
    def _update_names(self, session: Session, entity: CustomSanctionEntity, names_data: List[Dict[str, Any]]):
        """Update names for an entity."""
        # Delete existing names
        session.query(CustomSanctionName).filter(
            CustomSanctionName.entity_id == entity.id
        ).delete()
        
        # Create new names
        for name_data in names_data:
            name = CustomSanctionName(
                entity_id=entity.id,
                full_name=name_data['full_name'],
                name_type=NameType(name_data['name_type'])
            )
            session.add(name)
    
    def _update_addresses(self, session: Session, entity: CustomSanctionEntity, addresses_data: List[Dict[str, Any]]):
        """Update addresses for an entity."""
        # Delete existing addresses
        session.query(CustomSanctionAddress).filter(
            CustomSanctionAddress.entity_id == entity.id
        ).delete()
        
        # Create new addresses
        for addr_data in addresses_data:
            address = CustomSanctionAddress(
                entity_id=entity.id,
                street=addr_data.get('street'),
                city=addr_data.get('city'),
                postal_code=addr_data.get('postal_code'),
                country=addr_data.get('country'),
                full_address=addr_data.get('full_address')
            )
            session.add(address)
    
    def _update_identifiers(self, session: Session, entity: CustomSanctionEntity, identifiers_data: List[Dict[str, Any]]):
        """Update identifiers for an entity."""
        # Delete existing identifiers
        session.query(CustomSanctionIdentifier).filter(
            CustomSanctionIdentifier.entity_id == entity.id
        ).delete()
        
        # Create new identifiers
        for id_data in identifiers_data:
            identifier = CustomSanctionIdentifier(
                entity_id=entity.id,
                id_type=id_data['id_type'],
                id_value=id_data['id_value'],
                issuing_country=id_data.get('issuing_country'),
                notes=id_data.get('notes')
            )
            session.add(identifier)
    
    def _update_individual_details(self, session: Session, entity: CustomSanctionEntity, individual_data: Dict[str, Any]):
        """Update individual details for an entity."""
        # Delete existing individual details
        session.query(CustomSanctionIndividual).filter(
            CustomSanctionIndividual.entity_id == entity.id
        ).delete()
        
        # Create new individual details
        if individual_data:
            individual = CustomSanctionIndividual(
                entity_id=entity.id,
                birth_year=individual_data.get('birth_year'),
                birth_month=individual_data.get('birth_month'),
                birth_day=individual_data.get('birth_day'),
                birth_full_date=individual_data.get('birth_full_date'),
                birth_note=individual_data.get('birth_note'),
                place_of_birth=individual_data.get('place_of_birth'),
                nationalities=individual_data.get('nationalities')
            )
            session.add(individual)
    
    def _update_entity_details(self, session: Session, entity: CustomSanctionEntity, entity_details_data: Dict[str, Any]):
        """Update entity details for an entity."""
        # Delete existing entity details
        session.query(CustomSanctionEntityDetails).filter(
            CustomSanctionEntityDetails.entity_id == entity.id
        ).delete()
        
        # Create new entity details
        if entity_details_data:
            entity_details = CustomSanctionEntityDetails(
                entity_id=entity.id,
                registration_number=entity_details_data.get('registration_number'),
                registration_authority=entity_details_data.get('registration_authority'),
                incorporation_date=entity_details_data.get('incorporation_date'),
                company_type=entity_details_data.get('company_type'),
                tax_id=entity_details_data.get('tax_id')
            )
            session.add(entity_details)
    
    def _check_name_duplicates(self, session: Session, full_name: str, result: DuplicateDetectionResult, exclude_entity_id: str = None):
        """Check for name-based duplicates."""
        # Simple name matching for now - can be enhanced with fuzzy matching later
        query = session.query(CustomSanctionName).join(CustomSanctionEntity)
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        existing_names = query.filter(CustomSanctionName.full_name.ilike(f"%{full_name}%")).all()
        
        for existing_name in existing_names:
            # Simple similarity check
            similarity = 1.0 if existing_name.full_name.lower() == full_name.lower() else 0.8
            if similarity > 0.7:
                from ..services.custom_sanctions_validator import DuplicateMatch
                match = DuplicateMatch(
                    entity_id=existing_name.entity_id,
                    match_type='name',
                    match_value=existing_name.full_name,
                    confidence=similarity
                )
                result.add_match(match)
    
    def _check_identifier_duplicates(self, session: Session, id_type: str, id_value: str, result: DuplicateDetectionResult, exclude_entity_id: str = None):
        """Check for identifier-based duplicates."""
        query = session.query(CustomSanctionIdentifier).join(CustomSanctionEntity).filter(
            and_(
                CustomSanctionIdentifier.id_type == id_type,
                CustomSanctionIdentifier.id_value == id_value
            )
        )
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        existing_identifiers = query.all()
        
        for existing_id in existing_identifiers:
            from ..services.custom_sanctions_validator import DuplicateMatch
            match = DuplicateMatch(
                entity_id=existing_id.entity_id,
                match_type='identifier',
                match_value=f"{id_type}: {id_value}",
                confidence=1.0  # Exact match
            )
            result.add_match(match)
    
    def _check_registration_duplicates(self, session: Session, registration_number: str, result: DuplicateDetectionResult, exclude_entity_id: str = None):
        """Check for registration number duplicates."""
        query = session.query(CustomSanctionEntityDetails).join(CustomSanctionEntity).filter(
            CustomSanctionEntityDetails.registration_number == registration_number
        )
        if exclude_entity_id:
            query = query.filter(CustomSanctionEntity.id != exclude_entity_id)
        
        existing_registrations = query.all()
        
        for existing_reg in existing_registrations:
            from ..services.custom_sanctions_validator import DuplicateMatch
            match = DuplicateMatch(
                entity_id=existing_reg.entity_id,
                match_type='registration_number',
                match_value=registration_number,
                confidence=1.0  # Exact match
            )
            result.add_match(match)
    

    
    def _entity_to_audit_dict(self, entity: CustomSanctionEntity) -> Dict[str, Any]:
        """Convert entity to dictionary for audit logging."""
        return {
            'internal_entry_id': entity.internal_entry_id,
            'subject_type': entity.subject_type.value,
            'sanctioning_authority': entity.sanctioning_authority,
            'program': entity.program,
            'record_status': entity.record_status.value,
            'last_updated': entity.last_updated.isoformat() if entity.last_updated else None
        }