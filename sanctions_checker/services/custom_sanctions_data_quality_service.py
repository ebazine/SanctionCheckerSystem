"""
Custom Sanctions Data Quality Service

This service provides comprehensive data quality analysis, maintenance operations,
and bulk update capabilities for custom sanctions data.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, desc

from ..database.manager import DatabaseManager
from ..models import (
    CustomSanctionEntity, CustomSanctionName, CustomSanctionIndividual,
    CustomSanctionEntityDetails, CustomSanctionAddress, CustomSanctionIdentifier
)
from ..models.base import SubjectType, NameType, RecordStatus
from ..services.custom_sanctions_validator import CustomSanctionsValidator, ValidationResult

logger = logging.getLogger(__name__)


@dataclass
class DataQualityIssue:
    """Represents a data quality issue for an entity."""
    entity_id: str
    internal_entry_id: str
    issue_type: str  # 'missing_field', 'invalid_format', 'outdated', 'duplicate'
    severity: str    # 'critical', 'medium', 'low'
    field_name: str
    description: str
    suggested_action: str


@dataclass
class CompletenessStats:
    """Statistics about data completeness."""
    total_entities: int
    complete_entities: int
    incomplete_entities: int
    critical_issues: int
    medium_issues: int
    low_issues: int
    outdated_entries: int
    avg_completeness_score: float


@dataclass
class DataQualityReport:
    """Comprehensive data quality report."""
    generated_at: datetime
    total_entities: int
    completeness_stats: CompletenessStats
    issues_by_severity: Dict[str, int]
    issues_by_type: Dict[str, int]
    entities_by_status: Dict[str, int]
    entities_by_subject_type: Dict[str, int]
    recent_activity: Dict[str, int]
    top_issues: List[DataQualityIssue]
    outdated_entities: List[str]
    incomplete_entities: List[str]


@dataclass
class BulkUpdateResult:
    """Result of a bulk update operation."""
    total_processed: int
    successful_updates: int
    failed_updates: int
    errors: List[str]
    updated_entity_ids: List[str]


class CustomSanctionsDataQualityService:
    """Service for data quality analysis and maintenance operations."""
    
    def __init__(self, database_manager: DatabaseManager = None):
        """Initialize the data quality service."""
        self.db_manager = database_manager or DatabaseManager()
        if not self.db_manager.engine:
            self.db_manager.initialize_database()
        self.validator = CustomSanctionsValidator()
    
    def generate_quality_report(self) -> DataQualityReport:
        """
        Generate a comprehensive data quality report.
        
        Returns:
            DataQualityReport with detailed analysis
        """
        session = self.db_manager.get_session()
        
        try:
            # Get all entities for analysis
            entities = session.query(CustomSanctionEntity).all()
            total_entities = len(entities)
            
            if total_entities == 0:
                return self._empty_quality_report()
            
            # Initialize counters
            issues_by_severity = {'critical': 0, 'medium': 0, 'low': 0}
            issues_by_type = {}
            entities_by_status = {}
            entities_by_subject_type = {}
            all_issues = []
            outdated_entities = []
            incomplete_entities = []
            
            complete_entities = 0
            total_completeness_score = 0
            
            # Analyze each entity
            for entity in entities:
                # Count by status and subject type
                status = entity.record_status.value if entity.record_status else 'Unknown'
                entities_by_status[status] = entities_by_status.get(status, 0) + 1
                
                subject_type = entity.subject_type.value if entity.subject_type else 'Unknown'
                entities_by_subject_type[subject_type] = entities_by_subject_type.get(subject_type, 0) + 1
                
                # Analyze entity quality
                entity_issues, completeness_score = self._analyze_entity_quality(entity)
                total_completeness_score += completeness_score
                
                if completeness_score >= 0.9:  # 90% complete
                    complete_entities += 1
                else:
                    incomplete_entities.append(entity.internal_entry_id)
                
                # Check if entity is outdated (not updated in 6 months)
                if self._is_entity_outdated(entity):
                    outdated_entities.append(entity.internal_entry_id)
                
                # Collect issues
                for issue in entity_issues:
                    all_issues.append(issue)
                    issues_by_severity[issue.severity] += 1
                    issues_by_type[issue.issue_type] = issues_by_type.get(issue.issue_type, 0) + 1
            
            # Calculate completeness stats
            completeness_stats = CompletenessStats(
                total_entities=total_entities,
                complete_entities=complete_entities,
                incomplete_entities=total_entities - complete_entities,
                critical_issues=issues_by_severity['critical'],
                medium_issues=issues_by_severity['medium'],
                low_issues=issues_by_severity['low'],
                outdated_entries=len(outdated_entities),
                avg_completeness_score=total_completeness_score / total_entities if total_entities > 0 else 0
            )
            
            # Get recent activity (last 30 days)
            recent_activity = self._get_recent_activity(session)
            
            # Get top issues (most critical first)
            top_issues = sorted(all_issues, key=lambda x: (
                {'critical': 0, 'medium': 1, 'low': 2}[x.severity],
                x.issue_type
            ))[:50]  # Limit to top 50 issues
            
            return DataQualityReport(
                generated_at=datetime.utcnow(),
                total_entities=total_entities,
                completeness_stats=completeness_stats,
                issues_by_severity=issues_by_severity,
                issues_by_type=issues_by_type,
                entities_by_status=entities_by_status,
                entities_by_subject_type=entities_by_subject_type,
                recent_activity=recent_activity,
                top_issues=top_issues,
                outdated_entities=outdated_entities,
                incomplete_entities=incomplete_entities
            )
            
        finally:
            self.db_manager.close_session(session)
    
    def get_entities_with_issues(self, issue_type: str = None, severity: str = None, 
                               limit: int = 100) -> List[Tuple[CustomSanctionEntity, List[DataQualityIssue]]]:
        """
        Get entities that have data quality issues.
        
        Args:
            issue_type: Optional filter by issue type
            severity: Optional filter by severity
            limit: Maximum number of entities to return
            
        Returns:
            List of tuples (entity, issues)
        """
        session = self.db_manager.get_session()
        
        try:
            entities = session.query(CustomSanctionEntity).limit(limit).all()
            results = []
            
            for entity in entities:
                entity_issues, _ = self._analyze_entity_quality(entity)
                
                # Filter issues if criteria provided
                filtered_issues = entity_issues
                if issue_type:
                    filtered_issues = [i for i in filtered_issues if i.issue_type == issue_type]
                if severity:
                    filtered_issues = [i for i in filtered_issues if i.severity == severity]
                
                if filtered_issues:
                    results.append((entity, filtered_issues))
            
            return results
            
        finally:
            self.db_manager.close_session(session)
    
    def bulk_update_status(self, entity_ids: List[str], new_status: RecordStatus, 
                          user_id: str = None) -> BulkUpdateResult:
        """
        Bulk update the status of multiple entities.
        
        Args:
            entity_ids: List of entity IDs to update
            new_status: New status to set
            user_id: Optional user ID for audit logging
            
        Returns:
            BulkUpdateResult with operation statistics
        """
        session = self.db_manager.get_session()
        
        try:
            successful_updates = []
            errors = []
            
            for entity_id in entity_ids:
                try:
                    entity = session.query(CustomSanctionEntity).filter(
                        CustomSanctionEntity.id == entity_id
                    ).first()
                    
                    if not entity:
                        errors.append(f"Entity not found: {entity_id}")
                        continue
                    
                    old_status = entity.record_status
                    entity.record_status = new_status
                    entity.last_updated = datetime.utcnow()
                    
                    successful_updates.append(entity_id)
                    
                    logger.info(f"Updated entity {entity.internal_entry_id} status: {old_status} -> {new_status}")
                    
                except Exception as e:
                    errors.append(f"Error updating entity {entity_id}: {str(e)}")
                    logger.error(f"Error in bulk status update for {entity_id}: {e}")
            
            session.commit()
            
            result = BulkUpdateResult(
                total_processed=len(entity_ids),
                successful_updates=len(successful_updates),
                failed_updates=len(errors),
                errors=errors,
                updated_entity_ids=successful_updates
            )
            
            logger.info(f"Bulk status update completed: {len(successful_updates)} successful, {len(errors)} failed")
            return result
            
        except Exception as e:
            session.rollback()
            logger.error(f"Bulk status update failed: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def bulk_update_field(self, entity_ids: List[str], field_name: str, 
                         field_value: Any, user_id: str = None) -> BulkUpdateResult:
        """
        Bulk update a specific field for multiple entities.
        
        Args:
            entity_ids: List of entity IDs to update
            field_name: Name of the field to update
            field_value: New value for the field
            user_id: Optional user ID for audit logging
            
        Returns:
            BulkUpdateResult with operation statistics
        """
        session = self.db_manager.get_session()
        
        try:
            successful_updates = []
            errors = []
            
            for entity_id in entity_ids:
                try:
                    entity = session.query(CustomSanctionEntity).filter(
                        CustomSanctionEntity.id == entity_id
                    ).first()
                    
                    if not entity:
                        errors.append(f"Entity not found: {entity_id}")
                        continue
                    
                    # Check if field exists on entity
                    if not hasattr(entity, field_name):
                        errors.append(f"Field '{field_name}' not found on entity {entity_id}")
                        continue
                    
                    # Update field
                    setattr(entity, field_name, field_value)
                    entity.last_updated = datetime.utcnow()
                    
                    successful_updates.append(entity_id)
                    
                    logger.info(f"Updated entity {entity.internal_entry_id} field {field_name}")
                    
                except Exception as e:
                    errors.append(f"Error updating entity {entity_id}: {str(e)}")
                    logger.error(f"Error in bulk field update for {entity_id}: {e}")
            
            session.commit()
            
            result = BulkUpdateResult(
                total_processed=len(entity_ids),
                successful_updates=len(successful_updates),
                failed_updates=len(errors),
                errors=errors,
                updated_entity_ids=successful_updates
            )
            
            logger.info(f"Bulk field update completed: {len(successful_updates)} successful, {len(errors)} failed")
            return result
            
        except Exception as e:
            session.rollback()
            logger.error(f"Bulk field update failed: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def mark_entities_as_verified(self, entity_ids: List[str], 
                                 user_id: str = None) -> BulkUpdateResult:
        """
        Mark multiple entities as verified.
        
        Args:
            entity_ids: List of entity IDs to mark as verified
            user_id: User ID performing the verification
            
        Returns:
            BulkUpdateResult with operation statistics
        """
        session = self.db_manager.get_session()
        
        try:
            successful_updates = []
            errors = []
            verification_date = datetime.utcnow()
            
            for entity_id in entity_ids:
                try:
                    entity = session.query(CustomSanctionEntity).filter(
                        CustomSanctionEntity.id == entity_id
                    ).first()
                    
                    if not entity:
                        errors.append(f"Entity not found: {entity_id}")
                        continue
                    
                    entity.verified_by = user_id
                    entity.verified_date = verification_date
                    entity.last_updated = verification_date
                    
                    successful_updates.append(entity_id)
                    
                    logger.info(f"Marked entity {entity.internal_entry_id} as verified by {user_id}")
                    
                except Exception as e:
                    errors.append(f"Error verifying entity {entity_id}: {str(e)}")
                    logger.error(f"Error in bulk verification for {entity_id}: {e}")
            
            session.commit()
            
            result = BulkUpdateResult(
                total_processed=len(entity_ids),
                successful_updates=len(successful_updates),
                failed_updates=len(errors),
                errors=errors,
                updated_entity_ids=successful_updates
            )
            
            logger.info(f"Bulk verification completed: {len(successful_updates)} successful, {len(errors)} failed")
            return result
            
        except Exception as e:
            session.rollback()
            logger.error(f"Bulk verification failed: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def cleanup_outdated_entities(self, days_threshold: int = 180, 
                                 dry_run: bool = True) -> BulkUpdateResult:
        """
        Clean up entities that haven't been updated in a specified time period.
        
        Args:
            days_threshold: Number of days to consider an entity outdated
            dry_run: If True, only return what would be cleaned up without making changes
            
        Returns:
            BulkUpdateResult with cleanup statistics
        """
        session = self.db_manager.get_session()
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_threshold)
            
            # Find outdated entities
            outdated_entities = session.query(CustomSanctionEntity).filter(
                and_(
                    CustomSanctionEntity.last_updated < cutoff_date,
                    CustomSanctionEntity.record_status == RecordStatus.ACTIVE
                )
            ).all()
            
            if dry_run:
                return BulkUpdateResult(
                    total_processed=len(outdated_entities),
                    successful_updates=0,
                    failed_updates=0,
                    errors=[],
                    updated_entity_ids=[e.id for e in outdated_entities]
                )
            
            # Update status to inactive
            successful_updates = []
            errors = []
            
            for entity in outdated_entities:
                try:
                    entity.record_status = RecordStatus.INACTIVE
                    entity.last_updated = datetime.utcnow()
                    successful_updates.append(entity.id)
                    
                    logger.info(f"Marked outdated entity {entity.internal_entry_id} as inactive")
                    
                except Exception as e:
                    errors.append(f"Error updating entity {entity.id}: {str(e)}")
                    logger.error(f"Error in cleanup for {entity.id}: {e}")
            
            session.commit()
            
            result = BulkUpdateResult(
                total_processed=len(outdated_entities),
                successful_updates=len(successful_updates),
                failed_updates=len(errors),
                errors=errors,
                updated_entity_ids=successful_updates
            )
            
            logger.info(f"Cleanup completed: {len(successful_updates)} entities marked inactive")
            return result
            
        except Exception as e:
            session.rollback()
            logger.error(f"Cleanup operation failed: {e}")
            raise
        finally:
            self.db_manager.close_session(session)
    
    def _analyze_entity_quality(self, entity: CustomSanctionEntity) -> Tuple[List[DataQualityIssue], float]:
        """
        Analyze the data quality of a single entity.
        
        Args:
            entity: Entity to analyze
            
        Returns:
            Tuple of (issues_list, completeness_score)
        """
        issues = []
        completeness_score = 0.0
        total_fields = 0
        complete_fields = 0
        
        # Check core required fields
        core_fields = [
            ('internal_entry_id', 'Internal Entry ID'),
            ('subject_type', 'Subject Type'),
            ('sanctioning_authority', 'Sanctioning Authority'),
            ('program', 'Program'),
            ('listing_date', 'Listing Date'),
            ('data_source', 'Data Source')
        ]
        
        for field_name, display_name in core_fields:
            total_fields += 1
            value = getattr(entity, field_name, None)
            
            if not value:
                issues.append(DataQualityIssue(
                    entity_id=entity.id,
                    internal_entry_id=entity.internal_entry_id or 'Unknown',
                    issue_type='missing_field',
                    severity='critical',
                    field_name=field_name,
                    description=f'Missing required field: {display_name}',
                    suggested_action=f'Add {display_name} to complete the entity'
                ))
            else:
                complete_fields += 1
        
        # Check if entity has at least one name
        if not entity.names or len(entity.names) == 0:
            issues.append(DataQualityIssue(
                entity_id=entity.id,
                internal_entry_id=entity.internal_entry_id or 'Unknown',
                issue_type='missing_field',
                severity='critical',
                field_name='names',
                description='Entity has no names defined',
                suggested_action='Add at least one name for the entity'
            ))
        else:
            complete_fields += 1
        total_fields += 1
        
        # Check optional but important fields
        optional_fields = [
            ('legal_basis', 'Legal Basis'),
            ('measures_imposed', 'Measures Imposed'),
            ('reason_for_listing', 'Reason for Listing')
        ]
        
        for field_name, display_name in optional_fields:
            total_fields += 1
            value = getattr(entity, field_name, None)
            
            if not value:
                issues.append(DataQualityIssue(
                    entity_id=entity.id,
                    internal_entry_id=entity.internal_entry_id or 'Unknown',
                    issue_type='missing_field',
                    severity='medium',
                    field_name=field_name,
                    description=f'Missing optional field: {display_name}',
                    suggested_action=f'Consider adding {display_name} for completeness'
                ))
            else:
                complete_fields += 1
        
        # Check subject-specific requirements
        if entity.subject_type == SubjectType.INDIVIDUAL:
            if not entity.individual_details:
                issues.append(DataQualityIssue(
                    entity_id=entity.id,
                    internal_entry_id=entity.internal_entry_id or 'Unknown',
                    issue_type='missing_field',
                    severity='medium',
                    field_name='individual_details',
                    description='Individual entity missing personal details',
                    suggested_action='Add birth information and nationalities'
                ))
            total_fields += 1
            if entity.individual_details:
                complete_fields += 1
        
        elif entity.subject_type == SubjectType.ENTITY:
            if not entity.entity_details:
                issues.append(DataQualityIssue(
                    entity_id=entity.id,
                    internal_entry_id=entity.internal_entry_id or 'Unknown',
                    issue_type='missing_field',
                    severity='medium',
                    field_name='entity_details',
                    description='Entity missing business details',
                    suggested_action='Add registration and incorporation information'
                ))
            total_fields += 1
            if entity.entity_details:
                complete_fields += 1
        
        # Check for outdated information
        if self._is_entity_outdated(entity):
            issues.append(DataQualityIssue(
                entity_id=entity.id,
                internal_entry_id=entity.internal_entry_id or 'Unknown',
                issue_type='outdated',
                severity='low',
                field_name='last_updated',
                description='Entity has not been updated recently',
                suggested_action='Review and update entity information'
            ))
        
        # Calculate completeness score
        completeness_score = complete_fields / total_fields if total_fields > 0 else 0.0
        
        return issues, completeness_score
    
    def _is_entity_outdated(self, entity: CustomSanctionEntity, months_threshold: int = 6) -> bool:
        """Check if an entity is considered outdated."""
        if not entity.last_updated:
            return True
        
        threshold_date = datetime.utcnow() - timedelta(days=months_threshold * 30)
        return entity.last_updated < threshold_date
    
    def _get_recent_activity(self, session: Session) -> Dict[str, int]:
        """Get recent activity statistics."""
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        
        return {
            'created_last_30_days': session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.created_at >= thirty_days_ago
            ).count(),
            'updated_last_30_days': session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.last_updated >= thirty_days_ago
            ).count(),
            'created_last_7_days': session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.created_at >= seven_days_ago
            ).count(),
            'updated_last_7_days': session.query(CustomSanctionEntity).filter(
                CustomSanctionEntity.last_updated >= seven_days_ago
            ).count()
        }
    
    def _empty_quality_report(self) -> DataQualityReport:
        """Return an empty quality report when no entities exist."""
        return DataQualityReport(
            generated_at=datetime.utcnow(),
            total_entities=0,
            completeness_stats=CompletenessStats(
                total_entities=0,
                complete_entities=0,
                incomplete_entities=0,
                critical_issues=0,
                medium_issues=0,
                low_issues=0,
                outdated_entries=0,
                avg_completeness_score=0.0
            ),
            issues_by_severity={'critical': 0, 'medium': 0, 'low': 0},
            issues_by_type={},
            entities_by_status={},
            entities_by_subject_type={},
            recent_activity={},
            top_issues=[],
            outdated_entities=[],
            incomplete_entities=[]
        )