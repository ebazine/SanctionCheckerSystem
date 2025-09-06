"""
CustomSanctionAuditLog model for tracking changes to custom sanctions.
"""
from sqlalchemy import Column, String, DateTime, Text, JSON, ForeignKey, Enum
from sqlalchemy.orm import relationship
from .base import Base, generate_uuid, get_current_timestamp
import enum


class AuditAction(enum.Enum):
    """Enumeration for audit log actions."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    STATUS_CHANGE = "STATUS_CHANGE"


class CustomSanctionAuditLog(Base):
    """
    Model for tracking all changes made to custom sanction entities.
    
    This model provides a complete audit trail of all operations
    performed on custom sanction entities for compliance and tracking purposes.
    """
    __tablename__ = 'custom_sanction_audit_log'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    entity_id = Column(String(36), ForeignKey('custom_sanction_entities.id', ondelete='CASCADE'), nullable=False, index=True)
    
    # Audit information
    action = Column(Enum(AuditAction), nullable=False, index=True)
    user_id = Column(String(100), index=True)  # User who performed the action
    timestamp = Column(DateTime, default=get_current_timestamp, nullable=False, index=True)
    changes = Column(JSON)  # JSON object containing the changes made
    notes = Column(Text)  # Additional notes about the change
    
    # Relationship back to entity
    entity = relationship("CustomSanctionEntity", back_populates="audit_logs")
    
    def __repr__(self):
        return f"<CustomSanctionAuditLog(id='{self.id}', action='{self.action.value}', user='{self.user_id}')>"
    
    def to_dict(self):
        """Convert the audit log to a dictionary representation."""
        return {
            'id': self.id,
            'entity_id': self.entity_id,
            'action': self.action.value,
            'user_id': self.user_id,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'changes': self.changes or {},
            'notes': self.notes
        }