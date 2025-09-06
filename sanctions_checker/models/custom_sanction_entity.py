"""
CustomSanctionEntity model for storing user-created sanctions data.
"""
from sqlalchemy import Column, String, DateTime, Text, Date, Integer, JSON, Enum
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import Base, generate_uuid, get_current_timestamp, SubjectType, RecordStatus


class CustomSanctionEntity(Base):
    """
    Model representing a custom sanction entity created by users.
    
    This model stores the core information for custom sanctions including
    individuals, entities, vessels, aircraft, and other subject types.
    """
    __tablename__ = 'custom_sanction_entities'
    
    # Core identification
    id = Column(String(36), primary_key=True, default=generate_uuid)
    internal_entry_id = Column(String(50), unique=True, nullable=False, index=True)
    subject_type = Column(Enum(SubjectType), nullable=False, index=True)
    
    # Sanction details
    sanctioning_authority = Column(String(200), nullable=False)
    program = Column(String(200), nullable=False)
    legal_basis = Column(Text)
    listing_date = Column(Date, nullable=False)
    measures_imposed = Column(Text)
    reason_for_listing = Column(Text)
    
    # Metadata
    data_source = Column(String(500), nullable=False)
    record_status = Column(Enum(RecordStatus), default=RecordStatus.ACTIVE, nullable=False, index=True)
    last_updated = Column(DateTime, default=get_current_timestamp, onupdate=get_current_timestamp, nullable=False)
    internal_notes = Column(Text)
    created_by = Column(String(100))
    verified_by = Column(String(100))
    verified_date = Column(DateTime)
    created_at = Column(DateTime, default=get_current_timestamp, nullable=False)
    
    # Relationships
    names = relationship("CustomSanctionName", back_populates="entity", cascade="all, delete-orphan")
    addresses = relationship("CustomSanctionAddress", back_populates="entity", cascade="all, delete-orphan")
    identifiers = relationship("CustomSanctionIdentifier", back_populates="entity", cascade="all, delete-orphan")
    individual_details = relationship("CustomSanctionIndividual", back_populates="entity", cascade="all, delete-orphan", uselist=False)
    entity_details = relationship("CustomSanctionEntityDetails", back_populates="entity", cascade="all, delete-orphan", uselist=False)
    audit_logs = relationship("CustomSanctionAuditLog", back_populates="entity", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<CustomSanctionEntity(id='{self.id}', internal_entry_id='{self.internal_entry_id}', subject_type='{self.subject_type.value}')>"
    
    def to_dict(self):
        """Convert the entity to a dictionary representation."""
        return {
            'id': self.id,
            'internal_entry_id': self.internal_entry_id,
            'subject_type': self.subject_type.value,
            'sanctioning_authority': self.sanctioning_authority,
            'program': self.program,
            'legal_basis': self.legal_basis,
            'listing_date': self.listing_date.isoformat() if self.listing_date else None,
            'measures_imposed': self.measures_imposed,
            'reason_for_listing': self.reason_for_listing,
            'data_source': self.data_source,
            'record_status': self.record_status.value,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None,
            'internal_notes': self.internal_notes,
            'created_by': self.created_by,
            'verified_by': self.verified_by,
            'verified_date': self.verified_date.isoformat() if self.verified_date else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'names': [name.to_dict() for name in self.names] if self.names else [],
            'addresses': [addr.to_dict() for addr in self.addresses] if self.addresses else [],
            'identifiers': [ident.to_dict() for ident in self.identifiers] if self.identifiers else [],
            'individual_details': self.individual_details.to_dict() if self.individual_details else None,
            'entity_details': self.entity_details.to_dict() if self.entity_details else None
        }
    
    def get_primary_name(self):
        """Get the primary name of the entity."""
        primary_names = [name for name in self.names if name.name_type.value == 'Primary']
        return primary_names[0].full_name if primary_names else None
    
    def get_all_names(self):
        """Get all names including primary name and aliases."""
        return [name.full_name for name in self.names] if self.names else []