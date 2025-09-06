"""
SanctionedEntity model for storing sanctions data.
"""
from sqlalchemy import Column, String, DateTime, Text, JSON
from sqlalchemy.sql import func
from .base import Base, generate_uuid, get_current_timestamp


class SanctionedEntity(Base):
    """
    Model representing a sanctioned individual or entity.
    
    Attributes:
        id: Unique identifier for the entity
        name: Primary name of the sanctioned entity
        aliases: JSON field containing list of alternative names
        entity_type: Type of entity (INDIVIDUAL, COMPANY, etc.)
        sanctions_type: Type of sanctions applied
        effective_date: When the sanctions became effective
        source: Source of the sanctions data (EU, UN, OFAC, etc.)
        source_version: Version of the source data
        additional_info: JSON field for additional metadata
        created_at: Timestamp when record was created
        updated_at: Timestamp when record was last updated
    """
    __tablename__ = 'sanctioned_entities'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    name = Column(String(500), nullable=False, index=True)
    aliases = Column(JSON, default=list)  # List of alternative names
    entity_type = Column(String(50), nullable=False, index=True)  # INDIVIDUAL, COMPANY
    sanctions_type = Column(String(200), nullable=False)
    effective_date = Column(DateTime, nullable=True)
    source = Column(String(50), nullable=False, index=True)  # EU, UN, OFAC, etc.
    source_version = Column(String(100), nullable=False)
    additional_info = Column(JSON, default=dict)  # Additional metadata
    created_at = Column(DateTime, default=get_current_timestamp, nullable=False)
    updated_at = Column(DateTime, default=get_current_timestamp, onupdate=get_current_timestamp, nullable=False)
    
    def __repr__(self):
        return f"<SanctionedEntity(id='{self.id}', name='{self.name}', type='{self.entity_type}')>"
    
    def to_dict(self):
        """Convert the entity to a dictionary representation."""
        return {
            'id': self.id,
            'name': self.name,
            'aliases': self.aliases or [],
            'entity_type': self.entity_type,
            'sanctions_type': self.sanctions_type,
            'effective_date': self.effective_date.isoformat() if self.effective_date else None,
            'source': self.source,
            'source_version': self.source_version,
            'additional_info': self.additional_info or {},
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def get_all_names(self):
        """Get all names including primary name and aliases."""
        names = [self.name]
        if self.aliases:
            names.extend(self.aliases)
        return names