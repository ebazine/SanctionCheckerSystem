"""
CustomSanctionIdentifier model for storing identification documents and numbers.
"""
from sqlalchemy import Column, String, Text, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base, generate_uuid


class CustomSanctionIdentifier(Base):
    """
    Model representing identification documents and numbers for custom sanction entities.
    
    This model stores various types of identification including passports,
    national IDs, driver's licenses, and other identification documents.
    """
    __tablename__ = 'custom_sanction_identifiers'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    entity_id = Column(String(36), ForeignKey('custom_sanction_entities.id', ondelete='CASCADE'), nullable=False, index=True)
    
    # Identifier information
    id_type = Column(String(100), nullable=False, index=True)  # Passport, National ID, Driver's License, etc.
    id_value = Column(String(200), nullable=False, index=True)  # The actual ID number/value
    issuing_country = Column(String(100), index=True)  # Country that issued the ID
    notes = Column(Text)  # Additional notes about the identifier
    
    # Relationship back to entity
    entity = relationship("CustomSanctionEntity", back_populates="identifiers")
    
    def __repr__(self):
        return f"<CustomSanctionIdentifier(id='{self.id}', type='{self.id_type}', value='{self.id_value}')>"
    
    def to_dict(self):
        """Convert the identifier to a dictionary representation."""
        return {
            'id': self.id,
            'entity_id': self.entity_id,
            'id_type': self.id_type,
            'id_value': self.id_value,
            'issuing_country': self.issuing_country,
            'notes': self.notes
        }