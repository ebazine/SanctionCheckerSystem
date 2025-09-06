"""
CustomSanctionEntityDetails model for storing entity-specific details.
"""
from sqlalchemy import Column, String, Date, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base, generate_uuid


class CustomSanctionEntityDetails(Base):
    """
    Model representing entity-specific details for custom sanction entities.
    
    This model stores registration information, incorporation details,
    and other business-related information for entities in the custom sanctions system.
    """
    __tablename__ = 'custom_sanction_entity_details'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    entity_id = Column(String(36), ForeignKey('custom_sanction_entities.id', ondelete='CASCADE'), nullable=False, unique=True, index=True)
    
    # Registration and incorporation details
    registration_number = Column(String(100), index=True)
    registration_authority = Column(String(200))
    incorporation_date = Column(Date)
    company_type = Column(String(100))
    tax_id = Column(String(100), index=True)
    
    # Relationship back to entity
    entity = relationship("CustomSanctionEntity", back_populates="entity_details")
    
    def __repr__(self):
        return f"<CustomSanctionEntityDetails(id='{self.id}', entity_id='{self.entity_id}', reg_number='{self.registration_number}')>"
    
    def to_dict(self):
        """Convert the entity details to a dictionary representation."""
        return {
            'id': self.id,
            'entity_id': self.entity_id,
            'registration_number': self.registration_number,
            'registration_authority': self.registration_authority,
            'incorporation_date': self.incorporation_date.isoformat() if self.incorporation_date else None,
            'company_type': self.company_type,
            'tax_id': self.tax_id
        }