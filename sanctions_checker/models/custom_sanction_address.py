"""
CustomSanctionAddress model for storing addresses of custom sanctions.
"""
from sqlalchemy import Column, String, Text, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base, generate_uuid


class CustomSanctionAddress(Base):
    """
    Model representing addresses for custom sanction entities.
    
    This model stores address information for custom sanction entities,
    supporting multiple addresses per entity.
    """
    __tablename__ = 'custom_sanction_addresses'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    entity_id = Column(String(36), ForeignKey('custom_sanction_entities.id', ondelete='CASCADE'), nullable=False, index=True)
    
    # Address components
    street = Column(Text)
    city = Column(String(100), index=True)
    postal_code = Column(String(20))
    country = Column(String(100), index=True)
    full_address = Column(Text, index=True)  # Complete address as single field
    
    # Relationship back to entity
    entity = relationship("CustomSanctionEntity", back_populates="addresses")
    
    def __repr__(self):
        return f"<CustomSanctionAddress(id='{self.id}', city='{self.city}', country='{self.country}')>"
    
    def to_dict(self):
        """Convert the address to a dictionary representation."""
        return {
            'id': self.id,
            'entity_id': self.entity_id,
            'street': self.street,
            'city': self.city,
            'postal_code': self.postal_code,
            'country': self.country,
            'full_address': self.full_address
        }
    
    def get_formatted_address(self):
        """Get a formatted address string."""
        if self.full_address:
            return self.full_address
        
        # Build address from components
        parts = []
        if self.street:
            parts.append(self.street)
        if self.city:
            parts.append(self.city)
        if self.postal_code:
            parts.append(self.postal_code)
        if self.country:
            parts.append(self.country)
        
        return ', '.join(parts) if parts else None