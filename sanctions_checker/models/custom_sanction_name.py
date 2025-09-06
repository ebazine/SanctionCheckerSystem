"""
CustomSanctionName model for storing names and aliases of custom sanctions.
"""
from sqlalchemy import Column, String, Text, ForeignKey, Enum
from sqlalchemy.orm import relationship
from .base import Base, generate_uuid, NameType


class CustomSanctionName(Base):
    """
    Model representing names and aliases for custom sanction entities.
    
    This model stores all names associated with a custom sanction entity,
    including primary names, aliases, AKAs, FKAs, and low quality AKAs.
    """
    __tablename__ = 'custom_sanction_names'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    entity_id = Column(String(36), ForeignKey('custom_sanction_entities.id', ondelete='CASCADE'), nullable=False, index=True)
    full_name = Column(Text, nullable=False, index=True)
    name_type = Column(Enum(NameType), nullable=False, index=True)
    
    # Relationship back to entity
    entity = relationship("CustomSanctionEntity", back_populates="names")
    
    def __repr__(self):
        return f"<CustomSanctionName(id='{self.id}', name='{self.full_name}', type='{self.name_type.value}')>"
    
    def to_dict(self):
        """Convert the name to a dictionary representation."""
        return {
            'id': self.id,
            'entity_id': self.entity_id,
            'full_name': self.full_name,
            'name_type': self.name_type.value
        }