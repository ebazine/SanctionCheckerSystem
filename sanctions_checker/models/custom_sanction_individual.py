"""
CustomSanctionIndividual model for storing individual-specific details.
"""
from sqlalchemy import Column, String, Integer, Date, Text, JSON, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base, generate_uuid


class CustomSanctionIndividual(Base):
    """
    Model representing individual-specific details for custom sanction entities.
    
    This model stores birth information, nationalities, and other details
    specific to individuals in the custom sanctions system.
    """
    __tablename__ = 'custom_sanction_individuals'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    entity_id = Column(String(36), ForeignKey('custom_sanction_entities.id', ondelete='CASCADE'), nullable=False, unique=True, index=True)
    
    # Birth information - flexible to handle partial dates
    birth_year = Column(Integer)
    birth_month = Column(Integer)
    birth_day = Column(Integer)
    birth_full_date = Column(Date)
    birth_note = Column(Text)  # For additional birth information or clarifications
    place_of_birth = Column(String(200))
    
    # Nationalities stored as JSON array for multiple nationalities
    nationalities = Column(JSON, default=list)
    
    # Relationship back to entity
    entity = relationship("CustomSanctionEntity", back_populates="individual_details")
    
    def __repr__(self):
        return f"<CustomSanctionIndividual(id='{self.id}', entity_id='{self.entity_id}')>"
    
    def to_dict(self):
        """Convert the individual details to a dictionary representation."""
        return {
            'id': self.id,
            'entity_id': self.entity_id,
            'birth_year': self.birth_year,
            'birth_month': self.birth_month,
            'birth_day': self.birth_day,
            'birth_full_date': self.birth_full_date.isoformat() if self.birth_full_date else None,
            'birth_note': self.birth_note,
            'place_of_birth': self.place_of_birth,
            'nationalities': self.nationalities or []
        }
    
    def get_birth_date_string(self):
        """Get a formatted birth date string based on available information."""
        if self.birth_full_date:
            return self.birth_full_date.strftime('%Y-%m-%d')
        elif self.birth_year and self.birth_month and self.birth_day:
            return f"{self.birth_year:04d}-{self.birth_month:02d}-{self.birth_day:02d}"
        elif self.birth_year and self.birth_month:
            return f"{self.birth_year:04d}-{self.birth_month:02d}"
        elif self.birth_year:
            return str(self.birth_year)
        else:
            return None
    
    def add_nationality(self, nationality):
        """Add a nationality to the list."""
        if not self.nationalities:
            self.nationalities = []
        if nationality not in self.nationalities:
            self.nationalities.append(nationality)
    
    def remove_nationality(self, nationality):
        """Remove a nationality from the list."""
        if self.nationalities and nationality in self.nationalities:
            self.nationalities.remove(nationality)