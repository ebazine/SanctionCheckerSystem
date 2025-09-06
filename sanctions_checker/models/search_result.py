"""
SearchResult model for storing individual search match results.
"""
from sqlalchemy import Column, String, Float, JSON, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base, generate_uuid


class SearchResult(Base):
    """
    Model representing an individual match result from a search operation.
    
    Attributes:
        id: Unique identifier for the search result
        search_record_id: Foreign key to the SearchRecord
        entity_id: ID of the matched sanctioned entity
        confidence_scores: JSON field containing algorithm-specific confidence scores
        match_details: JSON field containing detailed matching information
        overall_confidence: Calculated overall confidence score
        search_record: Relationship back to the SearchRecord
    """
    __tablename__ = 'search_results'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    search_record_id = Column(String(36), ForeignKey('search_records.id'), nullable=False, index=True)
    entity_id = Column(String(36), ForeignKey('sanctioned_entities.id'), nullable=False, index=True)
    confidence_scores = Column(JSON, default=dict)  # {algorithm: score} mapping
    match_details = Column(JSON, default=dict)  # Detailed matching information
    overall_confidence = Column(Float, nullable=False, index=True)  # Calculated overall score
    
    # Relationships
    search_record = relationship("SearchRecord", back_populates="results")
    entity = relationship("SanctionedEntity")
    
    def __repr__(self):
        return f"<SearchResult(id='{self.id}', entity_id='{self.entity_id}', confidence={self.overall_confidence:.3f})>"
    
    def to_dict(self):
        """Convert the search result to a dictionary representation."""
        return {
            'id': self.id,
            'search_record_id': self.search_record_id,
            'entity_id': self.entity_id,
            'confidence_scores': self.confidence_scores or {},
            'match_details': self.match_details or {},
            'overall_confidence': self.overall_confidence,
            'entity': self.entity.to_dict() if self.entity else None
        }
    
    def get_best_algorithm(self):
        """Get the algorithm that produced the highest confidence score."""
        if not self.confidence_scores:
            return None
        
        return max(self.confidence_scores.items(), key=lambda x: x[1])
    
    def get_confidence_level(self):
        """Get a human-readable confidence level."""
        if self.overall_confidence >= 0.8:
            return "HIGH"
        elif self.overall_confidence >= 0.6:
            return "MEDIUM"
        elif self.overall_confidence >= 0.4:
            return "LOW"
        else:
            return "VERY_LOW"
    
    def is_high_confidence_match(self, threshold=0.8):
        """Check if this is a high confidence match."""
        return self.overall_confidence >= threshold