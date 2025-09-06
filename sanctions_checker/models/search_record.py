"""
SearchRecord model for storing search history and audit trails.
"""
from sqlalchemy import Column, String, DateTime, Text, JSON
from sqlalchemy.orm import relationship
from .base import Base, generate_uuid, get_current_timestamp


class SearchRecord(Base):
    """
    Model representing a search operation performed by a user.
    
    Attributes:
        id: Unique identifier for the search record
        search_query: The original search query string
        search_timestamp: When the search was performed
        user_id: Identifier of the user who performed the search
        sanctions_list_versions: JSON field containing versions of sanctions lists used
        verification_hash: Cryptographic hash for verifying search authenticity
        search_parameters: JSON field containing search configuration used
        created_at: Timestamp when record was created
        results: Relationship to SearchResult objects
    """
    __tablename__ = 'search_records'
    
    id = Column(String(36), primary_key=True, default=generate_uuid)
    search_query = Column(Text, nullable=False)
    search_timestamp = Column(DateTime, default=get_current_timestamp, nullable=False, index=True)
    user_id = Column(String(100), nullable=True, index=True)  # Optional user identification
    tags = Column(JSON, default=list)  # List of tags for categorization/filtering
    sanctions_list_versions = Column(JSON, default=dict)  # {source: version} mapping
    verification_hash = Column(String(64), nullable=False)  # SHA-256 hash
    search_parameters = Column(JSON, default=dict)  # Search configuration used
    created_at = Column(DateTime, default=get_current_timestamp, nullable=False)
    
    # Relationship to search results
    results = relationship("SearchResult", back_populates="search_record", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<SearchRecord(id='{self.id}', query='{self.search_query[:50]}...', timestamp='{self.search_timestamp}')>"
    
    def to_dict(self):
        """Convert the search record to a dictionary representation."""
        return {
            'id': self.id,
            'search_query': self.search_query,
            'search_timestamp': self.search_timestamp.isoformat() + ' UTC' if self.search_timestamp else None,
            'user_id': self.user_id,
            'tags': self.tags or [],
            'sanctions_list_versions': self.sanctions_list_versions or {},
            'verification_hash': self.verification_hash,
            'search_parameters': self.search_parameters or {},
            'created_at': self.created_at.isoformat() + ' UTC' if self.created_at else None,
            'results_count': len(self.results) if self.results else 0
        }
    
    def get_results_summary(self):
        """Get a summary of search results."""
        if not self.results:
            return {'total': 0, 'high_confidence': 0, 'medium_confidence': 0, 'low_confidence': 0}
        
        summary = {'total': len(self.results), 'high_confidence': 0, 'medium_confidence': 0, 'low_confidence': 0}
        
        for result in self.results:
            if result.overall_confidence >= 0.8:
                summary['high_confidence'] += 1
            elif result.overall_confidence >= 0.6:
                summary['medium_confidence'] += 1
            else:
                summary['low_confidence'] += 1
        
        return summary