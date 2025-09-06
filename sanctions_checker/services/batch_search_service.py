"""
Batch search service for running multiple searches simultaneously.
"""

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from PyQt6.QtCore import QObject, pyqtSignal, QThread
import json

from ..database.manager import DatabaseManager
from ..services.search_service import SearchService
from ..models.search_record import SearchRecord

logger = logging.getLogger(__name__)


class BatchSearchWorker(QThread):
    """Worker thread for performing batch searches."""
    
    # Signals
    batch_started = pyqtSignal(int)  # Total number of searches
    search_progress = pyqtSignal(int, str, str)  # Current index, query, status
    search_completed = pyqtSignal(int, str, list, str)  # Index, query, results, record_id
    search_error = pyqtSignal(int, str, str)  # Index, query, error
    batch_completed = pyqtSignal(list)  # List of all results
    
    def __init__(self, search_service: SearchService, search_queries: List[Dict]):
        super().__init__()
        self.search_service = search_service
        self.search_queries = search_queries
        self._is_cancelled = False
    
    def run(self):
        """Execute the batch search operation."""
        try:
            total_searches = len(self.search_queries)
            self.batch_started.emit(total_searches)
            
            all_results = []
            
            for i, query_data in enumerate(self.search_queries):
                if self._is_cancelled:
                    break
                
                query = query_data.get('query', '')
                entity_type = query_data.get('entity_type')
                tags = query_data.get('tags', [])
                user_id = query_data.get('user_id', 'batch_user')
                
                self.search_progress.emit(i, query, 'searching')
                
                try:
                    # Perform the search
                    matches, search_record_id = self.search_service.search_entities(
                        query=query,
                        entity_type=entity_type,
                        user_id=user_id,
                        tags=tags
                    )
                    
                    result_data = {
                        'index': i,
                        'query': query,
                        'matches': matches,
                        'record_id': search_record_id,
                        'entity_type': entity_type,
                        'tags': tags,
                        'status': 'completed'
                    }
                    
                    all_results.append(result_data)
                    self.search_completed.emit(i, query, matches, search_record_id)
                    
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Batch search error for '{query}': {error_msg}")
                    
                    result_data = {
                        'index': i,
                        'query': query,
                        'matches': [],
                        'record_id': None,
                        'entity_type': entity_type,
                        'tags': tags,
                        'status': 'error',
                        'error': error_msg
                    }
                    
                    all_results.append(result_data)
                    self.search_error.emit(i, query, error_msg)
            
            if not self._is_cancelled:
                self.batch_completed.emit(all_results)
                
        except Exception as e:
            logger.error(f"Batch search worker error: {e}")
    
    def cancel(self):
        """Cancel the batch search operation."""
        self._is_cancelled = True


class BatchSearchService(QObject):
    """Service for managing batch searches."""
    
    # Signals
    batch_started = pyqtSignal(int)
    search_progress = pyqtSignal(int, str, str)
    search_completed = pyqtSignal(int, str, list, str)
    search_error = pyqtSignal(int, str, str)
    batch_completed = pyqtSignal(list)
    
    def __init__(self, search_service: SearchService, db_manager: DatabaseManager):
        super().__init__()
        self.search_service = search_service
        self.db_manager = db_manager
        self.current_worker = None
    
    def get_historical_searches_by_tags(self, tags: List[str]) -> List[Dict]:
        """Get historical searches filtered by tags."""
        try:
            session = self.db_manager.get_session()
            
            # Get search records with tags
            from ..models.search_record import SearchRecord
            records = session.query(SearchRecord).filter(
                SearchRecord.tags.isnot(None)
            ).order_by(SearchRecord.timestamp.desc()).all()
            
            matching_searches = []
            for record in records:
                try:
                    # Parse tags from JSON
                    record_tags = json.loads(record.tags) if record.tags else []
                    
                    # Check if any of the requested tags match
                    if any(tag in record_tags for tag in tags):
                        matching_searches.append({
                            'query': record.search_query,
                            'entity_type': record.search_parameters.get('entity_type') if record.search_parameters else None,
                            'tags': record_tags,
                            'user_id': record.user_id,
                            'timestamp': record.search_timestamp.isoformat() if record.search_timestamp else 'Unknown'
                        })
                except (json.JSONDecodeError, TypeError):
                    # Skip records with invalid tag data
                    continue
            
            self.db_manager.close_session(session)
            return matching_searches
            
        except Exception as e:
            logger.error(f"Error getting historical searches by tags: {e}")
            return []
    
    def get_all_historical_searches(self, limit: int = 100) -> List[Dict]:
        """Get all historical searches."""
        try:
            session = self.db_manager.get_session()
            
            from ..models.search_record import SearchRecord
            records = session.query(SearchRecord).order_by(
                SearchRecord.search_timestamp.desc()
            ).limit(limit).all()
            
            searches = []
            for record in records:
                try:
                    tags = json.loads(record.tags) if record.tags else []
                except (json.JSONDecodeError, TypeError):
                    tags = []
                
                searches.append({
                    'query': record.search_query,
                    'entity_type': record.search_parameters.get('entity_type') if record.search_parameters else None,
                    'tags': tags,
                    'user_id': record.user_id,
                    'timestamp': record.search_timestamp.isoformat() if record.search_timestamp else 'Unknown'
                })
            
            self.db_manager.close_session(session)
            return searches
            
        except Exception as e:
            logger.error(f"Error getting historical searches: {e}")
            return []
    
    def get_available_tags(self) -> List[str]:
        """Get all available tags from historical searches."""
        try:
            session = self.db_manager.get_session()
            
            from ..models.search_record import SearchRecord
            records = session.query(SearchRecord).filter(
                SearchRecord.tags.isnot(None)
            ).all()
            
            all_tags = set()
            for record in records:
                try:
                    tags = json.loads(record.tags) if record.tags else []
                    all_tags.update(tags)
                except (json.JSONDecodeError, TypeError):
                    continue
            
            self.db_manager.close_session(session)
            return sorted(list(all_tags))
            
        except Exception as e:
            logger.error(f"Error getting available tags: {e}")
            return []
    
    def start_batch_search(self, search_queries: List[Dict]):
        """Start a batch search operation."""
        if self.current_worker and self.current_worker.isRunning():
            self.current_worker.cancel()
            self.current_worker.wait()
        
        self.current_worker = BatchSearchWorker(self.search_service, search_queries)
        
        # Connect signals
        self.current_worker.batch_started.connect(self.batch_started)
        self.current_worker.search_progress.connect(self.search_progress)
        self.current_worker.search_completed.connect(self.search_completed)
        self.current_worker.search_error.connect(self.search_error)
        self.current_worker.batch_completed.connect(self.batch_completed)
        
        self.current_worker.start()
    
    def cancel_batch_search(self):
        """Cancel the current batch search operation."""
        if self.current_worker and self.current_worker.isRunning():
            self.current_worker.cancel()
