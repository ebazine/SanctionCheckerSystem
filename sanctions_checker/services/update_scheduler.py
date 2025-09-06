"""
Automated data update scheduler for sanctions data.
"""
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from enum import Enum
import queue

from ..config import Config
from .data_downloader import DataDownloader
from .data_parser import DataParser
from .data_service import DataService

logger = logging.getLogger(__name__)


class UpdateStatus(Enum):
    """Status of data update operations."""
    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"


class UpdateNotification:
    """Notification about update status."""
    
    def __init__(self, source: str, status: UpdateStatus, message: str, 
                 timestamp: datetime = None, details: Dict[str, Any] = None):
        self.source = source
        self.status = status
        self.message = message
        self.timestamp = timestamp or datetime.now()
        self.details = details or {}


class DataUpdateScheduler:
    """
    Automated scheduler for downloading and updating sanctions data.
    
    Features:
    - Background scheduling with configurable intervals
    - Health monitoring of data sources
    - Retry mechanisms with exponential backoff
    - Notification system for update status
    - Graceful handling of source failures
    """
    
    def __init__(self, config: Config = None, data_service: DataService = None):
        """
        Initialize the update scheduler.
        
        Args:
            config: Configuration instance
            data_service: DataService instance for storing data
        """
        self.config = config or Config()
        self.data_service = data_service or DataService()
        self.data_downloader = DataDownloader(
            data_dir=str(self.config.data_directory),
            timeout=self.config.get('updates.timeout', 30),
            max_retries=self.config.get('updates.retry_attempts', 3)
        )
        self.data_parser = DataParser()
        
        # Scheduler state
        self._running = False
        self._scheduler_thread = None
        self._last_update_times = {}
        self._source_health = {}
        self._notification_queue = queue.Queue()
        self._notification_callbacks = []
        
        # Update configuration
        self.update_interval_hours = self.config.get('updates.update_interval_hours', 24)
        self.auto_update_enabled = self.config.get('updates.auto_update', True)
        self.retry_attempts = self.config.get('updates.retry_attempts', 3)
        self.retry_delay_seconds = self.config.get('updates.retry_delay_seconds', 300)
        
        # Initialize source health status
        self._initialize_source_health()
    
    def _initialize_source_health(self):
        """Initialize health status for all data sources."""
        for source_name in self.data_downloader.DATA_SOURCES:
            self._source_health[source_name] = {
                'status': 'unknown',
                'last_success': None,
                'last_failure': None,
                'consecutive_failures': 0,
                'total_attempts': 0,
                'total_successes': 0,
                'total_failures': 0
            }
    
    def start_scheduler(self):
        """Start the background update scheduler."""
        if self._running:
            logger.warning("Scheduler is already running")
            return
        
        if not self.auto_update_enabled:
            logger.info("Auto-update is disabled in configuration")
            return
        
        self._running = True
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        
        logger.info(f"Data update scheduler started with {self.update_interval_hours}h interval")
        self._notify("SCHEDULER", UpdateStatus.SUCCESS, "Update scheduler started")
    
    def stop_scheduler(self):
        """Stop the background update scheduler."""
        if not self._running:
            return
        
        self._running = False
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=5)
        
        logger.info("Data update scheduler stopped")
        self._notify("SCHEDULER", UpdateStatus.SUCCESS, "Update scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop running in background thread."""
        logger.info("Scheduler loop started")
        
        while self._running:
            try:
                # Check if any sources need updating
                sources_to_update = self._get_sources_needing_update()
                
                if sources_to_update:
                    logger.info(f"Updating sources: {sources_to_update}")
                    self._update_sources(sources_to_update)
                
                # Sleep for a shorter interval to check more frequently
                # but only update based on the configured interval
                time.sleep(min(3600, self.update_interval_hours * 3600 // 10))  # Check every hour or 1/10 of interval
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                self._notify("SCHEDULER", UpdateStatus.FAILED, f"Scheduler error: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying
    
    def _get_sources_needing_update(self) -> List[str]:
        """Get list of sources that need updating based on interval."""
        sources_to_update = []
        current_time = datetime.now()
        update_interval = timedelta(hours=self.update_interval_hours)
        
        for source_name in self.data_downloader.DATA_SOURCES:
            last_update = self._last_update_times.get(source_name)
            
            if last_update is None:
                # Never updated, needs update
                sources_to_update.append(source_name)
            elif current_time - last_update >= update_interval:
                # Update interval has passed
                sources_to_update.append(source_name)
        
        return sources_to_update
    
    def _update_sources(self, source_names: List[str]):
        """Update specified data sources."""
        overall_success = True
        
        for source_name in source_names:
            try:
                success = self._update_single_source(source_name)
                if not success:
                    overall_success = False
            except Exception as e:
                logger.error(f"Unexpected error updating {source_name}: {e}")
                self._update_source_health(source_name, False, str(e))
                overall_success = False
        
        # Send overall status notification
        if overall_success:
            self._notify("ALL_SOURCES", UpdateStatus.SUCCESS, 
                        f"Successfully updated {len(source_names)} sources")
        else:
            self._notify("ALL_SOURCES", UpdateStatus.PARTIAL_SUCCESS,
                        f"Updated sources with some failures: {source_names}")
    
    def _update_single_source(self, source_name: str) -> bool:
        """
        Update a single data source with retry logic.
        
        Args:
            source_name: Name of the source to update
            
        Returns:
            True if update was successful
        """
        logger.info(f"Starting update for source: {source_name}")
        self._notify(source_name, UpdateStatus.RUNNING, f"Starting update for {source_name}")
        
        for attempt in range(self.retry_attempts):
            try:
                # Download data
                download_result = self.data_downloader.download_source(source_name)
                
                if not download_result.get('success', False):
                    raise Exception(f"Download failed: {download_result.get('error', 'Unknown error')}")
                
                # Parse data
                file_path = download_result['file_path']
                parsed_data = self.data_parser.parse_file(file_path, source_name)
                
                if not parsed_data:
                    raise Exception("No data parsed from file")
                
                # Store in database
                source_version = download_result.get('content_hash', datetime.now().isoformat())
                created, updated, skipped = self.data_service.store_sanctions_data(
                    parsed_data, source_name, source_version
                )
                
                # Update tracking
                self._last_update_times[source_name] = datetime.now()
                self._update_source_health(source_name, True)
                
                # Clean up old files
                self.data_downloader.cleanup_old_files(source_name, keep_count=3)
                
                success_message = f"Updated {source_name}: {created} created, {updated} updated, {skipped} skipped"
                logger.info(success_message)
                self._notify(source_name, UpdateStatus.SUCCESS, success_message, {
                    'created': created,
                    'updated': updated,
                    'skipped': skipped,
                    'source_version': source_version
                })
                
                return True
                
            except Exception as e:
                error_message = f"Update attempt {attempt + 1} failed for {source_name}: {e}"
                logger.warning(error_message)
                
                if attempt < self.retry_attempts - 1:
                    # Calculate exponential backoff delay
                    delay = self.retry_delay_seconds * (2 ** attempt)
                    logger.info(f"Retrying {source_name} in {delay} seconds...")
                    time.sleep(delay)
                else:
                    # Final failure
                    self._update_source_health(source_name, False, str(e))
                    self._notify(source_name, UpdateStatus.FAILED, 
                               f"Failed to update {source_name} after {self.retry_attempts} attempts: {e}")
                    return False
        
        return False
    
    def _update_source_health(self, source_name: str, success: bool, error_message: str = None):
        """Update health status for a data source."""
        health = self._source_health[source_name]
        health['total_attempts'] += 1
        
        if success:
            health['status'] = 'healthy'
            health['last_success'] = datetime.now()
            health['consecutive_failures'] = 0
            health['total_successes'] += 1
        else:
            health['status'] = 'unhealthy'
            health['last_failure'] = datetime.now()
            health['consecutive_failures'] += 1
            health['total_failures'] += 1
            health['last_error'] = error_message
        
        # Log health status changes
        if success:
            logger.info(f"Source {source_name} is healthy")
        else:
            logger.warning(f"Source {source_name} is unhealthy: {health['consecutive_failures']} consecutive failures")
    
    def _notify(self, source: str, status: UpdateStatus, message: str, details: Dict[str, Any] = None):
        """Send notification about update status."""
        notification = UpdateNotification(source, status, message, details=details)
        
        # Add to queue
        try:
            self._notification_queue.put_nowait(notification)
        except queue.Full:
            logger.warning("Notification queue is full, dropping notification")
        
        # Call registered callbacks
        for callback in self._notification_callbacks:
            try:
                callback(notification)
            except Exception as e:
                logger.error(f"Error in notification callback: {e}")
    
    def add_notification_callback(self, callback: Callable[[UpdateNotification], None]):
        """
        Add a callback function to receive update notifications.
        
        Args:
            callback: Function that takes an UpdateNotification parameter
        """
        self._notification_callbacks.append(callback)
    
    def remove_notification_callback(self, callback: Callable[[UpdateNotification], None]):
        """Remove a notification callback."""
        if callback in self._notification_callbacks:
            self._notification_callbacks.remove(callback)
    
    def get_notifications(self, max_count: int = 100) -> List[UpdateNotification]:
        """
        Get pending notifications from the queue.
        
        Args:
            max_count: Maximum number of notifications to retrieve
            
        Returns:
            List of UpdateNotification objects
        """
        notifications = []
        
        for _ in range(max_count):
            try:
                notification = self._notification_queue.get_nowait()
                notifications.append(notification)
            except queue.Empty:
                break
        
        return notifications
    
    def get_source_health(self, source_name: str = None) -> Dict[str, Any]:
        """
        Get health status for data sources.
        
        Args:
            source_name: Optional specific source name
            
        Returns:
            Health status dictionary
        """
        if source_name:
            return self._source_health.get(source_name, {})
        else:
            return self._source_health.copy()
    
    def get_last_update_times(self) -> Dict[str, datetime]:
        """Get the last update times for all sources."""
        return self._last_update_times.copy()
    
    def force_update(self, source_names: List[str] = None) -> Dict[str, bool]:
        """
        Force immediate update of specified sources.
        
        Args:
            source_names: List of source names to update. If None, updates all sources.
            
        Returns:
            Dictionary mapping source names to success status
        """
        if source_names is None:
            source_names = list(self.data_downloader.DATA_SOURCES.keys())
        
        results = {}
        
        logger.info(f"Force updating sources: {source_names}")
        
        for source_name in source_names:
            if source_name not in self.data_downloader.DATA_SOURCES:
                logger.warning(f"Unknown source: {source_name}")
                results[source_name] = False
                continue
            
            try:
                success = self._update_single_source(source_name)
                results[source_name] = success
            except Exception as e:
                logger.error(f"Error force updating {source_name}: {e}")
                results[source_name] = False
        
        return results
    
    def get_update_statistics(self) -> Dict[str, Any]:
        """Get comprehensive update statistics."""
        stats = {
            'scheduler_running': self._running,
            'auto_update_enabled': self.auto_update_enabled,
            'update_interval_hours': self.update_interval_hours,
            'last_update_times': {
                source: time.isoformat() if time else None 
                for source, time in self._last_update_times.items()
            },
            'source_health_summary': {},
            'overall_health': 'healthy'
        }
        
        healthy_sources = 0
        total_sources = len(self._source_health)
        
        for source_name, health in self._source_health.items():
            stats['source_health_summary'][source_name] = {
                'status': health['status'],
                'consecutive_failures': health['consecutive_failures'],
                'success_rate': (
                    health['total_successes'] / health['total_attempts'] 
                    if health['total_attempts'] > 0 else 0
                )
            }
            
            if health['status'] == 'healthy':
                healthy_sources += 1
        
        # Determine overall health
        if healthy_sources == total_sources:
            stats['overall_health'] = 'healthy'
        elif healthy_sources > 0:
            stats['overall_health'] = 'degraded'
        else:
            stats['overall_health'] = 'unhealthy'
        
        return stats
    
    def is_running(self) -> bool:
        """Check if the scheduler is currently running."""
        return self._running
    
    def __del__(self):
        """Cleanup when scheduler is destroyed."""
        self.stop_scheduler()