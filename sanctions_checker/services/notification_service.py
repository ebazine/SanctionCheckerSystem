"""
Notification service for system-wide notifications and alerts.
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Callable, Optional
from enum import Enum
import threading
import queue

logger = logging.getLogger(__name__)


class NotificationLevel(Enum):
    """Notification severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"


class NotificationType(Enum):
    """Types of notifications."""
    DATA_UPDATE = "data_update"
    SYSTEM_STATUS = "system_status"
    USER_ACTION = "user_action"
    ERROR = "error"
    MAINTENANCE = "maintenance"


class Notification:
    """Represents a system notification."""
    
    def __init__(self, title: str, message: str, level: NotificationLevel,
                 notification_type: NotificationType, timestamp: datetime = None,
                 details: Dict[str, Any] = None, source: str = None,
                 action_required: bool = False, expires_at: datetime = None):
        """
        Initialize a notification.
        
        Args:
            title: Short notification title
            message: Detailed notification message
            level: Severity level
            notification_type: Type of notification
            timestamp: When the notification was created
            details: Additional details dictionary
            source: Source component that generated the notification
            action_required: Whether user action is required
            expires_at: When the notification expires (auto-dismiss)
        """
        self.id = self._generate_id()
        self.title = title
        self.message = message
        self.level = level
        self.notification_type = notification_type
        self.timestamp = timestamp or datetime.now()
        self.details = details or {}
        self.source = source
        self.action_required = action_required
        self.expires_at = expires_at
        self.dismissed = False
        self.read = False
    
    def _generate_id(self) -> str:
        """Generate a unique notification ID."""
        import uuid
        return str(uuid.uuid4())
    
    def is_expired(self) -> bool:
        """Check if the notification has expired."""
        if self.expires_at is None:
            return False
        return datetime.now() > self.expires_at
    
    def dismiss(self):
        """Mark the notification as dismissed."""
        self.dismissed = True
    
    def mark_read(self):
        """Mark the notification as read."""
        self.read = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert notification to dictionary."""
        return {
            'id': self.id,
            'title': self.title,
            'message': self.message,
            'level': self.level.value,
            'type': self.notification_type.value,
            'timestamp': self.timestamp.isoformat(),
            'details': self.details,
            'source': self.source,
            'action_required': self.action_required,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'dismissed': self.dismissed,
            'read': self.read
        }


class NotificationService:
    """
    Service for managing system notifications.
    
    Features:
    - Centralized notification management
    - Multiple notification levels and types
    - Callback system for real-time updates
    - Automatic expiration and cleanup
    - Filtering and querying capabilities
    """
    
    def __init__(self, max_notifications: int = 1000):
        """
        Initialize the notification service.
        
        Args:
            max_notifications: Maximum number of notifications to keep in memory
        """
        self.max_notifications = max_notifications
        self._notifications = []
        self._callbacks = []
        self._lock = threading.RLock()
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_running = True
        self._cleanup_thread.start()
    
    def add_notification(self, title: str, message: str, level: NotificationLevel,
                        notification_type: NotificationType, details: Dict[str, Any] = None,
                        source: str = None, action_required: bool = False,
                        expires_in_minutes: int = None) -> str:
        """
        Add a new notification.
        
        Args:
            title: Short notification title
            message: Detailed notification message
            level: Severity level
            notification_type: Type of notification
            details: Additional details dictionary
            source: Source component that generated the notification
            action_required: Whether user action is required
            expires_in_minutes: Minutes until notification expires
            
        Returns:
            Notification ID
        """
        expires_at = None
        if expires_in_minutes is not None:
            expires_at = datetime.now() + timedelta(minutes=expires_in_minutes)
        
        notification = Notification(
            title=title,
            message=message,
            level=level,
            notification_type=notification_type,
            details=details,
            source=source,
            action_required=action_required,
            expires_at=expires_at
        )
        
        with self._lock:
            self._notifications.append(notification)
            
            # Maintain max notifications limit
            if len(self._notifications) > self.max_notifications:
                # Remove oldest non-action-required notifications first
                self._notifications = [n for n in self._notifications if n.action_required] + \
                                   self._notifications[-self.max_notifications:]
        
        # Log the notification
        log_level = {
            NotificationLevel.INFO: logging.INFO,
            NotificationLevel.WARNING: logging.WARNING,
            NotificationLevel.ERROR: logging.ERROR,
            NotificationLevel.SUCCESS: logging.INFO
        }.get(level, logging.INFO)
        
        logger.log(log_level, f"Notification [{source or 'SYSTEM'}]: {title} - {message}")
        
        # Notify callbacks
        self._notify_callbacks(notification)
        
        return notification.id
    
    def get_notifications(self, level: NotificationLevel = None,
                         notification_type: NotificationType = None,
                         source: str = None, unread_only: bool = False,
                         action_required_only: bool = False,
                         limit: int = None) -> List[Notification]:
        """
        Get notifications with optional filtering.
        
        Args:
            level: Filter by notification level
            notification_type: Filter by notification type
            source: Filter by source
            unread_only: Only return unread notifications
            action_required_only: Only return notifications requiring action
            limit: Maximum number of notifications to return
            
        Returns:
            List of matching notifications
        """
        with self._lock:
            notifications = self._notifications.copy()
        
        # Apply filters
        if level is not None:
            notifications = [n for n in notifications if n.level == level]
        
        if notification_type is not None:
            notifications = [n for n in notifications if n.notification_type == notification_type]
        
        if source is not None:
            notifications = [n for n in notifications if n.source == source]
        
        if unread_only:
            notifications = [n for n in notifications if not n.read]
        
        if action_required_only:
            notifications = [n for n in notifications if n.action_required]
        
        # Filter out expired and dismissed notifications
        notifications = [n for n in notifications if not n.is_expired() and not n.dismissed]
        
        # Sort by timestamp (newest first)
        notifications.sort(key=lambda n: n.timestamp, reverse=True)
        
        # Apply limit
        if limit is not None:
            notifications = notifications[:limit]
        
        return notifications
    
    def get_notification(self, notification_id: str) -> Optional[Notification]:
        """
        Get a specific notification by ID.
        
        Args:
            notification_id: ID of the notification
            
        Returns:
            Notification object or None if not found
        """
        with self._lock:
            for notification in self._notifications:
                if notification.id == notification_id:
                    return notification
        return None
    
    def dismiss_notification(self, notification_id: str) -> bool:
        """
        Dismiss a notification.
        
        Args:
            notification_id: ID of the notification to dismiss
            
        Returns:
            True if notification was found and dismissed
        """
        notification = self.get_notification(notification_id)
        if notification:
            notification.dismiss()
            logger.debug(f"Dismissed notification: {notification_id}")
            return True
        return False
    
    def mark_read(self, notification_id: str) -> bool:
        """
        Mark a notification as read.
        
        Args:
            notification_id: ID of the notification to mark as read
            
        Returns:
            True if notification was found and marked as read
        """
        notification = self.get_notification(notification_id)
        if notification:
            notification.mark_read()
            logger.debug(f"Marked notification as read: {notification_id}")
            return True
        return False
    
    def mark_all_read(self, level: NotificationLevel = None,
                     notification_type: NotificationType = None,
                     source: str = None):
        """
        Mark multiple notifications as read.
        
        Args:
            level: Optional level filter
            notification_type: Optional type filter
            source: Optional source filter
        """
        notifications = self.get_notifications(
            level=level,
            notification_type=notification_type,
            source=source,
            unread_only=True
        )
        
        for notification in notifications:
            notification.mark_read()
        
        logger.debug(f"Marked {len(notifications)} notifications as read")
    
    def clear_notifications(self, level: NotificationLevel = None,
                           notification_type: NotificationType = None,
                           source: str = None, older_than_hours: int = None):
        """
        Clear (dismiss) multiple notifications.
        
        Args:
            level: Optional level filter
            notification_type: Optional type filter
            source: Optional source filter
            older_than_hours: Only clear notifications older than specified hours
        """
        with self._lock:
            notifications_to_clear = []
            
            for notification in self._notifications:
                # Apply filters
                if level is not None and notification.level != level:
                    continue
                if notification_type is not None and notification.notification_type != notification_type:
                    continue
                if source is not None and notification.source != source:
                    continue
                if older_than_hours is not None:
                    cutoff_time = datetime.now() - timedelta(hours=older_than_hours)
                    if notification.timestamp > cutoff_time:
                        continue
                
                notifications_to_clear.append(notification)
            
            for notification in notifications_to_clear:
                notification.dismiss()
        
        logger.debug(f"Cleared {len(notifications_to_clear)} notifications")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get notification statistics."""
        with self._lock:
            notifications = [n for n in self._notifications if not n.is_expired() and not n.dismissed]
        
        stats = {
            'total_notifications': len(notifications),
            'unread_notifications': len([n for n in notifications if not n.read]),
            'action_required': len([n for n in notifications if n.action_required]),
            'by_level': {},
            'by_type': {},
            'by_source': {}
        }
        
        # Count by level
        for level in NotificationLevel:
            count = len([n for n in notifications if n.level == level])
            stats['by_level'][level.value] = count
        
        # Count by type
        for notification_type in NotificationType:
            count = len([n for n in notifications if n.notification_type == notification_type])
            stats['by_type'][notification_type.value] = count
        
        # Count by source
        sources = set(n.source for n in notifications if n.source)
        for source in sources:
            count = len([n for n in notifications if n.source == source])
            stats['by_source'][source] = count
        
        return stats
    
    def add_callback(self, callback: Callable[[Notification], None]):
        """
        Add a callback function to receive new notifications.
        
        Args:
            callback: Function that takes a Notification parameter
        """
        with self._lock:
            self._callbacks.append(callback)
    
    def remove_callback(self, callback: Callable[[Notification], None]):
        """Remove a notification callback."""
        with self._lock:
            if callback in self._callbacks:
                self._callbacks.remove(callback)
    
    def _notify_callbacks(self, notification: Notification):
        """Notify all registered callbacks about a new notification."""
        with self._lock:
            callbacks = self._callbacks.copy()
        
        for callback in callbacks:
            try:
                callback(notification)
            except Exception as e:
                logger.error(f"Error in notification callback: {e}")
    
    def _cleanup_loop(self):
        """Background cleanup loop for expired notifications."""
        while self._cleanup_running:
            try:
                with self._lock:
                    # Remove expired and old dismissed notifications
                    cutoff_time = datetime.now() - timedelta(hours=24)
                    self._notifications = [
                        n for n in self._notifications
                        if not n.is_expired() and not (n.dismissed and n.timestamp < cutoff_time)
                    ]
                
                # Sleep for 5 minutes
                import time
                time.sleep(300)
                
            except Exception as e:
                logger.error(f"Error in notification cleanup: {e}")
                import time
                time.sleep(60)  # Wait 1 minute before retrying
    
    def shutdown(self):
        """Shutdown the notification service."""
        self._cleanup_running = False
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)
    
    def __del__(self):
        """Cleanup when service is destroyed."""
        self.shutdown()


# Global notification service instance
_notification_service = None


def get_notification_service() -> NotificationService:
    """Get the global notification service instance."""
    global _notification_service
    if _notification_service is None:
        _notification_service = NotificationService()
    return _notification_service


# Convenience functions for common notification types
def notify_info(title: str, message: str, source: str = None, **kwargs):
    """Send an info notification."""
    service = get_notification_service()
    return service.add_notification(
        title, message, NotificationLevel.INFO, NotificationType.SYSTEM_STATUS,
        source=source, **kwargs
    )


def notify_warning(title: str, message: str, source: str = None, **kwargs):
    """Send a warning notification."""
    service = get_notification_service()
    return service.add_notification(
        title, message, NotificationLevel.WARNING, NotificationType.SYSTEM_STATUS,
        source=source, **kwargs
    )


def notify_error(title: str, message: str, source: str = None, **kwargs):
    """Send an error notification."""
    service = get_notification_service()
    return service.add_notification(
        title, message, NotificationLevel.ERROR, NotificationType.ERROR,
        source=source, **kwargs
    )


def notify_success(title: str, message: str, source: str = None, **kwargs):
    """Send a success notification."""
    service = get_notification_service()
    return service.add_notification(
        title, message, NotificationLevel.SUCCESS, NotificationType.SYSTEM_STATUS,
        source=source, **kwargs
    )


def notify_data_update(title: str, message: str, source: str = None, **kwargs):
    """Send a data update notification."""
    service = get_notification_service()
    return service.add_notification(
        title, message, NotificationLevel.INFO, NotificationType.DATA_UPDATE,
        source=source, **kwargs
    )