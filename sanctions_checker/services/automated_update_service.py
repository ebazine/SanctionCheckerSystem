"""
Automated update service that integrates scheduler, health monitoring, and notifications.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

from ..config import Config
from .update_scheduler import DataUpdateScheduler, UpdateNotification, UpdateStatus
from .health_monitor import HealthMonitor, HealthStatus
from .notification_service import (
    get_notification_service, NotificationLevel, NotificationType,
    notify_info, notify_warning, notify_error, notify_success, notify_data_update
)
from .data_service import DataService

logger = logging.getLogger(__name__)


class AutomatedUpdateService:
    """
    Comprehensive automated update service that coordinates:
    - Scheduled data updates
    - Health monitoring
    - Notification management
    - Error handling and recovery
    """
    
    def __init__(self, config: Config = None, data_service: DataService = None):
        """
        Initialize the automated update service.
        
        Args:
            config: Configuration instance
            data_service: DataService instance
        """
        self.config = config or Config()
        self.data_service = data_service or DataService()
        self.notification_service = get_notification_service()
        
        # Initialize components
        self.scheduler = DataUpdateScheduler(self.config, self.data_service)
        self.health_monitor = HealthMonitor(self.config)
        
        # Service state
        self._running = False
        
        # Setup notification callbacks
        self._setup_notification_callbacks()
    
    def _setup_notification_callbacks(self):
        """Setup callbacks to integrate scheduler and health monitor notifications."""
        # Add scheduler notification callback
        self.scheduler.add_notification_callback(self._handle_scheduler_notification)
        
        # Add health monitor callback
        self.health_monitor.add_check_callback(self._handle_health_check_completed)
    
    def start(self):
        """Start all automated update services."""
        if self._running:
            logger.warning("Automated update service is already running")
            return
        
        try:
            # Start health monitoring first
            self.health_monitor.start_monitoring()
            
            # Start the scheduler
            self.scheduler.start_scheduler()
            
            self._running = True
            
            logger.info("Automated update service started successfully")
            notify_success(
                "Automated Updates Started",
                "All automated update services are now running",
                source="AUTO_UPDATE_SERVICE"
            )
            
        except Exception as e:
            logger.error(f"Failed to start automated update service: {e}")
            notify_error(
                "Failed to Start Automated Updates",
                f"Error starting automated update services: {e}",
                source="AUTO_UPDATE_SERVICE",
                action_required=True
            )
            raise
    
    def stop(self):
        """Stop all automated update services."""
        if not self._running:
            return
        
        try:
            # Stop scheduler
            self.scheduler.stop_scheduler()
            
            # Stop health monitor
            self.health_monitor.stop_monitoring()
            
            self._running = False
            
            logger.info("Automated update service stopped")
            notify_info(
                "Automated Updates Stopped",
                "All automated update services have been stopped",
                source="AUTO_UPDATE_SERVICE"
            )
            
        except Exception as e:
            logger.error(f"Error stopping automated update service: {e}")
            notify_error(
                "Error Stopping Automated Updates",
                f"Error stopping automated update services: {e}",
                source="AUTO_UPDATE_SERVICE"
            )
    
    def _handle_scheduler_notification(self, notification: UpdateNotification):
        """Handle notifications from the update scheduler."""
        try:
            # Map scheduler status to notification level
            level_mapping = {
                UpdateStatus.SUCCESS: NotificationLevel.SUCCESS,
                UpdateStatus.FAILED: NotificationLevel.ERROR,
                UpdateStatus.PARTIAL_SUCCESS: NotificationLevel.WARNING,
                UpdateStatus.RUNNING: NotificationLevel.INFO
            }
            
            level = level_mapping.get(notification.status, NotificationLevel.INFO)
            
            # Determine if action is required
            action_required = notification.status == UpdateStatus.FAILED
            
            # Create appropriate notification title
            if notification.source == "SCHEDULER":
                title = f"Update Scheduler: {notification.message}"
            elif notification.source == "ALL_SOURCES":
                title = f"Data Update: {notification.message}"
            else:
                title = f"{notification.source} Update: {notification.message}"
            
            # Send notification
            self.notification_service.add_notification(
                title=title,
                message=notification.message,
                level=level,
                notification_type=NotificationType.DATA_UPDATE,
                source="DATA_SCHEDULER",
                action_required=action_required,
                details=notification.details,
                expires_in_minutes=60 if level == NotificationLevel.INFO else None
            )
            
            # Log important events
            if notification.status == UpdateStatus.FAILED:
                logger.error(f"Data update failed for {notification.source}: {notification.message}")
            elif notification.status == UpdateStatus.SUCCESS and notification.source != "SCHEDULER":
                logger.info(f"Data update successful for {notification.source}")
                
        except Exception as e:
            logger.error(f"Error handling scheduler notification: {e}")
    
    def _handle_health_check_completed(self):
        """Handle completion of health check cycle."""
        try:
            # Get overall health status
            overall_health = self.health_monitor.get_overall_health()
            
            # Check for any critical health issues
            current_status = self.health_monitor.get_current_status()
            unhealthy_components = [
                name for name, status in current_status.items() 
                if status == HealthStatus.UNHEALTHY
            ]
            
            # If we have unhealthy components, check if scheduler should be paused
            if unhealthy_components:
                self._handle_unhealthy_components(unhealthy_components)
            
            # Log health summary periodically
            if datetime.now().minute % 30 == 0:  # Every 30 minutes
                logger.info(f"System health check completed. Overall status: {overall_health.value}")
                
        except Exception as e:
            logger.error(f"Error handling health check completion: {e}")
    
    def _handle_unhealthy_components(self, unhealthy_components: List[str]):
        """Handle unhealthy system components."""
        # Check if any data sources are unhealthy
        data_source_names = set(self.config.get('data_sources', {}).keys())
        unhealthy_sources = [comp for comp in unhealthy_components if comp in data_source_names]
        
        if unhealthy_sources:
            logger.warning(f"Unhealthy data sources detected: {unhealthy_sources}")
            
            # Optionally pause scheduler if too many sources are unhealthy
            if len(unhealthy_sources) >= len(data_source_names) * 0.5:  # 50% or more
                logger.warning("Majority of data sources are unhealthy, considering scheduler pause")
                notify_warning(
                    "Data Sources Unhealthy",
                    f"Multiple data sources are unhealthy: {', '.join(unhealthy_sources)}. "
                    "Automated updates may be affected.",
                    source="AUTO_UPDATE_SERVICE",
                    action_required=True,
                    details={'unhealthy_sources': unhealthy_sources}
                )
        
        # Check if database is unhealthy
        if 'database' in unhealthy_components:
            logger.error("Database is unhealthy, this will affect data updates")
            notify_error(
                "Database Unhealthy",
                "Database connectivity issues detected. Data updates cannot proceed.",
                source="AUTO_UPDATE_SERVICE",
                action_required=True
            )
    
    def get_service_status(self) -> Dict[str, Any]:
        """Get comprehensive status of all automated update services."""
        scheduler_stats = self.scheduler.get_update_statistics()
        health_summary = self.health_monitor.get_health_summary()
        notification_stats = self.notification_service.get_statistics()
        
        return {
            'service_running': self._running,
            'scheduler': {
                'running': scheduler_stats['scheduler_running'],
                'auto_update_enabled': scheduler_stats['auto_update_enabled'],
                'update_interval_hours': scheduler_stats['update_interval_hours'],
                'last_update_times': scheduler_stats['last_update_times'],
                'overall_health': scheduler_stats['overall_health']
            },
            'health_monitor': {
                'running': health_summary['monitoring_active'],
                'overall_health': health_summary['overall_health'],
                'component_status': health_summary['component_status'],
                'recent_issues_count': len(health_summary['recent_issues'])
            },
            'notifications': {
                'total_notifications': notification_stats['total_notifications'],
                'unread_notifications': notification_stats['unread_notifications'],
                'action_required': notification_stats['action_required']
            },
            'data_sources': self._get_data_source_summary()
        }
    
    def _get_data_source_summary(self) -> Dict[str, Any]:
        """Get summary of data source status."""
        source_health = self.health_monitor.get_current_status()
        last_updates = self.scheduler.get_last_update_times()
        
        summary = {}
        for source_name in self.config.get('data_sources', {}):
            summary[source_name] = {
                'health_status': source_health.get(source_name, HealthStatus.UNKNOWN).value,
                'last_update': last_updates.get(source_name).isoformat() if last_updates.get(source_name) else None,
                'enabled': self.config.get(f'data_sources.{source_name}.enabled', True)
            }
        
        return summary
    
    def force_update_all(self) -> Dict[str, bool]:
        """Force immediate update of all data sources."""
        logger.info("Forcing update of all data sources")
        
        notify_info(
            "Manual Update Started",
            "Forcing immediate update of all data sources",
            source="AUTO_UPDATE_SERVICE"
        )
        
        try:
            results = self.scheduler.force_update()
            
            successful_sources = [source for source, success in results.items() if success]
            failed_sources = [source for source, success in results.items() if not success]
            
            if failed_sources:
                notify_warning(
                    "Manual Update Completed with Errors",
                    f"Update completed. Success: {len(successful_sources)}, Failed: {len(failed_sources)}. "
                    f"Failed sources: {', '.join(failed_sources)}",
                    source="AUTO_UPDATE_SERVICE",
                    details={'successful': successful_sources, 'failed': failed_sources}
                )
            else:
                notify_success(
                    "Manual Update Completed",
                    f"Successfully updated all {len(successful_sources)} data sources",
                    source="AUTO_UPDATE_SERVICE",
                    details={'successful': successful_sources}
                )
            
            return results
            
        except Exception as e:
            logger.error(f"Error during forced update: {e}")
            notify_error(
                "Manual Update Failed",
                f"Error during forced update: {e}",
                source="AUTO_UPDATE_SERVICE",
                action_required=True
            )
            raise
    
    def force_update_source(self, source_name: str) -> bool:
        """Force immediate update of a specific data source."""
        logger.info(f"Forcing update of data source: {source_name}")
        
        notify_info(
            f"Manual Update Started: {source_name}",
            f"Forcing immediate update of {source_name}",
            source="AUTO_UPDATE_SERVICE"
        )
        
        try:
            results = self.scheduler.force_update([source_name])
            success = results.get(source_name, False)
            
            if success:
                notify_success(
                    f"Manual Update Completed: {source_name}",
                    f"Successfully updated {source_name}",
                    source="AUTO_UPDATE_SERVICE"
                )
            else:
                notify_error(
                    f"Manual Update Failed: {source_name}",
                    f"Failed to update {source_name}",
                    source="AUTO_UPDATE_SERVICE",
                    action_required=True
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Error during forced update of {source_name}: {e}")
            notify_error(
                f"Manual Update Error: {source_name}",
                f"Error during forced update of {source_name}: {e}",
                source="AUTO_UPDATE_SERVICE",
                action_required=True
            )
            raise
    
    def force_health_check(self, component_name: str = None):
        """Force immediate health check."""
        logger.info(f"Forcing health check" + (f" for {component_name}" if component_name else ""))
        
        try:
            self.health_monitor.force_check(component_name)
            
            notify_info(
                "Health Check Completed",
                f"Manual health check completed" + (f" for {component_name}" if component_name else ""),
                source="AUTO_UPDATE_SERVICE"
            )
            
        except Exception as e:
            logger.error(f"Error during forced health check: {e}")
            notify_error(
                "Health Check Failed",
                f"Error during manual health check: {e}",
                source="AUTO_UPDATE_SERVICE"
            )
            raise
    
    def get_recent_activity(self, hours: int = 24) -> Dict[str, Any]:
        """Get recent activity summary."""
        # Get recent notifications
        recent_notifications = self.notification_service.get_notifications(
            notification_type=NotificationType.DATA_UPDATE,
            limit=20
        )
        
        # Get health summary
        health_summary = self.health_monitor.get_health_summary()
        
        # Get update statistics
        update_stats = self.scheduler.get_update_statistics()
        
        return {
            'recent_notifications': [n.to_dict() for n in recent_notifications],
            'recent_health_issues': health_summary['recent_issues'],
            'last_update_times': update_stats['last_update_times'],
            'overall_health': health_summary['overall_health']
        }
    
    def is_running(self) -> bool:
        """Check if the automated update service is running."""
        return self._running
    
    def __del__(self):
        """Cleanup when service is destroyed."""
        if self._running:
            self.stop()