#!/usr/bin/env python3
"""
Update dialog for the Sanctions Checker application.
Provides GUI for checking, downloading, and installing updates.
"""

import sys
from pathlib import Path
from typing import Dict, Optional
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTextEdit, QProgressBar, QCheckBox, QGroupBox, QMessageBox,
    QScrollArea, QFrame
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QPixmap, QIcon

from ..services.update_service import UpdateService, AutoUpdateManager
from ..config import Config
from ..utils.logger import get_logger

logger = get_logger(__name__)

class UpdateCheckThread(QThread):
    """Thread for checking updates without blocking the UI."""
    
    update_found = pyqtSignal(dict)
    no_update = pyqtSignal()
    error_occurred = pyqtSignal(str)
    
    def __init__(self, update_service: UpdateService):
        super().__init__()
        self.update_service = update_service
    
    def run(self):
        """Check for updates in background thread."""
        try:
            update_info = self.update_service.check_for_updates()
            if update_info:
                self.update_found.emit(update_info)
            else:
                self.no_update.emit()
        except Exception as e:
            self.error_occurred.emit(str(e))

class UpdateDownloadThread(QThread):
    """Thread for downloading updates without blocking the UI."""
    
    progress_updated = pyqtSignal(int)
    download_completed = pyqtSignal(str)  # file path
    download_failed = pyqtSignal(str)
    
    def __init__(self, update_service: UpdateService, update_info: Dict):
        super().__init__()
        self.update_service = update_service
        self.update_info = update_info
    
    def run(self):
        """Download update in background thread."""
        try:
            def progress_callback(progress):
                self.progress_updated.emit(int(progress))
            
            update_file = self.update_service.download_update(
                self.update_info, progress_callback
            )
            
            if update_file:
                self.download_completed.emit(str(update_file))
            else:
                self.download_failed.emit("Download failed")
                
        except Exception as e:
            self.download_failed.emit(str(e))

class UpdateDialog(QDialog):
    """Dialog for managing application updates."""
    
    def __init__(self, config: Config, parent=None):
        super().__init__(parent)
        self.config = config
        self.update_service = UpdateService(config)
        self.auto_update_manager = AutoUpdateManager(config, self.update_service)
        
        self.current_update_info = None
        self.downloaded_file = None
        
        self.setup_ui()
        self.setup_connections()
        
        # Auto-check for updates if enabled
        if config.get('auto_check_updates', True):
            QTimer.singleShot(1000, self.check_for_updates)
    
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle("Application Updates")
        self.setFixedSize(600, 500)
        self.setModal(True)
        
        layout = QVBoxLayout(self)
        
        # Header
        header_layout = QHBoxLayout()
        
        # Icon (if available)
        icon_label = QLabel()
        icon_label.setFixedSize(48, 48)
        try:
            icon_path = Path(__file__).parent / 'icons' / 'update_icon.png'
            if icon_path.exists():
                pixmap = QPixmap(str(icon_path)).scaled(48, 48, Qt.AspectRatioMode.KeepAspectRatio)
                icon_label.setPixmap(pixmap)
        except Exception:
            pass
        
        header_layout.addWidget(icon_label)
        
        # Title and version info
        title_layout = QVBoxLayout()
        title_label = QLabel("Sanctions Checker Updates")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title_label.setFont(title_font)
        
        version_label = QLabel(f"Current Version: {self.update_service.current_version}")
        version_label.setStyleSheet("color: #666;")
        
        title_layout.addWidget(title_label)
        title_layout.addWidget(version_label)
        title_layout.addStretch()
        
        header_layout.addLayout(title_layout)
        header_layout.addStretch()
        
        layout.addLayout(header_layout)
        
        # Status area
        self.status_label = QLabel("Ready to check for updates")
        self.status_label.setStyleSheet("padding: 10px; background-color: #f0f0f0; border-radius: 5px;")
        layout.addWidget(self.status_label)
        
        # Update information area
        self.update_info_group = QGroupBox("Update Information")
        self.update_info_group.setVisible(False)
        
        info_layout = QVBoxLayout(self.update_info_group)
        
        self.version_info_label = QLabel()
        self.version_info_label.setFont(QFont("", 10, QFont.Weight.Bold))
        info_layout.addWidget(self.version_info_label)
        
        self.release_notes = QTextEdit()
        self.release_notes.setMaximumHeight(150)
        self.release_notes.setReadOnly(True)
        info_layout.addWidget(QLabel("Release Notes:"))
        info_layout.addWidget(self.release_notes)
        
        layout.addWidget(self.update_info_group)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Settings group
        settings_group = QGroupBox("Update Settings")
        settings_layout = QVBoxLayout(settings_group)
        
        self.auto_check_checkbox = QCheckBox("Automatically check for updates")
        self.auto_check_checkbox.setChecked(self.config.get('auto_check_updates', True))
        settings_layout.addWidget(self.auto_check_checkbox)
        
        self.auto_install_checkbox = QCheckBox("Automatically install updates (not recommended)")
        self.auto_install_checkbox.setChecked(self.config.get('auto_install_updates', False))
        settings_layout.addWidget(self.auto_install_checkbox)
        
        layout.addWidget(settings_group)
        
        # Update history
        history_group = QGroupBox("Update History")
        history_layout = QVBoxLayout(history_group)
        
        self.history_text = QTextEdit()
        self.history_text.setMaximumHeight(100)
        self.history_text.setReadOnly(True)
        history_layout.addWidget(self.history_text)
        
        self.load_update_history()
        
        layout.addWidget(history_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.check_button = QPushButton("Check for Updates")
        self.download_button = QPushButton("Download Update")
        self.download_button.setVisible(False)
        
        self.install_button = QPushButton("Install Update")
        self.install_button.setVisible(False)
        
        self.close_button = QPushButton("Close")
        
        button_layout.addWidget(self.check_button)
        button_layout.addWidget(self.download_button)
        button_layout.addWidget(self.install_button)
        button_layout.addStretch()
        button_layout.addWidget(self.close_button)
        
        layout.addLayout(button_layout)
    
    def setup_connections(self):
        """Set up signal connections."""
        self.check_button.clicked.connect(self.check_for_updates)
        self.download_button.clicked.connect(self.download_update)
        self.install_button.clicked.connect(self.install_update)
        self.close_button.clicked.connect(self.close)
        
        self.auto_check_checkbox.toggled.connect(self.save_settings)
        self.auto_install_checkbox.toggled.connect(self.save_settings)
    
    def check_for_updates(self):
        """Check for available updates."""
        self.status_label.setText("Checking for updates...")
        self.check_button.setEnabled(False)
        
        self.check_thread = UpdateCheckThread(self.update_service)
        self.check_thread.update_found.connect(self.on_update_found)
        self.check_thread.no_update.connect(self.on_no_update)
        self.check_thread.error_occurred.connect(self.on_check_error)
        self.check_thread.finished.connect(lambda: self.check_button.setEnabled(True))
        
        self.check_thread.start()
    
    def on_update_found(self, update_info: Dict):
        """Handle when an update is found."""
        self.current_update_info = update_info
        
        self.status_label.setText(f"Update available: Version {update_info['version']}")
        self.status_label.setStyleSheet("padding: 10px; background-color: #e8f5e8; border-radius: 5px; color: #2d5a2d;")
        
        # Show update information
        self.version_info_label.setText(f"New Version: {update_info['version']}")
        self.release_notes.setPlainText(update_info.get('release_notes', 'No release notes available.'))
        
        self.update_info_group.setVisible(True)
        self.download_button.setVisible(True)
        
        # Resize dialog to accommodate new content
        self.adjustSize()
    
    def on_no_update(self):
        """Handle when no update is available."""
        self.status_label.setText("Your application is up to date!")
        self.status_label.setStyleSheet("padding: 10px; background-color: #e8f5e8; border-radius: 5px; color: #2d5a2d;")
    
    def on_check_error(self, error_message: str):
        """Handle update check errors."""
        self.status_label.setText(f"Error checking for updates: {error_message}")
        self.status_label.setStyleSheet("padding: 10px; background-color: #f5e8e8; border-radius: 5px; color: #5a2d2d;")
    
    def download_update(self):
        """Download the available update."""
        if not self.current_update_info:
            return
        
        self.status_label.setText("Downloading update...")
        self.download_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        
        self.download_thread = UpdateDownloadThread(self.update_service, self.current_update_info)
        self.download_thread.progress_updated.connect(self.progress_bar.setValue)
        self.download_thread.download_completed.connect(self.on_download_completed)
        self.download_thread.download_failed.connect(self.on_download_failed)
        
        self.download_thread.start()
    
    def on_download_completed(self, file_path: str):
        """Handle successful download completion."""
        self.downloaded_file = file_path
        
        self.status_label.setText("Update downloaded successfully!")
        self.status_label.setStyleSheet("padding: 10px; background-color: #e8f5e8; border-radius: 5px; color: #2d5a2d;")
        
        self.progress_bar.setVisible(False)
        self.download_button.setVisible(False)
        self.install_button.setVisible(True)
        
        self.download_button.setEnabled(True)
    
    def on_download_failed(self, error_message: str):
        """Handle download failure."""
        self.status_label.setText(f"Download failed: {error_message}")
        self.status_label.setStyleSheet("padding: 10px; background-color: #f5e8e8; border-radius: 5px; color: #5a2d2d;")
        
        self.progress_bar.setVisible(False)
        self.download_button.setEnabled(True)
    
    def install_update(self):
        """Install the downloaded update."""
        if not self.downloaded_file or not self.current_update_info:
            return
        
        # Confirm installation
        reply = QMessageBox.question(
            self, "Install Update",
            f"Are you sure you want to install version {self.current_update_info['version']}?\n\n"
            "The application will be closed and restarted.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply != QMessageBox.StandardButton.Yes:
            return
        
        self.status_label.setText("Installing update...")
        self.install_button.setEnabled(False)
        
        try:
            success = self.update_service.install_update(Path(self.downloaded_file))
            
            # Record the installation
            self.update_service.record_update_installation(
                self.current_update_info['version'], success
            )
            
            if success:
                QMessageBox.information(
                    self, "Update Installed",
                    "Update installed successfully!\n\n"
                    "Please restart the application to use the new version."
                )
                
                # Close the application
                sys.exit(0)
            else:
                QMessageBox.warning(
                    self, "Installation Failed",
                    "Failed to install the update. Please try again or install manually."
                )
                
        except Exception as e:
            logger.error(f"Error installing update: {e}")
            QMessageBox.critical(
                self, "Installation Error",
                f"An error occurred during installation:\n{str(e)}"
            )
        
        finally:
            self.install_button.setEnabled(True)
    
    def load_update_history(self):
        """Load and display update history."""
        try:
            history = self.update_service.get_update_history()
            
            if not history:
                self.history_text.setPlainText("No update history available.")
                return
            
            history_text = ""
            for record in reversed(history[-5:]):  # Show last 5 records
                timestamp = record['timestamp'][:19].replace('T', ' ')
                status = "✓" if record['success'] else "✗"
                history_text += f"{timestamp} - {status} Version {record['version']}\n"
            
            self.history_text.setPlainText(history_text)
            
        except Exception as e:
            logger.error(f"Error loading update history: {e}")
            self.history_text.setPlainText("Error loading update history.")
    
    def save_settings(self):
        """Save update settings."""
        try:
            self.config.set('auto_check_updates', self.auto_check_checkbox.isChecked())
            self.config.set('auto_install_updates', self.auto_install_checkbox.isChecked())
            self.config.save()
            
        except Exception as e:
            logger.error(f"Error saving update settings: {e}")
    
    def closeEvent(self, event):
        """Handle dialog close event."""
        # Clean up threads
        if hasattr(self, 'check_thread') and self.check_thread.isRunning():
            self.check_thread.terminate()
            self.check_thread.wait()
        
        if hasattr(self, 'download_thread') and self.download_thread.isRunning():
            self.download_thread.terminate()
            self.download_thread.wait()
        
        # Clean up downloaded file if not installed
        if self.downloaded_file and Path(self.downloaded_file).exists():
            try:
                Path(self.downloaded_file).unlink()
            except Exception:
                pass
        
        event.accept()


class UpdateNotificationWidget(QFrame):
    """Widget for showing update notifications in the main window."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
        self.hide()
    
    def setup_ui(self):
        """Set up the notification widget UI."""
        self.setStyleSheet("""
            QFrame {
                background-color: #e8f4fd;
                border: 1px solid #bee5eb;
                border-radius: 5px;
                padding: 5px;
            }
        """)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 5, 10, 5)
        
        self.message_label = QLabel()
        layout.addWidget(self.message_label)
        
        layout.addStretch()
        
        self.update_button = QPushButton("Update Now")
        self.update_button.setMaximumWidth(100)
        layout.addWidget(self.update_button)
        
        self.dismiss_button = QPushButton("×")
        self.dismiss_button.setMaximumSize(20, 20)
        self.dismiss_button.setStyleSheet("border: none; font-weight: bold;")
        self.dismiss_button.clicked.connect(self.hide)
        layout.addWidget(self.dismiss_button)
    
    def show_update_notification(self, update_info: Dict):
        """Show notification about available update."""
        self.message_label.setText(
            f"Update available: Version {update_info['version']} is ready to download."
        )
        self.show()
    
    def connect_update_button(self, callback):
        """Connect the update button to a callback."""
        self.update_button.clicked.connect(callback)