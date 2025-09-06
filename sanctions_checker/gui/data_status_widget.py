#!/usr/bin/env python3
"""
Data Status Widget for monitoring and managing sanctions data downloads.
"""

from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, 
                            QTableWidgetItem, QPushButton, QLabel, QProgressBar,
                            QGroupBox, QHeaderView, QMessageBox, QFrame)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor
from datetime import datetime
from typing import Dict, Optional

from ..services.data_status_service import DataStatusService, DataSourceStatus


class DataDownloadWorker(QThread):
    """Worker thread for downloading data sources."""
    
    progress = pyqtSignal(str, str)  # source_id, message
    finished = pyqtSignal(str, bool)  # source_id, success
    all_finished = pyqtSignal(dict)  # results dict
    
    def __init__(self, data_service: DataStatusService, source_ids: list, force: bool = False):
        super().__init__()
        self.data_service = data_service
        self.source_ids = source_ids
        self.force = force
    
    def run(self):
        """Download the specified data sources."""
        results = {}
        
        for source_id in self.source_ids:
            self.progress.emit(source_id, f"Downloading {source_id}...")
            
            try:
                success = self.data_service.download_source(source_id, self.force)
                results[source_id] = success
                
                if success:
                    self.progress.emit(source_id, f"✅ {source_id} downloaded successfully")
                else:
                    self.progress.emit(source_id, f"❌ {source_id} download failed")
                
                self.finished.emit(source_id, success)
                
            except Exception as e:
                results[source_id] = False
                self.progress.emit(source_id, f"❌ {source_id} error: {str(e)}")
                self.finished.emit(source_id, False)
        
        self.all_finished.emit(results)


class DataStatusWidget(QWidget):
    """Widget for displaying and managing sanctions data status."""
    
    def __init__(self, config, data_service: DataStatusService):
        super().__init__()
        self.config = config
        self.data_service = data_service
        self.download_worker = None
        
        self.init_ui()
        self.setup_timer()
        self.refresh_status()
    
    def init_ui(self):
        """Initialize the user interface."""
        layout = QVBoxLayout(self)
        
        # Title
        title = QLabel("Sanctions Data Status & Management")
        title.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        layout.addWidget(title)
        
        # Control buttons
        button_layout = QHBoxLayout()
        
        self.refresh_btn = QPushButton("🔄 Refresh Status")
        self.refresh_btn.clicked.connect(self.refresh_status)
        button_layout.addWidget(self.refresh_btn)
        
        self.download_all_btn = QPushButton("⬇️ Download All")
        self.download_all_btn.clicked.connect(self.download_all)
        button_layout.addWidget(self.download_all_btn)
        
        self.force_download_btn = QPushButton("🔄 Force Update All")
        self.force_download_btn.clicked.connect(self.force_download_all)
        button_layout.addWidget(self.force_download_btn)
        
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        # Status table
        self.create_status_table()
        layout.addWidget(self.status_table)
        
        # Progress section
        progress_group = QGroupBox("Download Progress")
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        progress_layout.addWidget(self.progress_bar)
        
        self.progress_label = QLabel("")
        progress_layout.addWidget(self.progress_label)
        
        layout.addWidget(progress_group)
        
        # Summary section
        self.create_summary_section()
        layout.addWidget(self.summary_group)
    
    def create_status_table(self):
        """Create the status table."""
        self.status_table = QTableWidget()
        self.status_table.setColumnCount(9)
        
        headers = [
            "Source", "Status", "Last Download", "File Size", 
            "Entities", "Version", "Needs Update", "Actions", "Details"
        ]
        self.status_table.setHorizontalHeaderLabels(headers)
        
        # Set column widths
        header = self.status_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Source
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)  # Status
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)           # Last Download
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)  # File Size
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)  # Entities
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)  # Version
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)  # Needs Update
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)  # Actions
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.Stretch)           # Details
        
        # Remove alternating row colors for better readability
        self.status_table.setAlternatingRowColors(False)
        self.status_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
    
    def create_summary_section(self):
        """Create the summary section."""
        self.summary_group = QGroupBox("Summary")
        layout = QHBoxLayout(self.summary_group)
        
        self.total_entities_label = QLabel("Total Entities: 0")
        self.total_entities_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        layout.addWidget(self.total_entities_label)
        
        # Separator
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.VLine)
        layout.addWidget(separator)
        
        self.sources_status_label = QLabel("Sources: 0/0 Updated")
        layout.addWidget(self.sources_status_label)
        
        layout.addStretch()
        
        self.last_check_label = QLabel("Last Check: Never")
        layout.addWidget(self.last_check_label)
    
    def setup_timer(self):
        """Setup automatic refresh timer."""
        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_status)
        self.timer.start(300000)  # Refresh every 5 minutes
    
    def refresh_status(self):
        """Refresh the status display."""
        try:
            # Get status for all sources
            all_status = self.data_service.get_all_status()
            
            # Update table
            self.update_status_table(all_status)
            
            # Update summary
            self.update_summary(all_status)
            
            # Update last check time
            self.last_check_label.setText(f"Last Check: {datetime.now().strftime('%H:%M:%S')}")
            
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to refresh status: {str(e)}")
    
    def update_status_table(self, all_status: Dict[str, DataSourceStatus]):
        """Update the status table with current data."""
        self.status_table.setRowCount(len(all_status))
        
        for row, (source_id, status) in enumerate(all_status.items()):
            # Source name
            self.status_table.setItem(row, 0, QTableWidgetItem(source_id))
            
            # Status indicator
            status_item = QTableWidgetItem("✅ Downloaded" if status.is_downloaded else "❌ Not Downloaded")
            # Remove background colors for better text readability
            self.status_table.setItem(row, 1, status_item)
            
            # Last download
            last_download = "Never"
            if status.last_download:
                last_download = status.last_download.strftime("%Y-%m-%d %H:%M")
            self.status_table.setItem(row, 2, QTableWidgetItem(last_download))
            
            # File size
            file_size = self.format_file_size(status.file_size)
            self.status_table.setItem(row, 3, QTableWidgetItem(file_size))
            
            # Entity count
            self.status_table.setItem(row, 4, QTableWidgetItem(str(status.entity_count)))
            
            # Version - show both date and file hash if available
            version_info = []
            if status.version:
                version_info.append(status.version)
            if status.file_hash:
                version_info.append(f"#{status.file_hash[:8]}")
            
            version_text = " | ".join(version_info) if version_info else "Unknown"
            self.status_table.setItem(row, 5, QTableWidgetItem(version_text))
            
            # Needs update
            needs_update_item = QTableWidgetItem("⚠️ Yes" if status.needs_update else "✅ No")
            # Remove background colors for better text readability
            self.status_table.setItem(row, 6, needs_update_item)
            
            # Actions button
            action_btn = QPushButton("Download")
            action_btn.clicked.connect(lambda checked, sid=source_id: self.download_single(sid))
            self.status_table.setCellWidget(row, 7, action_btn)
            
            # Details/Error message
            details = status.error_message or "OK"
            self.status_table.setItem(row, 8, QTableWidgetItem(details))
    
    def update_summary(self, all_status: Dict[str, DataSourceStatus]):
        """Update the summary section."""
        total_entities = sum(status.entity_count for status in all_status.values())
        self.total_entities_label.setText(f"Total Entities: {total_entities:,}")
        
        downloaded_count = sum(1 for status in all_status.values() if status.is_downloaded)
        total_count = len(all_status)
        self.sources_status_label.setText(f"Sources: {downloaded_count}/{total_count} Downloaded")
    
    def format_file_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        if size_bytes == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        
        return f"{size_bytes:.1f} TB"
    
    def download_single(self, source_id: str):
        """Download a single data source."""
        if self.download_worker and self.download_worker.isRunning():
            QMessageBox.information(self, "Download in Progress", 
                                  "A download is already in progress. Please wait.")
            return
        
        self.start_download([source_id], force=False)
    
    def download_all(self):
        """Download all data sources that need updates."""
        if self.download_worker and self.download_worker.isRunning():
            QMessageBox.information(self, "Download in Progress", 
                                  "A download is already in progress. Please wait.")
            return
        
        # Get all source IDs
        source_ids = list(self.data_service.data_sources.keys())
        self.start_download(source_ids, force=False)
    
    def force_download_all(self):
        """Force download all data sources."""
        reply = QMessageBox.question(self, "Force Download", 
                                    "This will re-download all data sources regardless of their current status. Continue?",
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.Yes:
            if self.download_worker and self.download_worker.isRunning():
                QMessageBox.information(self, "Download in Progress", 
                                      "A download is already in progress. Please wait.")
                return
            
            source_ids = list(self.data_service.data_sources.keys())
            self.start_download(source_ids, force=True)
    
    def start_download(self, source_ids: list, force: bool = False):
        """Start downloading the specified sources."""
        # Disable buttons
        self.download_all_btn.setEnabled(False)
        self.force_download_btn.setEnabled(False)
        
        # Show progress
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(len(source_ids))
        self.progress_bar.setValue(0)
        self.progress_label.setText("Starting download...")
        
        # Create and start worker
        self.download_worker = DataDownloadWorker(self.data_service, source_ids, force)
        self.download_worker.progress.connect(self.on_download_progress)
        self.download_worker.finished.connect(self.on_download_finished)
        self.download_worker.all_finished.connect(self.on_all_downloads_finished)
        self.download_worker.start()
    
    def on_download_progress(self, source_id: str, message: str):
        """Handle download progress updates."""
        self.progress_label.setText(message)
    
    def on_download_finished(self, source_id: str, success: bool):
        """Handle individual download completion."""
        current_value = self.progress_bar.value()
        self.progress_bar.setValue(current_value + 1)
    
    def on_all_downloads_finished(self, results: dict):
        """Handle completion of all downloads."""
        # Re-enable buttons
        self.download_all_btn.setEnabled(True)
        self.force_download_btn.setEnabled(True)
        
        # Hide progress
        self.progress_bar.setVisible(False)
        self.progress_label.setText("")
        
        # Show results
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        if successful == total:
            QMessageBox.information(self, "Download Complete", 
                                  f"All {total} data sources downloaded successfully!")
        else:
            failed = total - successful
            QMessageBox.warning(self, "Download Complete", 
                              f"Downloaded {successful}/{total} sources successfully. {failed} failed.")
        
        # Refresh status
        self.refresh_status()
    
    def closeEvent(self, event):
        """Handle widget close event."""
        if self.download_worker and self.download_worker.isRunning():
            self.download_worker.terminate()
            self.download_worker.wait()
        
        if hasattr(self, 'timer'):
            self.timer.stop()
        
        event.accept()