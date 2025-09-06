"""
Custom Sanctions Export Dialog

This module provides a GUI dialog for exporting custom sanctions data to XML files.
It includes filtering options, file naming, and progress tracking.
"""

import os
import sys
from datetime import datetime, date
from typing import List, Dict, Optional
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFileDialog,
    QTextEdit, QTableWidget, QTableWidgetItem, QProgressBar, QTabWidget,
    QWidget, QGroupBox, QCheckBox, QComboBox, QMessageBox, QSplitter,
    QHeaderView, QFrame, QScrollArea, QLineEdit, QDateEdit, QSpinBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QDate
from PyQt6.QtGui import QFont, QIcon, QPixmap

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from sanctions_checker.services.custom_sanctions_service import CustomSanctionsService
from sanctions_checker.models.base import SubjectType, RecordStatus


class ExportWorkerThread(QThread):
    """Worker thread for handling XML export operations."""
    
    progress_updated = pyqtSignal(int, str)  # progress percentage, status message
    export_completed = pyqtSignal(str, int)  # file_path, entity_count
    error_occurred = pyqtSignal(str)  # error message
    
    def __init__(self, service: CustomSanctionsService, filters: Dict, file_path: str):
        super().__init__()
        self.service = service
        self.filters = filters
        self.file_path = file_path
        
    def run(self):
        """Execute the export operation."""
        try:
            self.progress_updated.emit(10, "Counting entities to export...")
            
            # Count entities first
            entity_count = self.service.count_sanction_entities(self.filters)
            
            if entity_count == 0:
                self.error_occurred.emit("No entities found matching the specified filters")
                return
            
            self.progress_updated.emit(30, f"Exporting {entity_count} entities...")
            
            # Export to XML
            xml_content = self.service.export_to_xml(filters=self.filters, file_path=self.file_path)
            
            self.progress_updated.emit(100, "Export completed")
            self.export_completed.emit(self.file_path, entity_count)
            
        except Exception as e:
            self.error_occurred.emit(f"Export failed: {str(e)}")


class CustomSanctionsExportDialog(QDialog):
    """Dialog for exporting custom sanctions data to XML files."""
    
    def __init__(self, service: CustomSanctionsService, parent=None):
        super().__init__(parent)
        self.service = service
        self.export_worker = None
        
        self.setWindowTitle("Export Custom Sanctions")
        self.setModal(True)
        self.resize(800, 600)
        
        self.setup_ui()
        self.setup_connections()
        self.load_filter_options()
        
    def setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)
        
        # Create tab widget
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)
        
        # Filter Options Tab
        self.setup_filter_tab()
        
        # Export Options Tab
        self.setup_export_tab()
        
        # Progress Tab
        self.setup_progress_tab()
        
        # Button layout
        button_layout = QHBoxLayout()
        
        self.cancel_button = QPushButton("Cancel")
        self.back_button = QPushButton("Back")
        self.next_button = QPushButton("Next")
        self.export_button = QPushButton("Export")
        self.close_button = QPushButton("Close")
        
        button_layout.addStretch()
        button_layout.addWidget(self.cancel_button)
        button_layout.addWidget(self.back_button)
        button_layout.addWidget(self.next_button)
        button_layout.addWidget(self.export_button)
        button_layout.addWidget(self.close_button)
        
        layout.addLayout(button_layout)
        
        # Initial button states
        self.back_button.setEnabled(False)
        self.export_button.setVisible(False)
        self.close_button.setVisible(False)
        
    def setup_filter_tab(self):
        """Set up the filter options tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Entity Type Filter
        type_group = QGroupBox("Subject Type Filter")
        type_layout = QVBoxLayout(type_group)
        
        self.all_types_checkbox = QCheckBox("All Subject Types")
        self.all_types_checkbox.setChecked(True)
        type_layout.addWidget(self.all_types_checkbox)
        
        self.type_checkboxes = {}
        for subject_type in SubjectType:
            checkbox = QCheckBox(subject_type.value)
            checkbox.setEnabled(False)  # Disabled when "All" is checked
            self.type_checkboxes[subject_type] = checkbox
            type_layout.addWidget(checkbox)
        
        layout.addWidget(type_group)
        
        # Status Filter
        status_group = QGroupBox("Record Status Filter")
        status_layout = QVBoxLayout(status_group)
        
        self.all_statuses_checkbox = QCheckBox("All Statuses")
        self.all_statuses_checkbox.setChecked(True)
        status_layout.addWidget(self.all_statuses_checkbox)
        
        self.status_checkboxes = {}
        for status in RecordStatus:
            checkbox = QCheckBox(status.value)
            checkbox.setEnabled(False)  # Disabled when "All" is checked
            self.status_checkboxes[status] = checkbox
            status_layout.addWidget(checkbox)
        
        layout.addWidget(status_group)
        
        # Date Range Filter
        date_group = QGroupBox("Date Range Filter")
        date_layout = QVBoxLayout(date_group)
        
        self.date_filter_checkbox = QCheckBox("Filter by Listing Date")
        date_layout.addWidget(self.date_filter_checkbox)
        
        date_range_layout = QHBoxLayout()
        date_range_layout.addWidget(QLabel("From:"))
        self.date_from = QDateEdit()
        self.date_from.setDate(QDate.currentDate().addYears(-1))
        self.date_from.setEnabled(False)
        date_range_layout.addWidget(self.date_from)
        
        date_range_layout.addWidget(QLabel("To:"))
        self.date_to = QDateEdit()
        self.date_to.setDate(QDate.currentDate())
        self.date_to.setEnabled(False)
        date_range_layout.addWidget(self.date_to)
        
        date_layout.addLayout(date_range_layout)
        layout.addWidget(date_group)
        
        # Authority Filter
        authority_group = QGroupBox("Sanctioning Authority Filter")
        authority_layout = QVBoxLayout(authority_group)
        
        self.authority_filter_checkbox = QCheckBox("Filter by Authority")
        authority_layout.addWidget(self.authority_filter_checkbox)
        
        self.authority_combo = QComboBox()
        self.authority_combo.setEnabled(False)
        authority_layout.addWidget(self.authority_combo)
        
        layout.addWidget(authority_group)
        
        # Search Term Filter
        search_group = QGroupBox("Search Filter")
        search_layout = QVBoxLayout(search_group)
        
        self.search_filter_checkbox = QCheckBox("Filter by Search Term")
        search_layout.addWidget(self.search_filter_checkbox)
        
        self.search_term_edit = QLineEdit()
        self.search_term_edit.setPlaceholderText("Enter name or internal ID to search...")
        self.search_term_edit.setEnabled(False)
        search_layout.addWidget(self.search_term_edit)
        
        layout.addWidget(search_group)
        
        layout.addStretch()
        
        self.tab_widget.addTab(tab, "Filters")
        
    def setup_export_tab(self):
        """Set up the export options tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Preview group
        preview_group = QGroupBox("Export Preview")
        preview_layout = QVBoxLayout(preview_group)
        
        self.preview_label = QLabel("Click 'Refresh Preview' to see entities that will be exported")
        preview_layout.addWidget(self.preview_label)
        
        self.refresh_preview_button = QPushButton("Refresh Preview")
        preview_layout.addWidget(self.refresh_preview_button)
        
        self.preview_table = QTableWidget()
        self.preview_table.setColumnCount(5)
        self.preview_table.setHorizontalHeaderLabels([
            "Internal ID", "Subject Type", "Primary Name", "Authority", "Status"
        ])
        self.preview_table.setMaximumHeight(200)
        
        # Set column widths
        header = self.preview_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        
        preview_layout.addWidget(self.preview_table)
        
        layout.addWidget(preview_group)
        
        # File options group
        file_group = QGroupBox("Export File Options")
        file_layout = QVBoxLayout(file_group)
        
        # File path selection
        file_path_layout = QHBoxLayout()
        file_path_layout.addWidget(QLabel("Export File:"))
        self.file_path_edit = QLineEdit()
        self.file_path_edit.setPlaceholderText("Select export file location...")
        file_path_layout.addWidget(self.file_path_edit)
        
        self.browse_file_button = QPushButton("Browse...")
        file_path_layout.addWidget(self.browse_file_button)
        
        file_layout.addLayout(file_path_layout)
        
        # Auto-generate filename option
        self.auto_filename_checkbox = QCheckBox("Auto-generate filename with timestamp")
        self.auto_filename_checkbox.setChecked(True)
        file_layout.addWidget(self.auto_filename_checkbox)
        
        layout.addWidget(file_group)
        
        # Export summary
        summary_group = QGroupBox("Export Summary")
        summary_layout = QVBoxLayout(summary_group)
        
        self.summary_text = QTextEdit()
        self.summary_text.setReadOnly(True)
        self.summary_text.setMaximumHeight(100)
        summary_layout.addWidget(self.summary_text)
        
        layout.addWidget(summary_group)
        
        layout.addStretch()
        
        self.tab_widget.addTab(tab, "Export Options")
        
    def setup_progress_tab(self):
        """Set up the progress tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Progress group
        progress_group = QGroupBox("Export Progress")
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        self.progress_label = QLabel("Ready to export")
        
        progress_layout.addWidget(self.progress_label)
        progress_layout.addWidget(self.progress_bar)
        
        layout.addWidget(progress_group)
        
        # Results group
        results_group = QGroupBox("Export Results")
        results_layout = QVBoxLayout(results_group)
        
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        results_layout.addWidget(self.results_text)
        
        layout.addWidget(results_group)
        
        self.tab_widget.addTab(tab, "Progress")
        
    def setup_connections(self):
        """Set up signal connections."""
        # Button connections
        self.cancel_button.clicked.connect(self.reject)
        self.back_button.clicked.connect(self.go_back)
        self.next_button.clicked.connect(self.go_next)
        self.export_button.clicked.connect(self.start_export)
        self.close_button.clicked.connect(self.accept)
        
        # Filter connections
        self.all_types_checkbox.toggled.connect(self.toggle_type_filters)
        self.all_statuses_checkbox.toggled.connect(self.toggle_status_filters)
        self.date_filter_checkbox.toggled.connect(self.toggle_date_filter)
        self.authority_filter_checkbox.toggled.connect(self.toggle_authority_filter)
        self.search_filter_checkbox.toggled.connect(self.toggle_search_filter)
        
        # Export options connections
        self.refresh_preview_button.clicked.connect(self.refresh_preview)
        self.browse_file_button.clicked.connect(self.browse_export_file)
        self.auto_filename_checkbox.toggled.connect(self.toggle_auto_filename)
        
    def load_filter_options(self):
        """Load available filter options from the database."""
        try:
            # Load sanctioning authorities
            entities = self.service.list_sanction_entities()
            authorities = set()
            for entity in entities:
                if entity.sanctioning_authority:
                    authorities.add(entity.sanctioning_authority)
            
            self.authority_combo.addItems(sorted(authorities))
            
        except Exception as e:
            print(f"Error loading filter options: {e}")
            
    def toggle_type_filters(self, checked: bool):
        """Toggle individual type checkboxes based on 'All Types' checkbox."""
        for checkbox in self.type_checkboxes.values():
            checkbox.setEnabled(not checked)
            if checked:
                checkbox.setChecked(False)
                
    def toggle_status_filters(self, checked: bool):
        """Toggle individual status checkboxes based on 'All Statuses' checkbox."""
        for checkbox in self.status_checkboxes.values():
            checkbox.setEnabled(not checked)
            if checked:
                checkbox.setChecked(False)
                
    def toggle_date_filter(self, checked: bool):
        """Toggle date range controls."""
        self.date_from.setEnabled(checked)
        self.date_to.setEnabled(checked)
        
    def toggle_authority_filter(self, checked: bool):
        """Toggle authority combo box."""
        self.authority_combo.setEnabled(checked)
        
    def toggle_search_filter(self, checked: bool):
        """Toggle search term edit."""
        self.search_term_edit.setEnabled(checked)
        
    def toggle_auto_filename(self, checked: bool):
        """Toggle auto filename generation."""
        self.file_path_edit.setEnabled(not checked)
        self.browse_file_button.setEnabled(not checked)
        
        if checked:
            # Generate default filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"custom_sanctions_export_{timestamp}.xml"
            self.file_path_edit.setText(filename)
            
    def browse_export_file(self):
        """Open file dialog to select export file location."""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Select Export File Location",
            "custom_sanctions_export.xml",
            "XML Files (*.xml);;All Files (*)"
        )
        
        if file_path:
            self.file_path_edit.setText(file_path)
            
    def get_current_filters(self) -> Dict:
        """Get current filter settings as a dictionary."""
        filters = {}
        
        # Subject type filters
        if not self.all_types_checkbox.isChecked():
            selected_types = []
            for subject_type, checkbox in self.type_checkboxes.items():
                if checkbox.isChecked():
                    selected_types.append(subject_type)
            if selected_types:
                filters['subject_type'] = selected_types[0] if len(selected_types) == 1 else selected_types
        
        # Status filters
        if not self.all_statuses_checkbox.isChecked():
            selected_statuses = []
            for status, checkbox in self.status_checkboxes.items():
                if checkbox.isChecked():
                    selected_statuses.append(status)
            if selected_statuses:
                filters['record_status'] = selected_statuses[0] if len(selected_statuses) == 1 else selected_statuses
        
        # Date range filter
        if self.date_filter_checkbox.isChecked():
            filters['date_from'] = self.date_from.date().toPyDate()
            filters['date_to'] = self.date_to.date().toPyDate()
        
        # Authority filter
        if self.authority_filter_checkbox.isChecked() and self.authority_combo.currentText():
            filters['sanctioning_authority'] = self.authority_combo.currentText()
        
        # Search term filter
        if self.search_filter_checkbox.isChecked() and self.search_term_edit.text().strip():
            filters['search_term'] = self.search_term_edit.text().strip()
        
        return filters
        
    def refresh_preview(self):
        """Refresh the export preview with current filters."""
        try:
            filters = self.get_current_filters()
            
            # Get entities matching filters (limit to first 100 for preview)
            entities = self.service.list_sanction_entities(filters=filters, limit=100)
            total_count = self.service.count_sanction_entities(filters=filters)
            
            # Update preview table
            self.preview_table.setRowCount(len(entities))
            
            for row, entity in enumerate(entities):
                # Internal ID
                self.preview_table.setItem(row, 0, QTableWidgetItem(entity.internal_entry_id))
                
                # Subject Type
                self.preview_table.setItem(row, 1, QTableWidgetItem(entity.subject_type.value))
                
                # Primary Name
                primary_name = "N/A"
                for name in entity.names:
                    if name.name_type.value == "Primary":
                        primary_name = name.full_name
                        break
                self.preview_table.setItem(row, 2, QTableWidgetItem(primary_name))
                
                # Authority
                self.preview_table.setItem(row, 3, QTableWidgetItem(entity.sanctioning_authority))
                
                # Status
                self.preview_table.setItem(row, 4, QTableWidgetItem(entity.record_status.value))
            
            # Update preview label
            if total_count > 100:
                self.preview_label.setText(f"Showing first 100 of {total_count} entities that will be exported")
            else:
                self.preview_label.setText(f"All {total_count} entities that will be exported")
            
            # Update summary
            summary = f"Export Summary:\n"
            summary += f"Total entities to export: {total_count}\n"
            summary += f"Applied filters: {len(filters)} active\n"
            
            if filters:
                summary += "\nActive filters:\n"
                for key, value in filters.items():
                    summary += f"• {key}: {value}\n"
            
            self.summary_text.setText(summary)
            
        except Exception as e:
            QMessageBox.warning(self, "Preview Error", f"Failed to refresh preview: {str(e)}")
            
    def go_back(self):
        """Go to previous tab."""
        current_index = self.tab_widget.currentIndex()
        if current_index > 0:
            self.tab_widget.setCurrentIndex(current_index - 1)
            self.update_button_states()
            
    def go_next(self):
        """Go to next tab."""
        current_index = self.tab_widget.currentIndex()
        
        if current_index == 0:  # Filters to export options
            self.refresh_preview()
            self.tab_widget.setCurrentIndex(1)
        elif current_index == 1:  # Export options to progress
            self.tab_widget.setCurrentIndex(2)
            
        self.update_button_states()
        
    def update_button_states(self):
        """Update button states based on current tab."""
        current_index = self.tab_widget.currentIndex()
        
        self.back_button.setEnabled(current_index > 0)
        self.next_button.setEnabled(current_index < 2)
        self.export_button.setEnabled(current_index == 2)
        
        if current_index == 2:
            self.next_button.setVisible(False)
            self.export_button.setVisible(True)
        else:
            self.next_button.setVisible(True)
            self.export_button.setVisible(False)
            
    def start_export(self):
        """Start the export process."""
        # Validate export file path
        file_path = self.file_path_edit.text().strip()
        if not file_path:
            QMessageBox.warning(self, "Export Error", "Please specify an export file path")
            return
        
        # Handle auto-generated filename
        if self.auto_filename_checkbox.isChecked() and not os.path.isabs(file_path):
            # Make it relative to user's documents or current directory
            documents_dir = os.path.expanduser("~/Documents")
            if os.path.exists(documents_dir):
                file_path = os.path.join(documents_dir, file_path)
            else:
                file_path = os.path.abspath(file_path)
            self.file_path_edit.setText(file_path)
        
        # Check if file already exists
        if os.path.exists(file_path):
            reply = QMessageBox.question(
                self,
                "File Exists",
                f"The file '{file_path}' already exists. Do you want to overwrite it?",
                QMessageBox.Yes | QMessageBox.No
            )
            if reply != QMessageBox.Yes:
                return
        
        # Get current filters
        filters = self.get_current_filters()
        
        # Disable buttons during export
        self.export_button.setEnabled(False)
        self.back_button.setEnabled(False)
        self.cancel_button.setEnabled(False)
        
        # Start export worker thread
        self.export_worker = ExportWorkerThread(self.service, filters, file_path)
        
        self.export_worker.progress_updated.connect(self.update_progress)
        self.export_worker.export_completed.connect(self.export_completed)
        self.export_worker.error_occurred.connect(self.export_error)
        
        self.export_worker.start()
        
    def update_progress(self, percentage: int, message: str):
        """Update progress bar and message."""
        self.progress_bar.setValue(percentage)
        self.progress_label.setText(message)
        
    def export_completed(self, file_path: str, entity_count: int):
        """Handle export completion."""
        self.progress_bar.setValue(100)
        self.progress_label.setText("Export completed")
        
        # Display results
        results_text = f"Export Results:\n\n"
        results_text += f"Successfully exported {entity_count} entities\n"
        results_text += f"Export file: {file_path}\n"
        results_text += f"File size: {os.path.getsize(file_path):,} bytes\n"
        results_text += f"Export completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        self.results_text.setText(results_text)
        
        # Show completion message
        QMessageBox.information(
            self, 
            "Export Successful", 
            f"Successfully exported {entity_count} entities to:\n{file_path}"
        )
        
        # Enable close button
        self.close_button.setVisible(True)
        self.cancel_button.setVisible(False)
        
    def export_error(self, error_message: str):
        """Handle export error."""
        self.progress_label.setText("Export failed")
        self.results_text.setText(f"Export failed: {error_message}")
        
        QMessageBox.critical(self, "Export Error", error_message)
        
        # Re-enable buttons
        self.export_button.setEnabled(True)
        self.back_button.setEnabled(True)
        self.cancel_button.setEnabled(True)
        
    def closeEvent(self, event):
        """Handle dialog close event."""
        if self.export_worker and self.export_worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Export in Progress",
                "Export is still running. Do you want to cancel it?",
                QMessageBox.Yes | QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.export_worker.terminate()
                self.export_worker.wait()
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()


if __name__ == "__main__":
    from PyQt6.QtWidgets import QApplication
    import sys
    
    app = QApplication(sys.argv)
    
    # Mock service for testing
    class MockService:
        def list_sanction_entities(self, filters=None, limit=None):
            return []
            
        def count_sanction_entities(self, filters=None):
            return 0
    
    service = MockService()
    dialog = CustomSanctionsExportDialog(service)
    dialog.show()
    
    sys.exit(app.exec_())