"""
Custom Sanctions Import Dialog

This module provides a GUI dialog for importing custom sanctions data from XML files.
It includes file selection, schema validation, preview functionality, and progress tracking.
"""

import os
import sys
from typing import List, Dict, Optional, Tuple
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFileDialog,
    QTextEdit, QTableWidget, QTableWidgetItem, QProgressBar, QTabWidget,
    QWidget, QGroupBox, QCheckBox, QComboBox, QMessageBox, QSplitter,
    QHeaderView, QFrame, QScrollArea
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QIcon, QPixmap

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from sanctions_checker.services.custom_sanctions_service import CustomSanctionsService
from sanctions_checker.services.custom_sanctions_xml_processor import ImportResult, ValidationResult


class ImportWorkerThread(QThread):
    """Worker thread for handling XML import operations."""
    
    progress_updated = pyqtSignal(int, str)  # progress percentage, status message
    import_completed = pyqtSignal(object)  # ImportResult
    error_occurred = pyqtSignal(str)  # error message
    
    def __init__(self, service: CustomSanctionsService, xml_content: str, 
                 conflict_resolution: str = 'skip'):
        super().__init__()
        self.service = service
        self.xml_content = xml_content
        self.conflict_resolution = conflict_resolution
        
    def run(self):
        """Execute the import operation."""
        try:
            self.progress_updated.emit(10, "Validating XML schema...")
            
            # Validate XML schema first
            validation_result = self.service.xml_processor.validate_against_schema(self.xml_content)
            if not validation_result.is_valid:
                self.error_occurred.emit(f"Schema validation failed: {validation_result.error_message}")
                return
                
            self.progress_updated.emit(30, "Parsing XML entities...")
            
            # Parse entities from XML
            entities_data = self.service.xml_processor.import_entities_from_xml(self.xml_content)
            
            self.progress_updated.emit(50, "Processing entities...")
            
            # Import entities with progress tracking
            total_entities = len(entities_data)
            imported_count = 0
            skipped_count = 0
            error_count = 0
            errors = []
            
            for i, entity_data in enumerate(entities_data):
                try:
                    # Check for duplicates
                    duplicates = self.service.check_for_duplicates(entity_data)
                    
                    if duplicates and self.conflict_resolution == 'skip':
                        skipped_count += 1
                    elif duplicates and self.conflict_resolution == 'update':
                        # Update existing entity
                        self.service.update_sanction_entity(duplicates[0], entity_data)
                        imported_count += 1
                    else:
                        # Create new entity
                        self.service.create_sanction_entity(entity_data)
                        imported_count += 1
                        
                except Exception as e:
                    error_count += 1
                    errors.append(f"Entity {i+1}: {str(e)}")
                
                # Update progress
                progress = 50 + int((i + 1) / total_entities * 40)
                self.progress_updated.emit(progress, f"Processed {i+1}/{total_entities} entities...")
            
            self.progress_updated.emit(100, "Import completed")
            
            # Create import result
            result = ImportResult(
                total_processed=total_entities,
                imported_count=imported_count,
                skipped_count=skipped_count,
                error_count=error_count,
                errors=errors
            )
            
            self.import_completed.emit(result)
            
        except Exception as e:
            self.error_occurred.emit(f"Import failed: {str(e)}")


class CustomSanctionsImportDialog(QDialog):
    """Dialog for importing custom sanctions data from XML files."""
    
    def __init__(self, service: CustomSanctionsService, parent=None):
        super().__init__(parent)
        self.service = service
        self.xml_content = ""
        self.parsed_entities = []
        self.import_worker = None
        
        self.setWindowTitle("Import Custom Sanctions")
        self.setModal(True)
        self.resize(900, 700)
        
        self.setup_ui()
        self.setup_connections()
        
    def setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)
        
        # Create tab widget
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)
        
        # File Selection Tab
        self.setup_file_selection_tab()
        
        # Preview Tab
        self.setup_preview_tab()
        
        # Import Progress Tab
        self.setup_progress_tab()
        
        # Button layout
        button_layout = QHBoxLayout()
        
        self.cancel_button = QPushButton("Cancel")
        self.back_button = QPushButton("Back")
        self.next_button = QPushButton("Next")
        self.import_button = QPushButton("Import")
        self.close_button = QPushButton("Close")
        
        button_layout.addStretch()
        button_layout.addWidget(self.cancel_button)
        button_layout.addWidget(self.back_button)
        button_layout.addWidget(self.next_button)
        button_layout.addWidget(self.import_button)
        button_layout.addWidget(self.close_button)
        
        layout.addLayout(button_layout)
        
        # Initial button states
        self.back_button.setEnabled(False)
        self.next_button.setEnabled(False)
        self.import_button.setEnabled(False)
        self.close_button.setVisible(False)
        
    def setup_file_selection_tab(self):
        """Set up the file selection tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # File selection group
        file_group = QGroupBox("Select XML File")
        file_layout = QVBoxLayout(file_group)
        
        # File path display
        file_path_layout = QHBoxLayout()
        self.file_path_label = QLabel("No file selected")
        self.file_path_label.setStyleSheet("QLabel { border: 1px solid gray; padding: 5px; }")
        self.browse_button = QPushButton("Browse...")
        
        file_path_layout.addWidget(self.file_path_label, 1)
        file_path_layout.addWidget(self.browse_button)
        file_layout.addLayout(file_path_layout)
        
        layout.addWidget(file_group)
        
        # Validation results group
        validation_group = QGroupBox("Validation Results")
        validation_layout = QVBoxLayout(validation_group)
        
        self.validation_text = QTextEdit()
        self.validation_text.setReadOnly(True)
        self.validation_text.setMaximumHeight(150)
        validation_layout.addWidget(self.validation_text)
        
        layout.addWidget(validation_group)
        
        # File information group
        info_group = QGroupBox("File Information")
        info_layout = QVBoxLayout(info_group)
        
        self.file_info_text = QTextEdit()
        self.file_info_text.setReadOnly(True)
        self.file_info_text.setMaximumHeight(100)
        info_layout.addWidget(self.file_info_text)
        
        layout.addWidget(info_group)
        
        layout.addStretch()
        
        self.tab_widget.addTab(tab, "File Selection")
        
    def setup_preview_tab(self):
        """Set up the preview tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Preview options group
        options_group = QGroupBox("Import Options")
        options_layout = QHBoxLayout(options_group)
        
        options_layout.addWidget(QLabel("Conflict Resolution:"))
        self.conflict_combo = QComboBox()
        self.conflict_combo.addItems([
            "Skip existing entities",
            "Update existing entities",
            "Create duplicates"
        ])
        options_layout.addWidget(self.conflict_combo)
        options_layout.addStretch()
        
        layout.addWidget(options_group)
        
        # Preview table
        preview_group = QGroupBox("Entities to Import")
        preview_layout = QVBoxLayout(preview_group)
        
        self.preview_table = QTableWidget()
        self.preview_table.setColumnCount(6)
        self.preview_table.setHorizontalHeaderLabels([
            "Internal ID", "Subject Type", "Primary Name", "Authority", "Status", "Action"
        ])
        
        # Set column widths
        header = self.preview_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        
        preview_layout.addWidget(self.preview_table)
        
        layout.addWidget(preview_group)
        
        # Summary group
        summary_group = QGroupBox("Import Summary")
        summary_layout = QHBoxLayout(summary_group)
        
        self.summary_label = QLabel("No entities to import")
        summary_layout.addWidget(self.summary_label)
        summary_layout.addStretch()
        
        layout.addWidget(summary_group)
        
        self.tab_widget.addTab(tab, "Preview")
        
    def setup_progress_tab(self):
        """Set up the progress tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Progress group
        progress_group = QGroupBox("Import Progress")
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        self.progress_label = QLabel("Ready to import")
        
        progress_layout.addWidget(self.progress_label)
        progress_layout.addWidget(self.progress_bar)
        
        layout.addWidget(progress_group)
        
        # Results group
        results_group = QGroupBox("Import Results")
        results_layout = QVBoxLayout(results_group)
        
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        results_layout.addWidget(self.results_text)
        
        layout.addWidget(results_group)
        
        self.tab_widget.addTab(tab, "Progress")
        
    def setup_connections(self):
        """Set up signal connections."""
        self.browse_button.clicked.connect(self.browse_file)
        self.cancel_button.clicked.connect(self.reject)
        self.back_button.clicked.connect(self.go_back)
        self.next_button.clicked.connect(self.go_next)
        self.import_button.clicked.connect(self.start_import)
        self.close_button.clicked.connect(self.accept)
        
    def browse_file(self):
        """Open file dialog to select XML file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select XML File",
            "",
            "XML Files (*.xml);;All Files (*)"
        )
        
        if file_path:
            self.load_file(file_path)
            
    def load_file(self, file_path: str):
        """Load and validate the selected XML file."""
        try:
            # Read file content
            with open(file_path, 'r', encoding='utf-8') as f:
                self.xml_content = f.read()
            
            # Update file path display
            self.file_path_label.setText(file_path)
            
            # Show file information
            file_size = os.path.getsize(file_path)
            file_info = f"File: {os.path.basename(file_path)}\n"
            file_info += f"Size: {file_size:,} bytes\n"
            file_info += f"Path: {file_path}"
            self.file_info_text.setText(file_info)
            
            # Validate XML schema
            self.validate_xml()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load file: {str(e)}")
            
    def validate_xml(self):
        """Validate the XML content against the schema."""
        try:
            validation_result = self.service.xml_processor.validate_against_schema(self.xml_content)
            
            if validation_result.is_valid:
                self.validation_text.setText("✓ XML schema validation passed")
                self.validation_text.setStyleSheet("QTextEdit { color: green; }")
                
                # Parse entities for preview
                self.parse_entities_for_preview()
                
                self.next_button.setEnabled(True)
            else:
                self.validation_text.setText(f"✗ Schema validation failed:\n{validation_result.error_message}")
                self.validation_text.setStyleSheet("QTextEdit { color: red; }")
                self.next_button.setEnabled(False)
                
        except Exception as e:
            self.validation_text.setText(f"✗ Validation error: {str(e)}")
            self.validation_text.setStyleSheet("QTextEdit { color: red; }")
            self.next_button.setEnabled(False)
            
    def parse_entities_for_preview(self):
        """Parse entities from XML for preview display."""
        try:
            self.parsed_entities = self.service.xml_processor.import_entities_from_xml(self.xml_content)
            
        except Exception as e:
            QMessageBox.warning(self, "Warning", f"Failed to parse entities: {str(e)}")
            self.parsed_entities = []
            
    def populate_preview_table(self):
        """Populate the preview table with parsed entities."""
        self.preview_table.setRowCount(len(self.parsed_entities))
        
        new_count = 0
        update_count = 0
        skip_count = 0
        
        for row, entity_data in enumerate(self.parsed_entities):
            # Internal ID
            internal_id = entity_data.get('internal_entry_id', 'N/A')
            self.preview_table.setItem(row, 0, QTableWidgetItem(internal_id))
            
            # Subject Type
            subject_type = entity_data.get('subject_type', 'N/A')
            self.preview_table.setItem(row, 1, QTableWidgetItem(subject_type))
            
            # Primary Name
            names = entity_data.get('names', [])
            primary_name = next((n['full_name'] for n in names if n.get('name_type') == 'Primary'), 'N/A')
            self.preview_table.setItem(row, 2, QTableWidgetItem(primary_name))
            
            # Authority
            authority = entity_data.get('sanctioning_authority', 'N/A')
            self.preview_table.setItem(row, 3, QTableWidgetItem(authority))
            
            # Status
            status = entity_data.get('record_status', 'Active')
            self.preview_table.setItem(row, 4, QTableWidgetItem(status))
            
            # Action (check for duplicates)
            duplicates = self.service.check_for_duplicates(entity_data)
            if duplicates:
                action = "Update" if self.conflict_combo.currentIndex() == 1 else "Skip"
                if action == "Skip":
                    skip_count += 1
                else:
                    update_count += 1
            else:
                action = "Create"
                new_count += 1
                
            action_item = QTableWidgetItem(action)
            if action == "Skip":
                action_item.setBackground(Qt.GlobalColor.yellow)
            elif action == "Update":
                action_item.setBackground(Qt.GlobalColor.cyan)
            else:
                action_item.setBackground(Qt.GlobalColor.lightGreen)
                
            self.preview_table.setItem(row, 5, action_item)
        
        # Update summary
        total = len(self.parsed_entities)
        summary = f"Total entities: {total} | New: {new_count} | Updates: {update_count} | Skipped: {skip_count}"
        self.summary_label.setText(summary)
        
    def go_back(self):
        """Go to previous tab."""
        current_index = self.tab_widget.currentIndex()
        if current_index > 0:
            self.tab_widget.setCurrentIndex(current_index - 1)
            self.update_button_states()
            
    def go_next(self):
        """Go to next tab."""
        current_index = self.tab_widget.currentIndex()
        
        if current_index == 0:  # File selection to preview
            self.populate_preview_table()
            self.tab_widget.setCurrentIndex(1)
        elif current_index == 1:  # Preview to progress
            self.tab_widget.setCurrentIndex(2)
            
        self.update_button_states()
        
    def update_button_states(self):
        """Update button states based on current tab."""
        current_index = self.tab_widget.currentIndex()
        
        self.back_button.setEnabled(current_index > 0)
        self.next_button.setEnabled(current_index < 2 and bool(self.xml_content))
        self.import_button.setEnabled(current_index == 2 and bool(self.parsed_entities))
        
        if current_index == 2:
            self.next_button.setVisible(False)
            self.import_button.setVisible(True)
        else:
            self.next_button.setVisible(True)
            self.import_button.setVisible(False)
            
    def start_import(self):
        """Start the import process."""
        if not self.parsed_entities:
            QMessageBox.warning(self, "Warning", "No entities to import")
            return
            
        # Get conflict resolution strategy
        conflict_strategies = ['skip', 'update', 'create']
        conflict_resolution = conflict_strategies[self.conflict_combo.currentIndex()]
        
        # Disable buttons during import
        self.import_button.setEnabled(False)
        self.back_button.setEnabled(False)
        self.cancel_button.setEnabled(False)
        
        # Start import worker thread
        self.import_worker = ImportWorkerThread(
            self.service, 
            self.xml_content, 
            conflict_resolution
        )
        
        self.import_worker.progress_updated.connect(self.update_progress)
        self.import_worker.import_completed.connect(self.import_completed)
        self.import_worker.error_occurred.connect(self.import_error)
        
        self.import_worker.start()
        
    def update_progress(self, percentage: int, message: str):
        """Update progress bar and message."""
        self.progress_bar.setValue(percentage)
        self.progress_label.setText(message)
        
    def import_completed(self, result: ImportResult):
        """Handle import completion."""
        self.progress_bar.setValue(100)
        self.progress_label.setText("Import completed")
        
        # Display results
        results_text = f"Import Results:\n\n"
        results_text += f"Total processed: {result.total_processed}\n"
        results_text += f"Successfully imported: {result.imported_count}\n"
        results_text += f"Skipped (duplicates): {result.skipped_count}\n"
        results_text += f"Errors: {result.error_count}\n\n"
        
        if result.errors:
            results_text += "Errors encountered:\n"
            for error in result.errors[:10]:  # Show first 10 errors
                results_text += f"• {error}\n"
            if len(result.errors) > 10:
                results_text += f"... and {len(result.errors) - 10} more errors\n"
                
        self.results_text.setText(results_text)
        
        # Show completion message
        if result.error_count == 0:
            QMessageBox.information(
                self, 
                "Import Successful", 
                f"Successfully imported {result.imported_count} entities"
            )
        else:
            QMessageBox.warning(
                self, 
                "Import Completed with Errors", 
                f"Imported {result.imported_count} entities with {result.error_count} errors"
            )
        
        # Enable close button
        self.close_button.setVisible(True)
        self.cancel_button.setVisible(False)
        
    def import_error(self, error_message: str):
        """Handle import error."""
        self.progress_label.setText("Import failed")
        self.results_text.setText(f"Import failed: {error_message}")
        
        QMessageBox.critical(self, "Import Error", error_message)
        
        # Re-enable buttons
        self.import_button.setEnabled(True)
        self.back_button.setEnabled(True)
        self.cancel_button.setEnabled(True)
        
    def closeEvent(self, event):
        """Handle dialog close event."""
        if self.import_worker and self.import_worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Import in Progress",
                "Import is still running. Do you want to cancel it?",
                QMessageBox.Yes | QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.import_worker.terminate()
                self.import_worker.wait()
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
        def __init__(self):
            self.xml_processor = MockXMLProcessor()
            
        def check_for_duplicates(self, entity_data):
            return []
            
    class MockXMLProcessor:
        def validate_against_schema(self, xml_content):
            from collections import namedtuple
            ValidationResult = namedtuple('ValidationResult', ['is_valid', 'error_message'])
            return ValidationResult(True, "")
            
        def import_entities_from_xml(self, xml_content):
            return []
    
    service = MockService()
    dialog = CustomSanctionsImportDialog(service)
    dialog.show()
    
    sys.exit(app.exec_())