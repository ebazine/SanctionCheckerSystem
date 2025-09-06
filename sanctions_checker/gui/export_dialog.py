"""
Export dialog for sanctions checker results with format selection, preview, and batch capabilities.
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit,
    QPushButton, QComboBox, QTextEdit, QGroupBox, QCheckBox, QSpinBox,
    QFileDialog, QMessageBox, QProgressBar, QTabWidget, QWidget,
    QTableWidget, QTableWidgetItem, QHeaderView, QSplitter, QFrame,
    QScrollArea, QButtonGroup, QRadioButton
)
from PyQt6.QtCore import Qt, pyqtSignal, QThread, pyqtSlot, QTimer
from PyQt6.QtGui import QFont, QPixmap, QPainter, QColor

from ..services.search_service import EntityMatch
from ..services.pdf_generator import PDFGenerator
from ..models.search_record import SearchRecord

logger = logging.getLogger(__name__)


class ExportWorker(QThread):
    """Worker thread for export operations."""
    
    # Signals
    export_started = pyqtSignal()
    export_progress = pyqtSignal(int, str)  # Progress percentage and status message
    export_completed = pyqtSignal(str, str)  # Output path and verification hash
    export_error = pyqtSignal(str)  # Error message
    
    def __init__(self, export_config: Dict[str, Any]):
        super().__init__()
        self.export_config = export_config
        self._is_cancelled = False
    
    def run(self):
        """Execute the export operation."""
        try:
            self.export_started.emit()
            
            if self._is_cancelled:
                return
            
            # Get export parameters
            matches = self.export_config['matches']
            output_path = self.export_config['output_path']
            format_type = self.export_config['format']
            search_query = self.export_config.get('search_query', '')
            entity_type = self.export_config.get('entity_type', 'All')
            
            self.export_progress.emit(10, "Initializing export...")
            
            if self._is_cancelled:
                return
            
            # Create PDF generator
            pdf_generator = PDFGenerator()
            
            self.export_progress.emit(30, "Generating report...")
            
            if self._is_cancelled:
                return
            
            # Generate the report
            verification_hash = pdf_generator.generate_search_report(
                search_query=search_query,
                entity_type=entity_type,
                matches=matches,
                output_path=output_path
            )
            
            self.export_progress.emit(100, "Export completed")
            
            if not self._is_cancelled:
                self.export_completed.emit(output_path, verification_hash)
            
        except Exception as e:
            if not self._is_cancelled:
                self.export_error.emit(str(e))
    
    def cancel(self):
        """Cancel the export operation."""
        self._is_cancelled = True


class BatchExportWorker(QThread):
    """Worker thread for batch export operations."""
    
    # Signals
    batch_started = pyqtSignal(int)  # Total number of exports
    export_progress = pyqtSignal(int, str, str)  # Current export index, filename, status
    batch_completed = pyqtSignal(list)  # List of export results
    batch_error = pyqtSignal(str)  # Error message
    
    def __init__(self, batch_config: Dict[str, Any]):
        super().__init__()
        self.batch_config = batch_config
        self._is_cancelled = False
    
    def run(self):
        """Execute the batch export operation."""
        try:
            search_records = self.batch_config['search_records']
            output_directory = self.batch_config['output_directory']
            format_type = self.batch_config['format']
            
            self.batch_started.emit(len(search_records))
            
            pdf_generator = PDFGenerator()
            results = []
            
            for i, search_record in enumerate(search_records):
                if self._is_cancelled:
                    break
                
                # Generate filename
                timestamp = search_record.search_timestamp.strftime('%Y%m%d_%H%M%S')
                query_safe = "".join(c for c in search_record.search_query if c.isalnum() or c in (' ', '-', '_')).rstrip()
                query_safe = query_safe.replace(' ', '_')[:30]
                filename = f"sanctions_report_{query_safe}_{timestamp}.pdf"
                output_path = os.path.join(output_directory, filename)
                
                self.export_progress.emit(i, filename, "Generating...")
                
                try:
                    # Generate the report
                    verification_hash = pdf_generator.generate_report(search_record, output_path)
                    
                    results.append({
                        'search_record_id': search_record.id,
                        'filename': filename,
                        'output_path': output_path,
                        'verification_hash': verification_hash,
                        'status': 'success'
                    })
                    
                    self.export_progress.emit(i, filename, "Completed")
                    
                except Exception as e:
                    results.append({
                        'search_record_id': search_record.id,
                        'filename': filename,
                        'output_path': output_path,
                        'verification_hash': None,
                        'status': 'error',
                        'error': str(e)
                    })
                    
                    self.export_progress.emit(i, filename, f"Error: {str(e)}")
            
            if not self._is_cancelled:
                self.batch_completed.emit(results)
            
        except Exception as e:
            if not self._is_cancelled:
                self.batch_error.emit(str(e))
    
    def cancel(self):
        """Cancel the batch export operation."""
        self._is_cancelled = True


class ExportDialog(QDialog):
    """
    Dialog for exporting search results with format selection, preview, and batch capabilities.
    """
    
    def __init__(self, matches: List[EntityMatch] = None, search_records: List[SearchRecord] = None, 
                 search_query: str = "", entity_type: str = "All", parent=None):
        """
        Initialize the export dialog.
        
        Args:
            matches: List of EntityMatch objects for single export
            search_records: List of SearchRecord objects for batch export
            search_query: Original search query
            entity_type: Entity type searched for
            parent: Parent widget
        """
        super().__init__(parent)
        self.matches = matches or []
        self.search_records = search_records or []
        self.search_query = search_query
        self.entity_type = entity_type
        self.export_worker: Optional[ExportWorker] = None
        self.batch_worker: Optional[BatchExportWorker] = None
        
        self.setup_ui()
        self.setup_connections()
        self.update_preview()
    
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle("Export Search Results")
        self.setModal(True)
        self.resize(800, 600)
        
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        
        # Create tabs for single and batch export
        tabs = QTabWidget()
        layout.addWidget(tabs)
        
        # Single export tab
        if self.matches:
            single_tab = self._create_single_export_tab()
            tabs.addTab(single_tab, f"Export Results ({len(self.matches)} matches)")
        
        # Batch export tab
        if self.search_records:
            batch_tab = self._create_batch_export_tab()
            tabs.addTab(batch_tab, f"Batch Export ({len(self.search_records)} searches)")
        
        # Progress bar (initially hidden)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Status label
        self.status_label = QLabel("Ready to export")
        self.status_label.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(self.status_label)
        
        # Dialog buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_button)
        
        self.export_button = QPushButton("Export")
        self.export_button.setDefault(True)
        self.export_button.clicked.connect(self.start_export)
        button_layout.addWidget(self.export_button)
        
        layout.addLayout(button_layout)
    
    def _create_single_export_tab(self) -> QWidget:
        """Create the single export tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Export settings group
        settings_group = QGroupBox("Export Settings")
        settings_layout = QGridLayout(settings_group)
        
        # Format selection
        settings_layout.addWidget(QLabel("Format:"), 0, 0)
        self.format_combo = QComboBox()
        self.format_combo.addItems(["PDF Report"])  # Future: CSV, JSON, etc.
        settings_layout.addWidget(self.format_combo, 0, 1)
        
        # Output file
        settings_layout.addWidget(QLabel("Output File:"), 1, 0)
        file_layout = QHBoxLayout()
        
        self.output_file_edit = QLineEdit()
        default_filename = f"sanctions_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        self.output_file_edit.setText(default_filename)
        file_layout.addWidget(self.output_file_edit)
        
        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self.browse_output_file)
        file_layout.addWidget(self.browse_button)
        
        settings_layout.addLayout(file_layout, 1, 1)
        
        # Include options
        settings_layout.addWidget(QLabel("Include:"), 2, 0)
        include_layout = QVBoxLayout()
        
        self.include_algorithm_breakdown = QCheckBox("Algorithm breakdown")
        self.include_algorithm_breakdown.setChecked(True)
        include_layout.addWidget(self.include_algorithm_breakdown)
        
        self.include_verification_hash = QCheckBox("Verification hash")
        self.include_verification_hash.setChecked(True)
        include_layout.addWidget(self.include_verification_hash)
        
        self.include_entity_details = QCheckBox("Full entity details")
        self.include_entity_details.setChecked(True)
        include_layout.addWidget(self.include_entity_details)
        
        settings_layout.addLayout(include_layout, 2, 1)
        
        layout.addWidget(settings_group)
        
        # Preview section
        preview_group = QGroupBox("Preview")
        preview_layout = QVBoxLayout(preview_group)
        
        self.preview_text = QTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_text.setMaximumHeight(200)
        preview_layout.addWidget(self.preview_text)
        
        layout.addWidget(preview_group)
        
        return tab
    
    def _create_batch_export_tab(self) -> QWidget:
        """Create the batch export tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Batch settings group
        settings_group = QGroupBox("Batch Export Settings")
        settings_layout = QGridLayout(settings_group)
        
        # Output directory
        settings_layout.addWidget(QLabel("Output Directory:"), 0, 0)
        dir_layout = QHBoxLayout()
        
        self.output_dir_edit = QLineEdit()
        self.output_dir_edit.setText(os.path.expanduser("~/Desktop"))
        dir_layout.addWidget(self.output_dir_edit)
        
        self.browse_dir_button = QPushButton("Browse...")
        self.browse_dir_button.clicked.connect(self.browse_output_directory)
        dir_layout.addWidget(self.browse_dir_button)
        
        settings_layout.addLayout(dir_layout, 0, 1)
        
        # Filename pattern
        settings_layout.addWidget(QLabel("Filename Pattern:"), 1, 0)
        self.filename_pattern_edit = QLineEdit()
        self.filename_pattern_edit.setText("sanctions_report_{query}_{timestamp}.pdf")
        self.filename_pattern_edit.setToolTip("Available variables: {query}, {timestamp}, {user_id}")
        settings_layout.addWidget(self.filename_pattern_edit, 1, 1)
        
        layout.addWidget(settings_group)
        
        # Search records table
        records_group = QGroupBox("Search Records to Export")
        records_layout = QVBoxLayout(records_group)
        
        self.records_table = QTableWidget()
        self.records_table.setColumnCount(5)
        self.records_table.setHorizontalHeaderLabels([
            "Select", "Query", "Timestamp", "Results", "User"
        ])
        self.records_table.horizontalHeader().setStretchLastSection(True)
        
        # Populate table
        self.records_table.setRowCount(len(self.search_records))
        for i, record in enumerate(self.search_records):
            # Select checkbox
            select_checkbox = QCheckBox()
            select_checkbox.setChecked(True)
            self.records_table.setCellWidget(i, 0, select_checkbox)
            
            # Query
            self.records_table.setItem(i, 1, QTableWidgetItem(record.search_query))
            
            # Timestamp
            timestamp_str = record.search_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            self.records_table.setItem(i, 2, QTableWidgetItem(timestamp_str))
            
            # Results count
            results_count = len(record.results) if record.results else 0
            self.records_table.setItem(i, 3, QTableWidgetItem(str(results_count)))
            
            # User
            self.records_table.setItem(i, 4, QTableWidgetItem(record.user_id or "Anonymous"))
        
        self.records_table.resizeColumnsToContents()
        records_layout.addWidget(self.records_table)
        
        # Select all/none buttons
        select_layout = QHBoxLayout()
        select_all_button = QPushButton("Select All")
        select_all_button.clicked.connect(self.select_all_records)
        select_layout.addWidget(select_all_button)
        
        select_none_button = QPushButton("Select None")
        select_none_button.clicked.connect(self.select_no_records)
        select_layout.addWidget(select_none_button)
        
        select_layout.addStretch()
        records_layout.addLayout(select_layout)
        
        layout.addWidget(records_group)
        
        return tab
    
    def setup_connections(self):
        """Set up signal-slot connections."""
        # Update preview when settings change
        if hasattr(self, 'format_combo'):
            self.format_combo.currentTextChanged.connect(self.update_preview)
            self.include_algorithm_breakdown.toggled.connect(self.update_preview)
            self.include_verification_hash.toggled.connect(self.update_preview)
            self.include_entity_details.toggled.connect(self.update_preview)
    
    def browse_output_file(self):
        """Browse for output file location."""
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "Save Export File",
            self.output_file_edit.text(),
            "PDF Files (*.pdf);;All Files (*)"
        )
        
        if filename:
            self.output_file_edit.setText(filename)
    
    def browse_output_directory(self):
        """Browse for output directory."""
        directory = QFileDialog.getExistingDirectory(
            self,
            "Select Output Directory",
            self.output_dir_edit.text()
        )
        
        if directory:
            self.output_dir_edit.setText(directory)
    
    def select_all_records(self):
        """Select all records for batch export."""
        for i in range(self.records_table.rowCount()):
            checkbox = self.records_table.cellWidget(i, 0)
            if checkbox:
                checkbox.setChecked(True)
    
    def select_no_records(self):
        """Deselect all records for batch export."""
        for i in range(self.records_table.rowCount()):
            checkbox = self.records_table.cellWidget(i, 0)
            if checkbox:
                checkbox.setChecked(False)
    
    def update_preview(self):
        """Update the export preview."""
        if not hasattr(self, 'preview_text') or not self.matches:
            return
        
        preview_lines = []
        preview_lines.append("EXPORT PREVIEW")
        preview_lines.append("=" * 50)
        preview_lines.append("")
        
        # Export settings
        if hasattr(self, 'format_combo'):
            preview_lines.append(f"Format: {self.format_combo.currentText()}")
            preview_lines.append(f"Search Query: {self.search_query}")
            preview_lines.append(f"Entity Type: {self.entity_type}")
            preview_lines.append(f"Total Matches: {len(self.matches)}")
            preview_lines.append("")
        
        # Sample matches
        preview_lines.append("SAMPLE MATCHES:")
        preview_lines.append("-" * 20)
        
        for i, match in enumerate(self.matches[:5]):  # Show first 5 matches
            preview_lines.append(f"{i+1}. {match.entity.name}")
            preview_lines.append(f"   Type: {match.entity.entity_type}")
            preview_lines.append(f"   Confidence: {match.overall_confidence:.1%}")
            preview_lines.append(f"   Source: {match.entity.source}")
            preview_lines.append("")
        
        if len(self.matches) > 5:
            preview_lines.append(f"... and {len(self.matches) - 5} more matches")
        
        # Include options
        if hasattr(self, 'include_algorithm_breakdown'):
            preview_lines.append("")
            preview_lines.append("INCLUDED SECTIONS:")
            if self.include_algorithm_breakdown.isChecked():
                preview_lines.append("✓ Algorithm breakdown")
            if self.include_verification_hash.isChecked():
                preview_lines.append("✓ Verification hash")
            if self.include_entity_details.isChecked():
                preview_lines.append("✓ Full entity details")
        
        self.preview_text.setPlainText("\n".join(preview_lines))
    
    def start_export(self):
        """Start the export process."""
        # Determine if this is single or batch export
        current_tab = self.findChild(QTabWidget).currentIndex()
        
        if current_tab == 0 and self.matches:
            self.start_single_export()
        elif (current_tab == 1 if self.matches else 0) and self.search_records:
            self.start_batch_export()
    
    def start_single_export(self):
        """Start single export process."""
        if not self.matches:
            QMessageBox.warning(self, "Export Error", "No matches to export.")
            return
        
        output_path = self.output_file_edit.text().strip()
        if not output_path:
            QMessageBox.warning(self, "Export Error", "Please specify an output file.")
            return
        
        # Prepare export configuration
        export_config = {
            'matches': self.matches,
            'output_path': output_path,
            'format': self.format_combo.currentText(),
            'search_query': self.search_query,
            'entity_type': self.entity_type,
            'include_algorithm_breakdown': self.include_algorithm_breakdown.isChecked(),
            'include_verification_hash': self.include_verification_hash.isChecked(),
            'include_entity_details': self.include_entity_details.isChecked()
        }
        
        # Start export worker
        self.export_worker = ExportWorker(export_config)
        self.export_worker.export_started.connect(self.on_export_started)
        self.export_worker.export_progress.connect(self.on_export_progress)
        self.export_worker.export_completed.connect(self.on_export_completed)
        self.export_worker.export_error.connect(self.on_export_error)
        self.export_worker.start()
    
    def start_batch_export(self):
        """Start batch export process."""
        # Get selected records
        selected_records = []
        for i in range(self.records_table.rowCount()):
            checkbox = self.records_table.cellWidget(i, 0)
            if checkbox and checkbox.isChecked():
                selected_records.append(self.search_records[i])
        
        if not selected_records:
            QMessageBox.warning(self, "Export Error", "Please select at least one search record to export.")
            return
        
        output_directory = self.output_dir_edit.text().strip()
        if not output_directory or not os.path.exists(output_directory):
            QMessageBox.warning(self, "Export Error", "Please specify a valid output directory.")
            return
        
        # Prepare batch export configuration
        batch_config = {
            'search_records': selected_records,
            'output_directory': output_directory,
            'format': 'PDF Report',
            'filename_pattern': self.filename_pattern_edit.text()
        }
        
        # Start batch export worker
        self.batch_worker = BatchExportWorker(batch_config)
        self.batch_worker.batch_started.connect(self.on_batch_started)
        self.batch_worker.export_progress.connect(self.on_batch_progress)
        self.batch_worker.batch_completed.connect(self.on_batch_completed)
        self.batch_worker.batch_error.connect(self.on_batch_error)
        self.batch_worker.start()
    
    @pyqtSlot()
    def on_export_started(self):
        """Handle export started signal."""
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.export_button.setEnabled(False)
        self.export_button.setText("Exporting...")
        self.status_label.setText("Export in progress...")
    
    @pyqtSlot(int, str)
    def on_export_progress(self, progress: int, message: str):
        """Handle export progress updates."""
        self.progress_bar.setValue(progress)
        self.status_label.setText(message)
    
    @pyqtSlot(str, str)
    def on_export_completed(self, output_path: str, verification_hash: str):
        """Handle export completion."""
        self.progress_bar.setVisible(False)
        self.export_button.setEnabled(True)
        self.export_button.setText("Export")
        self.status_label.setText("Export completed successfully")
        
        # Show success message with verification hash
        QMessageBox.information(
            self,
            "Export Successful",
            f"Search results exported successfully!\n\n"
            f"File: {output_path}\n"
            f"Verification Hash: {verification_hash[:16]}...\n\n"
            f"The verification hash can be used to validate the report's authenticity."
        )
        
        self.accept()
    
    @pyqtSlot(str)
    def on_export_error(self, error_message: str):
        """Handle export errors."""
        self.progress_bar.setVisible(False)
        self.export_button.setEnabled(True)
        self.export_button.setText("Export")
        self.status_label.setText("Export failed")
        
        QMessageBox.critical(
            self,
            "Export Error",
            f"An error occurred during export:\n{error_message}"
        )
    
    @pyqtSlot(int)
    def on_batch_started(self, total_exports: int):
        """Handle batch export started signal."""
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(total_exports)
        self.progress_bar.setValue(0)
        self.export_button.setEnabled(False)
        self.export_button.setText("Batch Exporting...")
        self.status_label.setText(f"Starting batch export of {total_exports} reports...")
    
    @pyqtSlot(int, str, str)
    def on_batch_progress(self, current_index: int, filename: str, status: str):
        """Handle batch export progress updates."""
        self.progress_bar.setValue(current_index + 1)
        self.status_label.setText(f"Exporting {filename}: {status}")
    
    @pyqtSlot(list)
    def on_batch_completed(self, results: List[Dict[str, Any]]):
        """Handle batch export completion."""
        self.progress_bar.setVisible(False)
        self.export_button.setEnabled(True)
        self.export_button.setText("Export")
        
        # Count successful and failed exports
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = len(results) - successful
        
        self.status_label.setText(f"Batch export completed: {successful} successful, {failed} failed")
        
        # Show detailed results
        message = f"Batch export completed!\n\n"
        message += f"Successful exports: {successful}\n"
        message += f"Failed exports: {failed}\n\n"
        
        if failed > 0:
            message += "Failed exports:\n"
            for result in results:
                if result['status'] == 'error':
                    message += f"- {result['filename']}: {result.get('error', 'Unknown error')}\n"
        
        QMessageBox.information(self, "Batch Export Results", message)
        
        self.accept()
    
    @pyqtSlot(str)
    def on_batch_error(self, error_message: str):
        """Handle batch export errors."""
        self.progress_bar.setVisible(False)
        self.export_button.setEnabled(True)
        self.export_button.setText("Export")
        self.status_label.setText("Batch export failed")
        
        QMessageBox.critical(
            self,
            "Batch Export Error",
            f"An error occurred during batch export:\n{error_message}"
        )
    
    def closeEvent(self, event):
        """Handle dialog close event."""
        # Cancel any running workers
        if self.export_worker and self.export_worker.isRunning():
            self.export_worker.cancel()
            self.export_worker.wait(3000)
        
        if self.batch_worker and self.batch_worker.isRunning():
            self.batch_worker.cancel()
            self.batch_worker.wait(3000)
        
        event.accept()