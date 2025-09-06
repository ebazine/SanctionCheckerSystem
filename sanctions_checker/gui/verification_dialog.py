"""
Report verification dialog for validating PDF report authenticity using cryptographic hashes.
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit,
    QPushButton, QTextEdit, QGroupBox, QFileDialog, QMessageBox,
    QFrame, QScrollArea, QTabWidget, QWidget, QTableWidget, QTableWidgetItem,
    QHeaderView, QProgressBar
)
from PyQt6.QtCore import Qt, pyqtSignal, QThread, pyqtSlot
from PyQt6.QtGui import QFont, QColor, QPalette

from ..services.pdf_generator import PDFGenerator, ReportVerifier
from ..models.search_record import SearchRecord
from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)


class VerificationWorker(QThread):
    """Worker thread for hash verification operations."""
    
    # Signals
    verification_started = pyqtSignal()
    verification_completed = pyqtSignal(dict)  # Verification results
    verification_error = pyqtSignal(str)  # Error message
    
    def __init__(self, search_record: SearchRecord, provided_hash: str):
        super().__init__()
        self.search_record = search_record
        self.provided_hash = provided_hash
    
    def run(self):
        """Execute the verification operation."""
        try:
            self.verification_started.emit()
            
            # Perform verification
            verification_result = ReportVerifier.verify_hash(
                self.search_record, 
                self.provided_hash
            )
            
            self.verification_completed.emit(verification_result)
            
        except Exception as e:
            self.verification_error.emit(str(e))


class VerificationDialog(QDialog):
    """
    Dialog for verifying PDF report authenticity using cryptographic hashes.
    """
    
    def __init__(self, parent=None):
        """Initialize the verification dialog."""
        super().__init__(parent)
        self.db_manager: Optional[DatabaseManager] = None
        self.verification_worker: Optional[VerificationWorker] = None
        
        # Initialize database manager
        try:
            self.db_manager = DatabaseManager()
            # Try to initialize the database
            self.db_manager.initialize_database()
        except Exception as e:
            logger.warning(f"Could not initialize database manager: {e}")
            self.db_manager = None
        
        self.setup_ui()
        self.setup_connections()
    
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle("Report Verification")
        self.setModal(True)
        self.resize(700, 500)
        
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # Title
        title_label = QLabel("Report Verification")
        title_font = QFont()
        title_font.setPointSize(16)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)
        
        # Description
        desc_label = QLabel(
            "Verify the authenticity and integrity of sanctions screening reports "
            "using cryptographic hash validation."
        )
        desc_label.setWordWrap(True)
        desc_label.setStyleSheet("color: gray; font-style: italic;")
        desc_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(desc_label)
        
        # Create tabs for different verification methods
        tabs = QTabWidget()
        layout.addWidget(tabs)
        
        # Hash verification tab
        hash_tab = self._create_hash_verification_tab()
        tabs.addTab(hash_tab, "Hash Verification")
        
        # Search record verification tab
        if self.db_manager:
            record_tab = self._create_record_verification_tab()
            tabs.addTab(record_tab, "Search Record Verification")
        
        # Results section
        results_group = QGroupBox("Verification Results")
        results_layout = QVBoxLayout(results_group)
        
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMaximumHeight(200)
        self.results_text.setPlaceholderText("Verification results will appear here...")
        results_layout.addWidget(self.results_text)
        
        layout.addWidget(results_group)
        
        # Progress bar (initially hidden)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Status label
        self.status_label = QLabel("Ready for verification")
        self.status_label.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(self.status_label)
        
        # Dialog buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.close_button = QPushButton("Close")
        self.close_button.clicked.connect(self.accept)
        button_layout.addWidget(self.close_button)
        
        self.verify_button = QPushButton("Verify")
        self.verify_button.setDefault(True)
        self.verify_button.clicked.connect(self.start_verification)
        button_layout.addWidget(self.verify_button)
        
        layout.addLayout(button_layout)
    
    def _create_hash_verification_tab(self) -> QWidget:
        """Create the hash verification tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Hash input group
        hash_group = QGroupBox("Hash Verification")
        hash_layout = QGridLayout(hash_group)
        
        # Search record ID
        hash_layout.addWidget(QLabel("Search Record ID:"), 0, 0)
        self.record_id_edit = QLineEdit()
        self.record_id_edit.setPlaceholderText("Enter the search record ID from the report...")
        hash_layout.addWidget(self.record_id_edit, 0, 1)
        
        # Hash input
        hash_layout.addWidget(QLabel("Verification Hash:"), 1, 0)
        self.hash_edit = QLineEdit()
        self.hash_edit.setPlaceholderText("Enter the verification hash from the report...")
        hash_layout.addWidget(self.hash_edit, 1, 1)
        
        # Load from file button
        load_button = QPushButton("Load from PDF...")
        load_button.setToolTip("Extract hash and record ID from a PDF report (future feature)")
        load_button.setEnabled(False)  # Future feature
        hash_layout.addWidget(load_button, 2, 0, 1, 2)
        
        layout.addWidget(hash_group)
        
        # Instructions
        instructions_group = QGroupBox("Instructions")
        instructions_layout = QVBoxLayout(instructions_group)
        
        instructions_text = QTextEdit()
        instructions_text.setReadOnly(True)
        instructions_text.setMaximumHeight(120)
        instructions_text.setHtml("""
        <b>How to verify a report:</b><br>
        1. Open the PDF report you want to verify<br>
        2. Find the "Report Verification" section at the end<br>
        3. Copy the Search Record ID and Verification Hash<br>
        4. Paste them into the fields above<br>
        5. Click "Verify" to check authenticity<br><br>
        <i>A valid hash confirms the report has not been tampered with.</i>
        """)
        instructions_layout.addWidget(instructions_text)
        
        layout.addWidget(instructions_group)
        
        return tab
    
    def _create_record_verification_tab(self) -> QWidget:
        """Create the search record verification tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Search records table
        records_group = QGroupBox("Available Search Records")
        records_layout = QVBoxLayout(records_group)
        
        self.records_table = QTableWidget()
        self.records_table.setColumnCount(4)
        self.records_table.setHorizontalHeaderLabels([
            "Record ID", "Query", "Timestamp", "Results"
        ])
        self.records_table.horizontalHeader().setStretchLastSection(True)
        self.records_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.records_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        
        records_layout.addWidget(self.records_table)
        
        # Refresh button
        refresh_button = QPushButton("Refresh Records")
        refresh_button.clicked.connect(self.refresh_search_records)
        records_layout.addWidget(refresh_button)
        
        layout.addWidget(records_group)
        
        # Generate hash group
        generate_group = QGroupBox("Generate Expected Hash")
        generate_layout = QVBoxLayout(generate_group)
        
        generate_desc = QLabel(
            "Select a search record above and click 'Generate Hash' to see what "
            "the verification hash should be for that record."
        )
        generate_desc.setWordWrap(True)
        generate_layout.addWidget(generate_desc)
        
        self.generate_hash_button = QPushButton("Generate Expected Hash")
        self.generate_hash_button.clicked.connect(self.generate_expected_hash)
        self.generate_hash_button.setEnabled(False)
        generate_layout.addWidget(self.generate_hash_button)
        
        layout.addWidget(generate_group)
        
        # Load initial records
        self.refresh_search_records()
        
        return tab
    
    def setup_connections(self):
        """Set up signal-slot connections."""
        # Enable/disable verify button based on input
        self.record_id_edit.textChanged.connect(self.update_verify_button_state)
        self.hash_edit.textChanged.connect(self.update_verify_button_state)
        
        # Enable generate hash button when record is selected
        if hasattr(self, 'records_table'):
            self.records_table.itemSelectionChanged.connect(self.update_generate_button_state)
    
    def update_verify_button_state(self):
        """Update the verify button state based on input."""
        has_record_id = bool(self.record_id_edit.text().strip())
        has_hash = bool(self.hash_edit.text().strip())
        self.verify_button.setEnabled(has_record_id and has_hash)
    
    def update_generate_button_state(self):
        """Update the generate hash button state based on selection."""
        if hasattr(self, 'generate_hash_button'):
            has_selection = bool(self.records_table.selectedItems())
            self.generate_hash_button.setEnabled(has_selection)
    
    def refresh_search_records(self):
        """Refresh the search records table."""
        if not self.db_manager:
            logger.info("Database manager not available, skipping search records refresh")
            return
        
        try:
            session = self.db_manager.get_session()
            
            # Get recent search records
            from ..models.search_record import SearchRecord
            records = session.query(SearchRecord).order_by(
                SearchRecord.search_timestamp.desc()
            ).limit(50).all()
            
            # Populate table
            self.records_table.setRowCount(len(records))
            for i, record in enumerate(records):
                # Record ID
                self.records_table.setItem(i, 0, QTableWidgetItem(record.id))
                
                # Query
                self.records_table.setItem(i, 1, QTableWidgetItem(record.search_query))
                
                # Timestamp
                timestamp_str = record.search_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                self.records_table.setItem(i, 2, QTableWidgetItem(timestamp_str))
                
                # Results count
                results_count = len(record.results) if record.results else 0
                self.records_table.setItem(i, 3, QTableWidgetItem(str(results_count)))
            
            self.records_table.resizeColumnsToContents()
            session.close()
            
        except Exception as e:
            logger.warning(f"Could not refresh search records: {e}")
            # Don't show error dialog during initialization, just log it
    
    def generate_expected_hash(self):
        """Generate the expected hash for the selected search record."""
        if not self.db_manager:
            return
        
        selected_items = self.records_table.selectedItems()
        if not selected_items:
            return
        
        # Get the record ID from the first column of the selected row
        row = selected_items[0].row()
        record_id = self.records_table.item(row, 0).text()
        
        try:
            session = self.db_manager.get_session()
            
            # Load the search record
            from ..models.search_record import SearchRecord
            search_record = session.query(SearchRecord).filter(
                SearchRecord.id == record_id
            ).first()
            
            if not search_record:
                QMessageBox.warning(self, "Error", "Search record not found.")
                session.close()
                return
            
            # Generate the expected hash
            pdf_generator = PDFGenerator()
            expected_hash = pdf_generator._generate_verification_hash(search_record)
            
            # Display the result
            self.results_text.clear()
            result_html = f"""
            <h3 style="color: blue;">Expected Hash Generated</h3>
            <p><b>Search Record ID:</b> {record_id}</p>
            <p><b>Search Query:</b> {search_record.search_query}</p>
            <p><b>Timestamp:</b> {search_record.search_timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><b>Expected Hash:</b></p>
            <p style="font-family: monospace; background-color: #f0f0f0; padding: 10px; word-break: break-all;">
            {expected_hash}
            </p>
            <p><i>This is the hash that should appear in any PDF report generated for this search record.</i></p>
            """
            self.results_text.setHtml(result_html)
            
            # Auto-fill the hash verification fields
            self.record_id_edit.setText(record_id)
            self.hash_edit.setText(expected_hash)
            
            self.status_label.setText("Expected hash generated successfully")
            session.close()
            
        except Exception as e:
            logger.error(f"Error generating expected hash: {e}")
            QMessageBox.critical(
                self, 
                "Error", 
                f"Could not generate expected hash: {e}"
            )
    
    def start_verification(self):
        """Start the verification process."""
        record_id = self.record_id_edit.text().strip()
        provided_hash = self.hash_edit.text().strip()
        
        if not record_id or not provided_hash:
            QMessageBox.warning(
                self, 
                "Input Error", 
                "Please provide both the search record ID and verification hash."
            )
            return
        
        if not self.db_manager:
            QMessageBox.warning(
                self, 
                "Database Error", 
                "Database connection is not available."
            )
            return
        
        try:
            session = self.db_manager.get_session()
            
            # Load the search record
            from ..models.search_record import SearchRecord
            search_record = session.query(SearchRecord).filter(
                SearchRecord.id == record_id
            ).first()
            
            if not search_record:
                QMessageBox.warning(
                    self, 
                    "Record Not Found", 
                    f"Search record with ID '{record_id}' was not found in the database."
                )
                session.close()
                return
            
            # Start verification worker
            self.verification_worker = VerificationWorker(search_record, provided_hash)
            self.verification_worker.verification_started.connect(self.on_verification_started)
            self.verification_worker.verification_completed.connect(self.on_verification_completed)
            self.verification_worker.verification_error.connect(self.on_verification_error)
            self.verification_worker.start()
            
            session.close()
            
        except Exception as e:
            logger.error(f"Error starting verification: {e}")
            QMessageBox.critical(
                self, 
                "Verification Error", 
                f"Could not start verification: {e}"
            )
    
    @pyqtSlot()
    def on_verification_started(self):
        """Handle verification started signal."""
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        self.verify_button.setEnabled(False)
        self.verify_button.setText("Verifying...")
        self.status_label.setText("Verification in progress...")
    
    @pyqtSlot(dict)
    def on_verification_completed(self, verification_result: Dict[str, Any]):
        """Handle verification completion."""
        self.progress_bar.setVisible(False)
        self.verify_button.setEnabled(True)
        self.verify_button.setText("Verify")
        
        is_valid = verification_result['is_valid']
        
        if is_valid:
            self.status_label.setText("✓ Verification successful - Report is authentic")
            self.status_label.setStyleSheet("color: green; font-weight: bold;")
            
            result_html = f"""
            <h3 style="color: green;">✓ VERIFICATION SUCCESSFUL</h3>
            <p><b>Status:</b> <span style="color: green;">VALID</span></p>
            <p><b>Message:</b> {verification_result['message']}</p>
            <p><b>Search Record ID:</b> {verification_result['search_record_id']}</p>
            <p><b>Verification Time:</b> {verification_result['verification_timestamp']}</p>
            <p><b>Expected Hash:</b></p>
            <p style="font-family: monospace; background-color: #e8f5e8; padding: 5px; word-break: break-all;">
            {verification_result['expected_hash']}
            </p>
            <p><b>Provided Hash:</b></p>
            <p style="font-family: monospace; background-color: #e8f5e8; padding: 5px; word-break: break-all;">
            {verification_result['provided_hash']}
            </p>
            <p style="color: green; font-weight: bold;">
            The report is authentic and has not been tampered with.
            </p>
            """
        else:
            self.status_label.setText("✗ Verification failed - Report may be tampered")
            self.status_label.setStyleSheet("color: red; font-weight: bold;")
            
            result_html = f"""
            <h3 style="color: red;">✗ VERIFICATION FAILED</h3>
            <p><b>Status:</b> <span style="color: red;">INVALID</span></p>
            <p><b>Message:</b> {verification_result['message']}</p>
            <p><b>Search Record ID:</b> {verification_result['search_record_id']}</p>
            <p><b>Verification Time:</b> {verification_result['verification_timestamp']}</p>
            <p><b>Expected Hash:</b></p>
            <p style="font-family: monospace; background-color: #f5e8e8; padding: 5px; word-break: break-all;">
            {verification_result['expected_hash']}
            </p>
            <p><b>Provided Hash:</b></p>
            <p style="font-family: monospace; background-color: #f5e8e8; padding: 5px; word-break: break-all;">
            {verification_result['provided_hash']}
            </p>
            <p style="color: red; font-weight: bold;">
            WARNING: The hashes do not match. The report may have been tampered with or corrupted.
            </p>
            """
        
        self.results_text.setHtml(result_html)
        
        # Show message box for important results
        if is_valid:
            QMessageBox.information(
                self,
                "Verification Successful",
                "The report is authentic and has not been tampered with."
            )
        else:
            QMessageBox.warning(
                self,
                "Verification Failed",
                "WARNING: The report verification failed. The report may have been "
                "tampered with or corrupted. Please verify the hash was copied correctly."
            )
    
    @pyqtSlot(str)
    def on_verification_error(self, error_message: str):
        """Handle verification errors."""
        self.progress_bar.setVisible(False)
        self.verify_button.setEnabled(True)
        self.verify_button.setText("Verify")
        self.status_label.setText("Verification failed due to error")
        self.status_label.setStyleSheet("color: red;")
        
        QMessageBox.critical(
            self,
            "Verification Error",
            f"An error occurred during verification:\n{error_message}"
        )
    
    def closeEvent(self, event):
        """Handle dialog close event."""
        # Cancel any running verification
        if self.verification_worker and self.verification_worker.isRunning():
            self.verification_worker.wait(3000)
        
        event.accept()