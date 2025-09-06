"""
Batch Search Dialog for running multiple searches simultaneously.
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, 
    QListWidget, QListWidgetItem, QProgressBar, QTextEdit,
    QGroupBox, QCheckBox, QComboBox, QSpinBox, QTabWidget,
    QWidget, QTableWidget, QTableWidgetItem, QHeaderView,
    QMessageBox, QSplitter, QFrame
)
from PyQt6.QtCore import Qt, pyqtSlot
from PyQt6.QtGui import QFont

from ..services.batch_search_service import BatchSearchService
from ..services.search_service import SearchService
from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)


class BatchSearchDialog(QDialog):
    """Dialog for performing batch searches on historical data."""
    
    def __init__(self, search_service: SearchService, db_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.search_service = search_service
        self.db_manager = db_manager
        self.batch_service = BatchSearchService(search_service, db_manager)
        self.selected_searches = []
        self.batch_results = []
        
        self.setup_ui()
        self.connect_signals()
        self.load_data()
    
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle("Batch Search - Rerun Historical Searches")
        self.setModal(True)
        self.resize(1000, 700)
        
        layout = QVBoxLayout(self)
        
        # Create tab widget
        tab_widget = QTabWidget()
        
        # Tab 1: Select searches
        select_tab = self.create_select_tab()
        tab_widget.addTab(select_tab, "Select Searches")
        
        # Tab 2: Run batch
        run_tab = self.create_run_tab()
        tab_widget.addTab(run_tab, "Run Batch")
        
        # Tab 3: Results
        results_tab = self.create_results_tab()
        tab_widget.addTab(results_tab, "Results")
        
        layout.addWidget(tab_widget)
        
        # Bottom buttons
        button_layout = QHBoxLayout()
        
        self.start_button = QPushButton("Start Batch Search")
        self.start_button.clicked.connect(self.start_batch_search)
        self.start_button.setEnabled(False)
        
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.cancel_batch_search)
        
        self.close_button = QPushButton("Close")
        self.close_button.clicked.connect(self.accept)
        
        button_layout.addStretch()
        button_layout.addWidget(self.start_button)
        button_layout.addWidget(self.cancel_button)
        button_layout.addWidget(self.close_button)
        
        layout.addLayout(button_layout)
    
    def create_select_tab(self) -> QWidget:
        """Create the search selection tab."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Filter section
        filter_group = QGroupBox("Filter Historical Searches")
        filter_layout = QVBoxLayout(filter_group)
        
        # Tag filter
        tag_layout = QHBoxLayout()
        tag_layout.addWidget(QLabel("Filter by Tags:"))
        
        self.tag_combo = QComboBox()
        self.tag_combo.addItem("All Searches")
        self.tag_combo.currentTextChanged.connect(self.filter_searches)
        tag_layout.addWidget(self.tag_combo)
        
        # Limit
        tag_layout.addWidget(QLabel("Limit:"))
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(1, 1000)
        self.limit_spin.setValue(100)
        self.limit_spin.valueChanged.connect(self.filter_searches)
        tag_layout.addWidget(self.limit_spin)
        
        filter_layout.addLayout(tag_layout)
        layout.addWidget(filter_group)
        
        # Search list
        list_group = QGroupBox("Historical Searches")
        list_layout = QVBoxLayout(list_group)
        
        # Select all/none buttons
        select_layout = QHBoxLayout()
        self.select_all_button = QPushButton("Select All")
        self.select_all_button.clicked.connect(self.select_all_searches)
        
        self.select_none_button = QPushButton("Select None")
        self.select_none_button.clicked.connect(self.select_no_searches)
        
        select_layout.addWidget(self.select_all_button)
        select_layout.addWidget(self.select_none_button)
        select_layout.addStretch()
        
        list_layout.addLayout(select_layout)
        
        # Search list widget
        self.search_list = QListWidget()
        self.search_list.itemChanged.connect(self.update_selection)
        list_layout.addWidget(self.search_list)
        
        # Selection info
        self.selection_label = QLabel("0 searches selected")
        list_layout.addWidget(self.selection_label)
        
        layout.addWidget(list_group)
        
        return widget
    
    def create_run_tab(self) -> QWidget:
        """Create the batch run tab."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Progress section
        progress_group = QGroupBox("Batch Search Progress")
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        self.progress_label = QLabel("Ready to start batch search")
        
        progress_layout.addWidget(self.progress_label)
        progress_layout.addWidget(self.progress_bar)
        
        layout.addWidget(progress_group)
        
        # Log section
        log_group = QGroupBox("Search Log")
        log_layout = QVBoxLayout(log_group)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(200)
        
        log_layout.addWidget(self.log_text)
        layout.addWidget(log_group)
        
        # Current results preview
        preview_group = QGroupBox("Current Results Preview")
        preview_layout = QVBoxLayout(preview_group)
        
        self.preview_table = QTableWidget()
        self.preview_table.setColumnCount(4)
        self.preview_table.setHorizontalHeaderLabels(["Query", "Matches", "Status", "Tags"])
        self.preview_table.horizontalHeader().setStretchLastSection(True)
        
        preview_layout.addWidget(self.preview_table)
        layout.addWidget(preview_group)
        
        return widget
    
    def create_results_tab(self) -> QWidget:
        """Create the results tab."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Summary section
        summary_group = QGroupBox("Batch Search Summary")
        summary_layout = QVBoxLayout(summary_group)
        
        self.summary_label = QLabel("No batch search completed yet")
        summary_layout.addWidget(self.summary_label)
        
        layout.addWidget(summary_group)
        
        # Results table
        results_group = QGroupBox("Detailed Results")
        results_layout = QVBoxLayout(results_group)
        
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(6)
        self.results_table.setHorizontalHeaderLabels([
            "Query", "Matches Found", "Status", "Tags", "Entity Type", "Timestamp"
        ])
        self.results_table.horizontalHeader().setStretchLastSection(True)
        
        results_layout.addWidget(self.results_table)
        layout.addWidget(results_group)
        
        # Export button
        export_layout = QHBoxLayout()
        self.export_button = QPushButton("Export Results to CSV")
        self.export_button.clicked.connect(self.export_results)
        self.export_button.setEnabled(False)
        
        export_layout.addStretch()
        export_layout.addWidget(self.export_button)
        
        layout.addLayout(export_layout)
        
        return widget
    
    def connect_signals(self):
        """Connect batch search service signals."""
        self.batch_service.batch_started.connect(self.on_batch_started)
        self.batch_service.search_progress.connect(self.on_search_progress)
        self.batch_service.search_completed.connect(self.on_search_completed)
        self.batch_service.search_error.connect(self.on_search_error)
        self.batch_service.batch_completed.connect(self.on_batch_completed)
    
    def load_data(self):
        """Load historical searches and tags."""
        # Load available tags
        tags = self.batch_service.get_available_tags()
        for tag in tags:
            self.tag_combo.addItem(tag)
        
        # Load all searches initially
        self.filter_searches()
    
    def filter_searches(self):
        """Filter searches based on selected criteria."""
        selected_tag = self.tag_combo.currentText()
        limit = self.limit_spin.value()
        
        if selected_tag == "All Searches":
            searches = self.batch_service.get_all_historical_searches(limit)
        else:
            searches = self.batch_service.get_historical_searches_by_tags([selected_tag])
            searches = searches[:limit]  # Apply limit
        
        self.populate_search_list(searches)
    
    def populate_search_list(self, searches: List[Dict]):
        """Populate the search list widget."""
        self.search_list.clear()
        
        for search in searches:
            query = search['query']
            tags = ', '.join(search['tags']) if search['tags'] else 'No tags'
            entity_type = search.get('entity_type', 'All')
            timestamp = search.get('timestamp', 'Unknown')
            
            # Create display text
            display_text = f"{query} | Tags: {tags} | Type: {entity_type} | {timestamp}"
            
            item = QListWidgetItem(display_text)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Unchecked)
            item.setData(Qt.ItemDataRole.UserRole, search)
            
            self.search_list.addItem(item)
        
        self.update_selection()
    
    def select_all_searches(self):
        """Select all searches in the list."""
        for i in range(self.search_list.count()):
            item = self.search_list.item(i)
            item.setCheckState(Qt.CheckState.Checked)
    
    def select_no_searches(self):
        """Deselect all searches in the list."""
        for i in range(self.search_list.count()):
            item = self.search_list.item(i)
            item.setCheckState(Qt.CheckState.Unchecked)
    
    def update_selection(self):
        """Update the selection count and enable/disable start button."""
        selected_count = 0
        self.selected_searches = []
        
        for i in range(self.search_list.count()):
            item = self.search_list.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                selected_count += 1
                search_data = item.data(Qt.ItemDataRole.UserRole)
                self.selected_searches.append(search_data)
        
        self.selection_label.setText(f"{selected_count} searches selected")
        self.start_button.setEnabled(selected_count > 0)
    
    def start_batch_search(self):
        """Start the batch search operation."""
        if not self.selected_searches:
            QMessageBox.warning(self, "No Searches Selected", 
                              "Please select at least one search to run.")
            return
        
        self.start_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        
        # Clear previous results
        self.batch_results = []
        self.log_text.clear()
        self.preview_table.setRowCount(0)
        
        # Start the batch search
        self.batch_service.start_batch_search(self.selected_searches)
    
    def cancel_batch_search(self):
        """Cancel the batch search operation."""
        self.batch_service.cancel_batch_search()
        self.start_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        self.log_text.append("Batch search cancelled by user.")
    
    @pyqtSlot(int)
    def on_batch_started(self, total_searches: int):
        """Handle batch search started."""
        self.progress_bar.setMaximum(total_searches)
        self.progress_bar.setValue(0)
        self.progress_label.setText(f"Starting batch search of {total_searches} queries...")
        self.log_text.append(f"Batch search started: {total_searches} searches queued")
    
    @pyqtSlot(int, str, str)
    def on_search_progress(self, index: int, query: str, status: str):
        """Handle individual search progress."""
        self.progress_bar.setValue(index + 1)
        self.progress_label.setText(f"Searching: {query}")
        self.log_text.append(f"[{index + 1}] Searching: {query}")
    
    @pyqtSlot(int, str, list, str)
    def on_search_completed(self, index: int, query: str, matches: list, record_id: str):
        """Handle individual search completion."""
        match_count = len(matches)
        self.log_text.append(f"[{index + 1}] Completed: {query} - {match_count} matches found")
        
        # Update preview table
        row = self.preview_table.rowCount()
        self.preview_table.insertRow(row)
        
        search_data = self.selected_searches[index]
        tags = ', '.join(search_data.get('tags', [])) if search_data.get('tags') else 'None'
        
        self.preview_table.setItem(row, 0, QTableWidgetItem(query))
        self.preview_table.setItem(row, 1, QTableWidgetItem(str(match_count)))
        self.preview_table.setItem(row, 2, QTableWidgetItem("Completed"))
        self.preview_table.setItem(row, 3, QTableWidgetItem(tags))
    
    @pyqtSlot(int, str, str)
    def on_search_error(self, index: int, query: str, error: str):
        """Handle individual search error."""
        self.log_text.append(f"[{index + 1}] Error: {query} - {error}")
        
        # Update preview table
        row = self.preview_table.rowCount()
        self.preview_table.insertRow(row)
        
        search_data = self.selected_searches[index]
        tags = ', '.join(search_data.get('tags', [])) if search_data.get('tags') else 'None'
        
        self.preview_table.setItem(row, 0, QTableWidgetItem(query))
        self.preview_table.setItem(row, 1, QTableWidgetItem("0"))
        self.preview_table.setItem(row, 2, QTableWidgetItem("Error"))
        self.preview_table.setItem(row, 3, QTableWidgetItem(tags))
    
    @pyqtSlot(list)
    def on_batch_completed(self, results: list):
        """Handle batch search completion."""
        self.batch_results = results
        
        total_searches = len(results)
        successful_searches = len([r for r in results if r['status'] == 'completed'])
        total_matches = sum(len(r['matches']) for r in results if r['status'] == 'completed')
        
        self.progress_label.setText(f"Batch search completed: {successful_searches}/{total_searches} successful")
        self.log_text.append(f"\nBatch search completed!")
        self.log_text.append(f"Total searches: {total_searches}")
        self.log_text.append(f"Successful: {successful_searches}")
        self.log_text.append(f"Total matches found: {total_matches}")
        
        # Update summary
        self.summary_label.setText(
            f"Completed {successful_searches}/{total_searches} searches successfully. "
            f"Found {total_matches} total matches."
        )
        
        # Populate results table
        self.populate_results_table(results)
        
        # Enable export and reset buttons
        self.export_button.setEnabled(True)
        self.start_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
    
    def populate_results_table(self, results: list):
        """Populate the results table."""
        self.results_table.setRowCount(len(results))
        
        for i, result in enumerate(results):
            query = result['query']
            match_count = len(result['matches']) if result['status'] == 'completed' else 0
            status = result['status'].title()
            tags = ', '.join(result.get('tags', [])) if result.get('tags') else 'None'
            entity_type = result.get('entity_type', 'All')
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.results_table.setItem(i, 0, QTableWidgetItem(query))
            self.results_table.setItem(i, 1, QTableWidgetItem(str(match_count)))
            self.results_table.setItem(i, 2, QTableWidgetItem(status))
            self.results_table.setItem(i, 3, QTableWidgetItem(tags))
            self.results_table.setItem(i, 4, QTableWidgetItem(entity_type))
            self.results_table.setItem(i, 5, QTableWidgetItem(timestamp))
    
    def export_results(self):
        """Export batch search results to CSV."""
        if not self.batch_results:
            QMessageBox.information(self, "No Results", "No batch search results to export.")
            return
        
        from PyQt6.QtWidgets import QFileDialog
        
        filename, _ = QFileDialog.getSaveFileName(
            self, "Export Batch Search Results", 
            f"batch_search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "CSV Files (*.csv)"
        )
        
        if filename:
            try:
                import csv
                
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    
                    # Write header
                    writer.writerow([
                        'Query', 'Matches Found', 'Status', 'Tags', 
                        'Entity Type', 'Error Message'
                    ])
                    
                    # Write data
                    for result in self.batch_results:
                        writer.writerow([
                            result['query'],
                            len(result['matches']) if result['status'] == 'completed' else 0,
                            result['status'].title(),
                            ', '.join(result.get('tags', [])) if result.get('tags') else 'None',
                            result.get('entity_type', 'All'),
                            result.get('error', '')
                        ])
                
                QMessageBox.information(self, "Export Successful", 
                                      f"Results exported to {filename}")
                
            except Exception as e:
                QMessageBox.critical(self, "Export Error", 
                                   f"Failed to export results: {str(e)}")
