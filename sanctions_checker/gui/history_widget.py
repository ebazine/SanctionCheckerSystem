"""
Search History Widget for displaying and managing search history.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit,
    QPushButton, QComboBox, QTableWidget, QTableWidgetItem, QHeaderView,
    QGroupBox, QDateEdit, QSpinBox, QCheckBox, QMessageBox, QMenu,
    QProgressBar, QSplitter, QTextEdit, QTabWidget, QFrame
)
from PyQt6.QtCore import Qt, QDate, QTimer, pyqtSignal, QThread, pyqtSlot
from PyQt6.QtGui import QAction, QFont, QContextMenuEvent

from sanctions_checker.models.search_record import SearchRecord
from sanctions_checker.services.data_service import DataService
from sanctions_checker.database.manager import DatabaseManager

logger = logging.getLogger(__name__)


class HistoryLoadWorker(QThread):
    """Worker thread for loading search history without blocking the UI."""
    
    # Signals
    history_loaded = pyqtSignal(list)  # List of SearchRecord objects
    load_error = pyqtSignal(str)  # Error message
    
    def __init__(self, data_service: DataService, user_id: str = None, 
                 limit: int = 100, offset: int = 0, 
                 start_date: datetime = None, end_date: datetime = None):
        super().__init__()
        self.data_service = data_service
        self.user_id = user_id
        self.limit = limit
        self.offset = offset
        self.start_date = start_date
        self.end_date = end_date
    
    def run(self):
        """Load search history."""
        try:
            history = self.data_service.get_search_history(
                user_id=self.user_id,
                limit=self.limit,
                offset=self.offset,
                start_date=self.start_date,
                end_date=self.end_date
            )
            self.history_loaded.emit(history)
        except Exception as e:
            self.load_error.emit(str(e))


class HistoryCleanupWorker(QThread):
    """Worker thread for cleaning up old search records."""
    
    # Signals
    cleanup_completed = pyqtSignal(int)  # Number of records deleted
    cleanup_error = pyqtSignal(str)  # Error message
    
    def __init__(self, data_service: DataService, retention_days: int):
        super().__init__()
        self.data_service = data_service
        self.retention_days = retention_days
    
    def run(self):
        """Perform cleanup operation."""
        try:
            deleted_count = self.data_service.cleanup_old_search_records(self.retention_days)
            self.cleanup_completed.emit(deleted_count)
        except Exception as e:
            self.cleanup_error.emit(str(e))


class SearchHistoryWidget(QWidget):
    """Widget for displaying and managing search history."""
    
    # Signals
    search_replay_requested = pyqtSignal(str, str, str)  # query, entity_type, search_record_id
    search_comparison_requested = pyqtSignal(list)  # List of search record IDs
    
    def __init__(self, data_service: DataService = None):
        """Initialize the search history widget.
        
        Args:
            data_service: DataService instance for database operations
        """
        super().__init__()
        
        # Initialize data service
        if data_service:
            self.data_service = data_service
        else:
            try:
                db_manager = DatabaseManager()
                self.data_service = DataService(db_manager)
            except Exception as e:
                logger.error(f"Failed to initialize data service: {e}")
                self.data_service = None
        
        self.search_records: List[SearchRecord] = []
        self.filtered_records: List[SearchRecord] = []
        self.selected_records: List[str] = []  # List of selected record IDs
        self.load_worker: Optional[HistoryLoadWorker] = None
        self.cleanup_worker: Optional[HistoryCleanupWorker] = None
        
        # Setup UI
        self.setup_ui()
        self.setup_connections()
        
        # Load initial data (only if data service is available)
        if self.data_service:
            # Use a timer to delay the initial refresh to avoid issues during testing
            QTimer.singleShot(100, self.refresh_history)
    
    def setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # Title
        title = QLabel("Search History")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title.setFont(title_font)
        layout.addWidget(title)
        
        # Create main splitter
        splitter = QSplitter(Qt.Orientation.Vertical)
        layout.addWidget(splitter)
        
        # Top section - Filters and controls
        top_section = self._create_filter_section()
        splitter.addWidget(top_section)
        
        # Middle section - History table
        middle_section = self._create_history_table_section()
        splitter.addWidget(middle_section)
        
        # Bottom section - Details and actions
        bottom_section = self._create_details_section()
        splitter.addWidget(bottom_section)
        
        # Set splitter proportions
        splitter.setSizes([150, 400, 200])
        
        # Progress bar (initially hidden)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
    
    def _create_filter_section(self) -> QWidget:
        """Create the filter and control section."""
        section = QFrame()
        section.setFrameStyle(QFrame.Shape.StyledPanel)
        layout = QVBoxLayout(section)
        
        # Filter controls
        filter_group = QGroupBox("Filters")
        filter_layout = QGridLayout(filter_group)
        
        # Date range filter
        filter_layout.addWidget(QLabel("From Date:"), 0, 0)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setDate(QDate.currentDate().addDays(-30))
        self.start_date_edit.setCalendarPopup(True)
        filter_layout.addWidget(self.start_date_edit, 0, 1)
        
        filter_layout.addWidget(QLabel("To Date:"), 0, 2)
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setDate(QDate.currentDate())
        self.end_date_edit.setCalendarPopup(True)
        filter_layout.addWidget(self.end_date_edit, 0, 3)
        
        # Search query filter
        filter_layout.addWidget(QLabel("Search Query:"), 1, 0)
        self.query_filter = QLineEdit()
        self.query_filter.setPlaceholderText("Filter by search query...")
        filter_layout.addWidget(self.query_filter, 1, 1, 1, 2)
        
        # Results filter
        filter_layout.addWidget(QLabel("Results:"), 1, 3)
        self.results_filter = QComboBox()
        self.results_filter.addItems(["All", "With Matches", "No Matches", "High Confidence", "Medium Confidence", "Low Confidence"])
        filter_layout.addWidget(self.results_filter, 1, 4)
        
        # Tag filter
        filter_layout.addWidget(QLabel("Tag:"), 2, 0)
        self.tag_filter = QComboBox()
        self.tag_filter.addItem("All Tags")
        self.refresh_tag_filter()
        filter_layout.addWidget(self.tag_filter, 2, 1)
        
        # Limit and sorting
        filter_layout.addWidget(QLabel("Limit:"), 2, 2)
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(10, 1000)
        self.limit_spin.setValue(100)
        filter_layout.addWidget(self.limit_spin, 2, 3)
        
        filter_layout.addWidget(QLabel("Sort By:"), 2, 4)
        self.sort_combo = QComboBox()
        self.sort_combo.addItems(["Date (Newest)", "Date (Oldest)", "Query (A-Z)", "Query (Z-A)", "Results Count"])
        filter_layout.addWidget(self.sort_combo, 2, 5)
        
        layout.addWidget(filter_group)
        
        # Action buttons
        button_layout = QHBoxLayout()
        
        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.clicked.connect(self.refresh_history)
        button_layout.addWidget(self.refresh_button)
        
        self.clear_filters_button = QPushButton("Clear Filters")
        self.clear_filters_button.clicked.connect(self.clear_filters)
        button_layout.addWidget(self.clear_filters_button)
        
        button_layout.addStretch()
        
        self.cleanup_button = QPushButton("Cleanup Old Records")
        self.cleanup_button.clicked.connect(self.show_cleanup_dialog)
        button_layout.addWidget(self.cleanup_button)
        
        layout.addLayout(button_layout)
        
        return section
    
    def _create_history_table_section(self) -> QWidget:
        """Create the history table section."""
        section = QFrame()
        section.setFrameStyle(QFrame.Shape.StyledPanel)
        layout = QVBoxLayout(section)
        
        # Table header
        header_layout = QHBoxLayout()
        header_label = QLabel("Search Records")
        header_font = QFont()
        header_font.setPointSize(12)
        header_font.setBold(True)
        header_label.setFont(header_font)
        header_layout.addWidget(header_label)
        
        header_layout.addStretch()
        
        self.record_count_label = QLabel("0 records")
        header_layout.addWidget(self.record_count_label)
        
        layout.addLayout(header_layout)
        
        # History table
        self.history_table = QTableWidget()
        self.history_table.setColumnCount(8)
        self.history_table.setHorizontalHeaderLabels([
            "Date/Time", "Query", "Tags", "Entity Type", "Results", "High Conf.", "Med Conf.", "Low Conf."
        ])
        
        # Configure table
        header = self.history_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Date/Time
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)  # Query
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)  # Tags
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)  # Entity Type
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)  # Results
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)  # High Conf.
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)  # Med Conf.
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)  # Low Conf.
        
        self.history_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.history_table.setSelectionMode(QTableWidget.SelectionMode.MultiSelection)
        self.history_table.setAlternatingRowColors(True)
        self.history_table.setSortingEnabled(True)
        
        layout.addWidget(self.history_table)
        
        return section
    
    def _create_details_section(self) -> QWidget:
        """Create the details and actions section."""
        section = QFrame()
        section.setFrameStyle(QFrame.Shape.StyledPanel)
        layout = QVBoxLayout(section)
        
        # Create tabs for different views
        tabs = QTabWidget()
        
        # Details tab
        details_tab = QWidget()
        details_layout = QVBoxLayout(details_tab)
        
        self.details_text = QTextEdit()
        self.details_text.setReadOnly(True)
        self.details_text.setPlaceholderText("Select a search record to view details...")
        details_layout.addWidget(self.details_text)
        
        tabs.addTab(details_tab, "Details")
        
        # Actions tab
        actions_tab = QWidget()
        actions_layout = QVBoxLayout(actions_tab)
        
        # Replay section
        replay_group = QGroupBox("Replay Search")
        replay_layout = QVBoxLayout(replay_group)
        
        self.replay_button = QPushButton("Replay Selected Search")
        self.replay_button.setEnabled(False)
        self.replay_button.clicked.connect(self.replay_selected_search)
        replay_layout.addWidget(self.replay_button)
        
        replay_info = QLabel("Select a single search record to replay it with current data.")
        replay_info.setWordWrap(True)
        replay_info.setStyleSheet("color: gray; font-size: 11px;")
        replay_layout.addWidget(replay_info)
        
        actions_layout.addWidget(replay_group)
        
        # Comparison section
        compare_group = QGroupBox("Batch Search")
        compare_layout = QVBoxLayout(compare_group)
        
        self.compare_button = QPushButton("Batch Search")
        self.compare_button.setEnabled(False)
        self.compare_button.clicked.connect(self.compare_selected_searches)
        compare_layout.addWidget(self.compare_button)
        
        compare_info = QLabel("Select search records to run batch search operations.")
        compare_info.setWordWrap(True)
        compare_info.setStyleSheet("color: gray; font-size: 11px;")
        compare_layout.addWidget(compare_info)
        
        actions_layout.addWidget(compare_group)
        
        # Export section
        export_group = QGroupBox("Export History")
        export_layout = QVBoxLayout(export_group)
        
        self.export_button = QPushButton("Export Selected Records")
        self.export_button.setEnabled(False)
        self.export_button.clicked.connect(self.export_selected_records)
        export_layout.addWidget(self.export_button)
        
        export_info = QLabel("Export selected search records to PDF or CSV format.")
        export_info.setWordWrap(True)
        export_info.setStyleSheet("color: gray; font-size: 11px;")
        export_layout.addWidget(export_info)
        
        actions_layout.addWidget(export_group)
        
        actions_layout.addStretch()
        
        tabs.addTab(actions_tab, "Actions")
        
        layout.addWidget(tabs)
        
        return section
    
    def setup_connections(self):
        """Set up signal-slot connections."""
        # Filter connections
        self.start_date_edit.dateChanged.connect(self.apply_filters)
        self.end_date_edit.dateChanged.connect(self.apply_filters)
        self.query_filter.textChanged.connect(self.apply_filters)
        self.results_filter.currentTextChanged.connect(self.apply_filters)
        self.tag_filter.currentTextChanged.connect(self.apply_filters)
        self.sort_combo.currentTextChanged.connect(self.apply_filters)
        
        # Table connections
        self.history_table.itemSelectionChanged.connect(self.on_selection_changed)
        self.history_table.itemDoubleClicked.connect(self.on_item_double_clicked)
    
    def refresh_history(self):
        """Refresh the search history from the database."""
        if not self.data_service:
            QMessageBox.warning(self, "Database Error", "Database service is not available.")
            return
        
        # Show progress
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        self.refresh_button.setEnabled(False)
        
        # Get filter parameters
        start_qdate = self.start_date_edit.date()
        end_qdate = self.end_date_edit.date()
        limit = self.limit_spin.value()
        
        # Convert QDate to datetime
        start_date = datetime(start_qdate.year(), start_qdate.month(), start_qdate.day())
        end_date = datetime(end_qdate.year(), end_qdate.month(), end_qdate.day(), 23, 59, 59)
        
        start_datetime = start_date
        end_datetime = end_date
        
        # Start worker thread
        self.load_worker = HistoryLoadWorker(
            data_service=self.data_service,
            user_id=None,  # Load all users for now
            limit=limit,
            offset=0,
            start_date=start_datetime,
            end_date=end_datetime
        )
        self.load_worker.history_loaded.connect(self.on_history_loaded)
        self.load_worker.load_error.connect(self.on_load_error)
        self.load_worker.start()
    
    @pyqtSlot(list)
    def on_history_loaded(self, history: List[SearchRecord]):
        """Handle successful history loading."""
        self.search_records = history
        self.apply_filters()
        
        # Hide progress
        self.progress_bar.setVisible(False)
        self.refresh_button.setEnabled(True)
        
        logger.info(f"Loaded {len(history)} search records")
        
        # Refresh tag filter options
        self.refresh_tag_filter()
    
    @pyqtSlot(str)
    def on_load_error(self, error_message: str):
        """Handle history loading error."""
        self.progress_bar.setVisible(False)
        self.refresh_button.setEnabled(True)
        
        QMessageBox.critical(self, "Load Error", f"Failed to load search history:\n{error_message}")
        logger.error(f"Failed to load search history: {error_message}")
    
    def apply_filters(self):
        """Apply current filters to the search records."""
        if not self.search_records:
            self.filtered_records = []
            self.update_table()
            return
        
        # Get filter values
        query_filter = self.query_filter.text().lower().strip()
        results_filter = self.results_filter.currentText()
        tag_filter = self.tag_filter.currentText()
        sort_option = self.sort_combo.currentText()
        
        # Apply filters
        filtered = []
        for record in self.search_records:
            # Query filter
            if query_filter and query_filter not in record.search_query.lower():
                continue
            
            # Tag filter
            if tag_filter != "All Tags":
                record_tags = record.tags or []
                if tag_filter not in record_tags:
                    continue
            
            # Results filter
            if results_filter != "All":
                summary = record.get_results_summary()
                total_results = summary['total']
                
                if results_filter == "With Matches" and total_results == 0:
                    continue
                elif results_filter == "No Matches" and total_results > 0:
                    continue
                elif results_filter == "High Confidence" and summary['high_confidence'] == 0:
                    continue
                elif results_filter == "Medium Confidence" and summary['medium_confidence'] == 0:
                    continue
                elif results_filter == "Low Confidence" and summary['low_confidence'] == 0:
                    continue
            
            filtered.append(record)
        
        # Apply sorting
        if sort_option == "Date (Newest)":
            filtered.sort(key=lambda r: r.search_timestamp, reverse=True)
        elif sort_option == "Date (Oldest)":
            filtered.sort(key=lambda r: r.search_timestamp)
        elif sort_option == "Query (A-Z)":
            filtered.sort(key=lambda r: r.search_query.lower())
        elif sort_option == "Query (Z-A)":
            filtered.sort(key=lambda r: r.search_query.lower(), reverse=True)
        elif sort_option == "Results Count":
            filtered.sort(key=lambda r: r.get_results_summary()['total'], reverse=True)
        
        self.filtered_records = filtered
        self.update_table()
    
    def update_table(self):
        """Update the history table with filtered records."""
        self.history_table.setRowCount(len(self.filtered_records))
        
        for row, record in enumerate(self.filtered_records):
            # Date/Time
            timestamp_str = record.search_timestamp.strftime("%Y-%m-%d %H:%M:%S") if record.search_timestamp else "N/A"
            self.history_table.setItem(row, 0, QTableWidgetItem(timestamp_str))
            
            # Query
            query_item = QTableWidgetItem(record.search_query)
            query_item.setData(Qt.ItemDataRole.UserRole, record.id)  # Store record ID
            self.history_table.setItem(row, 1, query_item)
            
            # Tags
            tags_text = ", ".join(record.tags) if record.tags else ""
            self.history_table.setItem(row, 2, QTableWidgetItem(tags_text))
            
            # Entity Type (extract from search parameters)
            entity_type = record.search_parameters.get('entity_type', 'All') if record.search_parameters else 'All'
            self.history_table.setItem(row, 3, QTableWidgetItem(entity_type))
            
            # Results summary
            summary = record.get_results_summary()
            self.history_table.setItem(row, 4, QTableWidgetItem(str(summary['total'])))
            self.history_table.setItem(row, 5, QTableWidgetItem(str(summary['high_confidence'])))
            self.history_table.setItem(row, 6, QTableWidgetItem(str(summary['medium_confidence'])))
            self.history_table.setItem(row, 7, QTableWidgetItem(str(summary['low_confidence'])))
        
        # Update record count
        self.record_count_label.setText(f"{len(self.filtered_records)} records")
    
    def clear_filters(self):
        """Clear all filters and reset to defaults."""
        self.start_date_edit.setDate(QDate.currentDate().addDays(-30))
        self.end_date_edit.setDate(QDate.currentDate())
        self.query_filter.clear()
        self.results_filter.setCurrentIndex(0)
        self.tag_filter.setCurrentIndex(0)
        self.sort_combo.setCurrentIndex(0)
        self.apply_filters()
    
    def refresh_tag_filter(self):
        """Refresh the tag filter dropdown with available tags from search records."""
        # Get all unique tags from search records
        all_tags = set()
        for record in self.search_records:
            if record.tags:
                all_tags.update(record.tags)
        
        # Update tag filter dropdown
        current_selection = self.tag_filter.currentText()
        self.tag_filter.clear()
        self.tag_filter.addItem("All Tags")
        
        for tag in sorted(all_tags):
            self.tag_filter.addItem(tag)
        
        # Restore selection if still valid
        index = self.tag_filter.findText(current_selection)
        if index >= 0:
            self.tag_filter.setCurrentIndex(index)
    
    def on_selection_changed(self):
        """Handle table selection changes."""
        selected_rows = set()
        for item in self.history_table.selectedItems():
            selected_rows.add(item.row())
        
        # Get selected record IDs
        self.selected_records = []
        for row in selected_rows:
            query_item = self.history_table.item(row, 1)
            if query_item:
                record_id = query_item.data(Qt.ItemDataRole.UserRole)
                if record_id:
                    self.selected_records.append(record_id)
        
        # Update button states
        self.replay_button.setEnabled(len(self.selected_records) == 1)
        self.compare_button.setEnabled(2 <= len(self.selected_records) <= 5)
        self.export_button.setEnabled(len(self.selected_records) > 0)
        
        # Update details view
        self.update_details_view()
    
    def update_details_view(self):
        """Update the details view with selected record information."""
        if len(self.selected_records) == 1:
            # Show details for single selected record
            record_id = self.selected_records[0]
            record = next((r for r in self.filtered_records if r.id == record_id), None)
            
            if record:
                details = self.format_record_details(record)
                self.details_text.setHtml(details)
            else:
                self.details_text.setPlainText("Record not found.")
        elif len(self.selected_records) > 1:
            # Show summary for multiple selected records
            summary = f"<h3>Selected Records Summary</h3>"
            summary += f"<p><strong>Number of records:</strong> {len(self.selected_records)}</p>"
            
            total_results = 0
            for record_id in self.selected_records:
                record = next((r for r in self.filtered_records if r.id == record_id), None)
                if record:
                    total_results += record.get_results_summary()['total']
            
            summary += f"<p><strong>Total results across all records:</strong> {total_results}</p>"
            self.details_text.setHtml(summary)
        else:
            self.details_text.setPlainText("Select a search record to view details...")
    
    def format_record_details(self, record: SearchRecord) -> str:
        """Format a search record for display in the details view."""
        html = f"<h3>Search Record Details</h3>"
        html += f"<p><strong>ID:</strong> {record.id}</p>"
        html += f"<p><strong>Query:</strong> {record.search_query}</p>"
        html += f"<p><strong>Timestamp:</strong> {record.search_timestamp.strftime('%Y-%m-%d %H:%M:%S') if record.search_timestamp else 'N/A'}</p>"
        html += f"<p><strong>User ID:</strong> {record.user_id or 'N/A'}</p>"
        
        # Tags
        if record.tags:
            tags_str = ", ".join(record.tags)
            html += f"<p><strong>Tags:</strong> {tags_str}</p>"
        else:
            html += f"<p><strong>Tags:</strong> None</p>"
        
        # Search parameters
        if record.search_parameters:
            html += f"<p><strong>Search Parameters:</strong></p><ul>"
            for key, value in record.search_parameters.items():
                html += f"<li>{key}: {value}</li>"
            html += "</ul>"
        
        # Results summary
        summary = record.get_results_summary()
        html += f"<p><strong>Results Summary:</strong></p><ul>"
        html += f"<li>Total Results: {summary['total']}</li>"
        html += f"<li>High Confidence: {summary['high_confidence']}</li>"
        html += f"<li>Medium Confidence: {summary['medium_confidence']}</li>"
        html += f"<li>Low Confidence: {summary['low_confidence']}</li>"
        html += "</ul>"
        
        # Sanctions list versions
        if record.sanctions_list_versions:
            html += f"<p><strong>Sanctions List Versions:</strong></p><ul>"
            for source, version in record.sanctions_list_versions.items():
                html += f"<li>{source}: {version}</li>"
            html += "</ul>"
        
        # Verification hash
        if record.verification_hash:
            html += f"<p><strong>Verification Hash:</strong> <code>{record.verification_hash}</code></p>"
        
        return html
    
    def on_item_double_clicked(self, item: QTableWidgetItem):
        """Handle double-click on table item."""
        if item.column() == 1:  # Query column
            record_id = item.data(Qt.ItemDataRole.UserRole)
            if record_id:
                self.replay_search_by_id(record_id)
    
    def replay_selected_search(self):
        """Replay the selected search."""
        if len(self.selected_records) == 1:
            self.replay_search_by_id(self.selected_records[0])
    
    def replay_search_by_id(self, record_id: str):
        """Replay a search by record ID."""
        record = next((r for r in self.filtered_records if r.id == record_id), None)
        if not record:
            QMessageBox.warning(self, "Replay Error", "Selected record not found.")
            return
        
        # Extract search parameters
        query = record.search_query
        entity_type = record.search_parameters.get('entity_type', 'All') if record.search_parameters else 'All'
        
        # Emit signal to request replay
        self.search_replay_requested.emit(query, entity_type, record_id)
    
    def compare_selected_searches(self):
        """Compare the selected searches."""
        if 2 <= len(self.selected_records) <= 5:
            self.search_comparison_requested.emit(self.selected_records)
        else:
            QMessageBox.information(self, "Batch Search", "Please select 2-5 search records to compare.")
    
    def export_selected_records(self):
        """Export the selected search records."""
        if not self.selected_records:
            QMessageBox.information(self, "Export", "No records selected for export.")
            return
        
        # This would be implemented to export records to PDF or CSV
        QMessageBox.information(self, "Export", f"Export functionality for {len(self.selected_records)} records will be implemented.")
    
    def show_cleanup_dialog(self):
        """Show dialog for cleaning up old search records."""
        from PyQt6.QtWidgets import QDialog, QDialogButtonBox, QFormLayout
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Cleanup Old Records")
        dialog.setModal(True)
        
        layout = QFormLayout(dialog)
        
        # Retention days input
        retention_spin = QSpinBox()
        retention_spin.setRange(1, 365)
        retention_spin.setValue(90)
        retention_spin.setSuffix(" days")
        layout.addRow("Delete records older than:", retention_spin)
        
        # Info label
        info_label = QLabel("This will permanently delete search records and their results older than the specified number of days.")
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: orange; font-size: 11px;")
        layout.addRow(info_label)
        
        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addRow(buttons)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.cleanup_old_records(retention_spin.value())
    
    def cleanup_old_records(self, retention_days: int):
        """Clean up old search records."""
        if not self.data_service:
            QMessageBox.warning(self, "Database Error", "Database service is not available.")
            return
        
        # Confirm action
        reply = QMessageBox.question(
            self, 
            "Confirm Cleanup", 
            f"Are you sure you want to delete all search records older than {retention_days} days?\n\nThis action cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply != QMessageBox.StandardButton.Yes:
            return
        
        # Show progress
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        self.cleanup_button.setEnabled(False)
        
        # Start cleanup worker
        self.cleanup_worker = HistoryCleanupWorker(self.data_service, retention_days)
        self.cleanup_worker.cleanup_completed.connect(self.on_cleanup_completed)
        self.cleanup_worker.cleanup_error.connect(self.on_cleanup_error)
        self.cleanup_worker.start()
    
    @pyqtSlot(int)
    def on_cleanup_completed(self, deleted_count: int):
        """Handle successful cleanup completion."""
        self.progress_bar.setVisible(False)
        self.cleanup_button.setEnabled(True)
        
        QMessageBox.information(
            self, 
            "Cleanup Completed", 
            f"Successfully deleted {deleted_count} old search records."
        )
        
        # Refresh the history to reflect changes
        self.refresh_history()
    
    @pyqtSlot(str)
    def on_cleanup_error(self, error_message: str):
        """Handle cleanup error."""
        self.progress_bar.setVisible(False)
        self.cleanup_button.setEnabled(True)
        
        QMessageBox.critical(self, "Cleanup Error", f"Failed to cleanup old records:\n{error_message}")
    
    def contextMenuEvent(self, event: QContextMenuEvent):
        """Handle right-click context menu."""
        if self.history_table.itemAt(self.history_table.mapFromGlobal(event.globalPos())):
            menu = QMenu(self)
            
            if len(self.selected_records) == 1:
                replay_action = QAction("Replay Search", self)
                replay_action.triggered.connect(self.replay_selected_search)
                menu.addAction(replay_action)
            
            if 2 <= len(self.selected_records) <= 5:
                compare_action = QAction("Batch Search", self)
                compare_action.triggered.connect(self.compare_selected_searches)
                menu.addAction(compare_action)
            
            if len(self.selected_records) > 0:
                menu.addSeparator()
                export_action = QAction("Export Selected", self)
                export_action.triggered.connect(self.export_selected_records)
                menu.addAction(export_action)
            
            menu.exec(event.globalPos())