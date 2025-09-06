"""
Search results display widget with table, filtering, sorting, and detail view.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QHeaderView, QLabel, QLineEdit, QComboBox, QPushButton, QSplitter,
    QTextEdit, QGroupBox, QFrame, QProgressBar, QMessageBox, QMenu,
    QAbstractItemView, QCheckBox, QSpinBox
)
from PyQt6.QtCore import Qt, pyqtSignal, QSortFilterProxyModel, QAbstractTableModel, QModelIndex
from PyQt6.QtGui import QAction, QFont, QColor, QPalette

from ..services.search_service import EntityMatch
from ..models import SanctionedEntity

logger = logging.getLogger(__name__)


class SearchResultsTableModel(QAbstractTableModel):
    """Table model for search results."""
    
    # Column definitions
    COLUMNS = [
        ('Name', 'name'),
        ('Type', 'entity_type'),
        ('Confidence', 'overall_confidence'),
        ('Source', 'source'),
        ('Sanctions Type', 'sanctions_type'),
        ('Effective Date', 'effective_date'),
        ('Best Algorithm', 'best_algorithm')
    ]
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.matches: List[EntityMatch] = []
        self.headers = [col[0] for col in self.COLUMNS]
    
    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self.matches)
    
    def columnCount(self, parent=QModelIndex()) -> int:
        return len(self.COLUMNS)
    
    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self.headers[section]
        return None
    
    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or index.row() >= len(self.matches):
            return None
        
        match = self.matches[index.row()]
        column_key = self.COLUMNS[index.column()][1]
        
        if role == Qt.ItemDataRole.DisplayRole:
            return self._get_display_value(match, column_key)
        elif role == Qt.ItemDataRole.BackgroundRole:
            return self._get_background_color(match)
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            if column_key in ['overall_confidence']:
                return Qt.AlignmentFlag.AlignCenter
            return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
        elif role == Qt.ItemDataRole.UserRole:
            return match  # Store the full match object
        
        return None
    
    def _get_display_value(self, match: EntityMatch, column_key: str) -> str:
        """Get the display value for a specific column."""
        if column_key == 'name':
            return match.entity.name
        elif column_key == 'entity_type':
            return match.entity.entity_type.title()
        elif column_key == 'overall_confidence':
            return f"{match.overall_confidence:.1%}"
        elif column_key == 'source':
            return match.entity.source
        elif column_key == 'sanctions_type':
            return match.entity.sanctions_type
        elif column_key == 'effective_date':
            if match.entity.effective_date:
                return match.entity.effective_date.strftime('%Y-%m-%d')
            return 'N/A'
        elif column_key == 'best_algorithm':
            if match.confidence_scores:
                best_alg, best_score = max(match.confidence_scores.items(), key=lambda x: x[1])
                return f"{best_alg.title()} ({best_score:.1%})"
            return 'N/A'
        return ''
    
    def _get_background_color(self, match: EntityMatch) -> QColor:
        """Get background color based on confidence level - using transparent for better readability."""
        # Return transparent color to avoid white backgrounds that make text hard to read
        return QColor(0, 0, 0, 0)  # Transparent
    
    def set_matches(self, matches: List[EntityMatch]):
        """Update the matches data."""
        self.beginResetModel()
        self.matches = matches
        self.endResetModel()
    
    def get_match(self, row: int) -> Optional[EntityMatch]:
        """Get the match at the specified row."""
        if 0 <= row < len(self.matches):
            return self.matches[row]
        return None


class SearchResultsWidget(QWidget):
    """
    Widget for displaying search results with table, filtering, and detail view.
    """
    
    # Signals
    export_requested = pyqtSignal(list)  # Emitted when export is requested
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_matches: List[EntityMatch] = []
        self.filtered_matches: List[EntityMatch] = []
        
        self.setup_ui()
        self.setup_connections()
        
        # Set initial state
        self._set_initial_state()
    
    def setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(10)
        
        # Header with title and summary
        header_layout = QHBoxLayout()
        
        self.title_label = QLabel("Search Results")
        title_font = QFont()
        title_font.setPointSize(12)
        title_font.setBold(True)
        self.title_label.setFont(title_font)
        header_layout.addWidget(self.title_label)
        
        header_layout.addStretch()
        
        self.summary_label = QLabel("No results")
        self.summary_label.setStyleSheet("color: gray; font-style: italic;")
        header_layout.addWidget(self.summary_label)
        
        # Tags display
        self.tags_label = QLabel("")
        self.tags_label.setStyleSheet("color: #666; font-size: 11px; font-style: italic;")
        self.tags_label.setVisible(False)
        header_layout.addWidget(self.tags_label)
        
        layout.addLayout(header_layout)
        
        # Filters section
        filters_group = QGroupBox("Filters")
        filters_layout = QHBoxLayout(filters_group)
        
        # Search filter
        filters_layout.addWidget(QLabel("Search:"))
        self.search_filter = QLineEdit()
        self.search_filter.setPlaceholderText("Filter by name...")
        filters_layout.addWidget(self.search_filter)
        
        # Entity type filter
        filters_layout.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItems(["All", "Individual", "Company"])
        filters_layout.addWidget(self.type_filter)
        
        # Confidence filter
        filters_layout.addWidget(QLabel("Min. Confidence:"))
        self.confidence_filter = QComboBox()
        self.confidence_filter.addItems(["0%", "40%", "50%", "60%", "70%", "80%", "90%"])
        self.confidence_filter.setCurrentText("0%")
        filters_layout.addWidget(self.confidence_filter)
        
        # Source filter
        filters_layout.addWidget(QLabel("Source:"))
        self.source_filter = QComboBox()
        self.source_filter.addItem("All")
        filters_layout.addWidget(self.source_filter)
        
        # Clear filters button
        self.clear_filters_btn = QPushButton("Clear Filters")
        filters_layout.addWidget(self.clear_filters_btn)
        
        filters_layout.addStretch()
        
        layout.addWidget(filters_group)
        
        # Main content area with splitter
        splitter = QSplitter(Qt.Orientation.Vertical)
        layout.addWidget(splitter)
        
        # Results table
        table_widget = QWidget()
        table_layout = QVBoxLayout(table_widget)
        table_layout.setContentsMargins(0, 0, 0, 0)
        
        # Table controls
        table_controls = QHBoxLayout()
        
        self.show_high_confidence_only = QCheckBox("High confidence only (≥80%)")
        table_controls.addWidget(self.show_high_confidence_only)
        
        table_controls.addStretch()
        
        # Results per page
        table_controls.addWidget(QLabel("Show:"))
        self.results_per_page = QSpinBox()
        self.results_per_page.setRange(10, 1000)
        self.results_per_page.setValue(100)
        self.results_per_page.setSuffix(" results")
        table_controls.addWidget(self.results_per_page)
        
        # Export button
        self.export_btn = QPushButton("Export Results")
        self.export_btn.setEnabled(False)
        table_controls.addWidget(self.export_btn)
        
        table_layout.addLayout(table_controls)
        
        # Table
        self.results_table = QTableWidget()
        self.results_model = SearchResultsTableModel()
        
        # Configure table - remove alternating colors for better readability
        self.results_table.setAlternatingRowColors(False)
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.results_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.results_table.setSortingEnabled(True)
        self.results_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.results_table.setVisible(False)  # Initially hidden
        
        table_layout.addWidget(self.results_table)
        
        splitter.addWidget(table_widget)
        
        # Detail view
        detail_widget = QWidget()
        detail_layout = QVBoxLayout(detail_widget)
        detail_layout.setContentsMargins(0, 0, 0, 0)
        
        detail_header = QLabel("Match Details")
        detail_header.setFont(title_font)
        detail_layout.addWidget(detail_header)
        
        self.detail_text = QTextEdit()
        self.detail_text.setReadOnly(True)
        # Remove fixed height to make it expandable
        self.detail_text.setMinimumHeight(100)  # Set minimum height instead
        self.detail_text.setPlaceholderText("Select a result to view detailed matching information...")
        detail_layout.addWidget(self.detail_text)
        
        splitter.addWidget(detail_widget)
        
        # Set splitter proportions (70% table, 30% details) - but allow user to resize
        splitter.setSizes([560, 240])
        splitter.setCollapsible(0, False)  # Don't allow table to be completely collapsed
        splitter.setCollapsible(1, False)  # Don't allow details to be completely collapsed
        
        # No results message (initially visible)
        self.no_results_label = QLabel("No search results to display.")
        self.no_results_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.no_results_label.setStyleSheet("color: gray; font-size: 14px; font-style: italic;")
        self.no_results_label.setVisible(True)
        layout.addWidget(self.no_results_label)
    
    def setup_connections(self):
        """Set up signal-slot connections."""
        # Filter connections
        self.search_filter.textChanged.connect(self.apply_filters)
        self.type_filter.currentTextChanged.connect(self.apply_filters)
        self.confidence_filter.currentTextChanged.connect(self.apply_filters)
        self.source_filter.currentTextChanged.connect(self.apply_filters)
        self.show_high_confidence_only.toggled.connect(self.apply_filters)
        self.results_per_page.valueChanged.connect(self.apply_filters)
        
        # Clear filters
        self.clear_filters_btn.clicked.connect(self.clear_filters)
        
        # Table selection
        self.results_table.itemSelectionChanged.connect(self.on_selection_changed)
        
        # Context menu
        self.results_table.customContextMenuRequested.connect(self.show_context_menu)
        
        # Export
        self.export_btn.clicked.connect(self.request_export)
    
    def _set_initial_state(self):
        """Set the initial state of the widget."""
        self.export_btn.setEnabled(False)
        self.no_results_label.setVisible(True)
        self.results_table.setVisible(False)
        self.summary_label.setText("No results")
        self.tags_label.setVisible(False)
        self.search_tags = []
    
    def set_results(self, matches: List[EntityMatch], search_tags: List[str] = None):
        """
        Set the search results to display.
        
        Args:
            matches: List of EntityMatch objects
            search_tags: Optional list of tags used for this search
        """
        self.current_matches = matches
        self.filtered_matches = matches.copy()
        self.search_tags = search_tags or []
        
        # Update source filter options
        self.update_source_filter()
        
        # Update table
        self.update_table()
        
        # Update summary
        self.update_summary()
        
        # Enable/disable export
        self.export_btn.setEnabled(len(matches) > 0)
        
        # Show/hide no results message
        self.no_results_label.setVisible(len(matches) == 0)
        self.results_table.setVisible(len(matches) > 0)
        
        # Update tags display
        if self.search_tags:
            tags_text = f"🏷️ Tags: {', '.join(self.search_tags)}"
            self.tags_label.setText(tags_text)
            self.tags_label.setVisible(True)
        else:
            self.tags_label.setVisible(False)
        
        logger.info(f"Updated results display with {len(matches)} matches")
    
    def update_source_filter(self):
        """Update the source filter dropdown with available sources."""
        sources = set()
        for match in self.current_matches:
            sources.add(match.entity.source)
        
        # Clear and repopulate
        current_selection = self.source_filter.currentText()
        self.source_filter.clear()
        self.source_filter.addItem("All")
        
        for source in sorted(sources):
            self.source_filter.addItem(source)
        
        # Restore selection if still valid
        index = self.source_filter.findText(current_selection)
        if index >= 0:
            self.source_filter.setCurrentIndex(index)
    
    def apply_filters(self):
        """Apply current filters to the results."""
        if not self.current_matches:
            return
        
        filtered = self.current_matches.copy()
        
        # Search filter
        search_text = self.search_filter.text().strip().lower()
        if search_text:
            filtered = [
                match for match in filtered
                if search_text in match.entity.name.lower() or
                any(search_text in alias.lower() for alias in (match.entity.aliases or []))
            ]
        
        # Entity type filter
        entity_type = self.type_filter.currentText()
        if entity_type != "All":
            filtered = [
                match for match in filtered
                if match.entity.entity_type.lower() == entity_type.lower()
            ]
        
        # Confidence filter
        confidence_text = self.confidence_filter.currentText()
        if confidence_text != "0%":
            min_confidence = float(confidence_text.rstrip('%')) / 100
            filtered = [
                match for match in filtered
                if match.overall_confidence >= min_confidence
            ]
        
        # Source filter
        source = self.source_filter.currentText()
        if source != "All":
            filtered = [
                match for match in filtered
                if match.entity.source == source
            ]
        
        # High confidence only filter
        if self.show_high_confidence_only.isChecked():
            filtered = [
                match for match in filtered
                if match.overall_confidence >= 0.8
            ]
        
        # Limit results
        max_results = self.results_per_page.value()
        if len(filtered) > max_results:
            filtered = filtered[:max_results]
        
        self.filtered_matches = filtered
        self.update_table()
        self.update_summary()
    
    def clear_filters(self):
        """Clear all filters."""
        self.search_filter.clear()
        self.type_filter.setCurrentIndex(0)
        self.confidence_filter.setCurrentIndex(0)
        self.source_filter.setCurrentIndex(0)
        self.show_high_confidence_only.setChecked(False)
        self.results_per_page.setValue(100)
    
    def update_table(self):
        """Update the table with filtered results."""
        # Clear existing table
        self.results_table.setRowCount(0)
        self.results_table.setColumnCount(len(SearchResultsTableModel.COLUMNS))
        
        # Set headers
        headers = [col[0] for col in SearchResultsTableModel.COLUMNS]
        self.results_table.setHorizontalHeaderLabels(headers)
        
        # Populate table
        self.results_table.setRowCount(len(self.filtered_matches))
        
        for row, match in enumerate(self.filtered_matches):
            # Name
            name_item = QTableWidgetItem(match.entity.name)
            name_item.setData(Qt.ItemDataRole.UserRole, match)
            self.results_table.setItem(row, 0, name_item)
            
            # Type
            type_item = QTableWidgetItem(match.entity.entity_type.title())
            self.results_table.setItem(row, 1, type_item)
            
            # Confidence with emoji indicator
            confidence_text = f"{match.overall_confidence:.1%}"
            if match.overall_confidence >= 0.8:
                confidence_text = f"🔴 {confidence_text}"  # High confidence
            elif match.overall_confidence >= 0.6:
                confidence_text = f"🟠 {confidence_text}"  # Medium confidence
            elif match.overall_confidence >= 0.4:
                confidence_text = f"🟡 {confidence_text}"  # Low confidence
            else:
                confidence_text = f"⚪ {confidence_text}"  # Very low confidence
            
            confidence_item = QTableWidgetItem(confidence_text)
            confidence_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.results_table.setItem(row, 2, confidence_item)
            
            # Source
            source_item = QTableWidgetItem(match.entity.source)
            self.results_table.setItem(row, 3, source_item)
            
            # Sanctions Type
            sanctions_item = QTableWidgetItem(match.entity.sanctions_type)
            self.results_table.setItem(row, 4, sanctions_item)
            
            # Effective Date
            if match.entity.effective_date:
                date_str = match.entity.effective_date.strftime('%Y-%m-%d')
            else:
                date_str = 'N/A'
            date_item = QTableWidgetItem(date_str)
            self.results_table.setItem(row, 5, date_item)
            
            # Best Algorithm
            if match.confidence_scores:
                best_alg, best_score = max(match.confidence_scores.items(), key=lambda x: x[1])
                alg_text = f"{best_alg.title()} ({best_score:.1%})"
            else:
                alg_text = 'N/A'
            alg_item = QTableWidgetItem(alg_text)
            self.results_table.setItem(row, 6, alg_item)
            
            # Set row background color based on confidence
            bg_color = self._get_confidence_color(match.overall_confidence)
            for col in range(self.results_table.columnCount()):
                item = self.results_table.item(row, col)
                if item:
                    item.setBackground(bg_color)
        
        # Resize columns to content
        self.results_table.resizeColumnsToContents()
        
        # Make sure name column is not too narrow
        header = self.results_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)  # Name column
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)    # Confidence column
        self.results_table.setColumnWidth(2, 100)
    
    def _get_confidence_color(self, confidence: float) -> QColor:
        """Get background color based on confidence level - using transparent for better readability."""
        # Return transparent color to avoid white backgrounds that make text hard to read
        return QColor(0, 0, 0, 0)  # Transparent
    
    def update_summary(self):
        """Update the summary label."""
        total = len(self.current_matches)
        filtered = len(self.filtered_matches)
        
        if total == 0:
            self.summary_label.setText("No results")
        elif filtered == total:
            self.summary_label.setText(f"{total} result{'s' if total != 1 else ''}")
        else:
            self.summary_label.setText(f"{filtered} of {total} result{'s' if total != 1 else ''}")
    
    def on_selection_changed(self):
        """Handle table selection changes."""
        selected_items = self.results_table.selectedItems()
        if not selected_items:
            self.detail_text.clear()
            return
        
        # Get the match from the first column of the selected row
        row = selected_items[0].row()
        name_item = self.results_table.item(row, 0)
        if not name_item:
            return
        
        match = name_item.data(Qt.ItemDataRole.UserRole)
        if not match:
            return
        
        self.show_match_details(match)
    
    def show_match_details(self, match: EntityMatch):
        """
        Show detailed information about a match.
        
        Args:
            match: EntityMatch object to display
        """
        details = []
        
        # Basic entity information
        details.append(f"<h3>{match.entity.name}</h3>")
        details.append(f"<b>Type:</b> {match.entity.entity_type.title()}")
        details.append(f"<b>Source:</b> {match.entity.source} (v{match.entity.source_version})")
        details.append(f"<b>Sanctions Type:</b> {match.entity.sanctions_type}")
        
        if match.entity.effective_date:
            details.append(f"<b>Effective Date:</b> {match.entity.effective_date.strftime('%Y-%m-%d')}")
        
        # Aliases
        if match.entity.aliases:
            aliases_text = ", ".join(match.entity.aliases[:5])  # Show first 5 aliases
            if len(match.entity.aliases) > 5:
                aliases_text += f" (and {len(match.entity.aliases) - 5} more)"
            details.append(f"<b>Aliases:</b> {aliases_text}")
        
        details.append("")  # Empty line
        
        # Matching information
        details.append(f"<h4>Match Analysis</h4>")
        details.append(f"<b>Overall Confidence:</b> {match.overall_confidence:.1%}")
        details.append(f"<b>Matched Name:</b> {match.matched_name}")
        
        # Algorithm scores
        if match.confidence_scores:
            details.append("<b>Algorithm Scores:</b>")
            for algorithm, score in sorted(match.confidence_scores.items(), key=lambda x: x[1], reverse=True):
                details.append(f"  • {algorithm.title()}: {score:.1%}")
        
        # Match details
        if match.match_details:
            original_query = match.match_details.get('original_query', 'N/A')
            normalized_query = match.match_details.get('normalized_query', 'N/A')
            original_name = match.match_details.get('original_name', 'N/A')
            normalized_name = match.match_details.get('normalized_name', 'N/A')
            
            details.append("")
            details.append("<b>Normalization:</b>")
            details.append(f"  Query: '{original_query}' → '{normalized_query}'")
            details.append(f"  Entity: '{original_name}' → '{normalized_name}'")
        
        # Additional info
        if match.entity.additional_info:
            details.append("")
            details.append("<b>Additional Information:</b>")
            for key, value in match.entity.additional_info.items():
                if isinstance(value, str) and len(value) < 100:
                    details.append(f"  • {key.title()}: {value}")
        
        self.detail_text.setHtml("<br>".join(details))
    
    def show_context_menu(self, position):
        """Show context menu for table."""
        item = self.results_table.itemAt(position)
        if not item:
            return
        
        match = self.results_table.item(item.row(), 0).data(Qt.ItemDataRole.UserRole)
        if not match:
            return
        
        menu = QMenu(self)
        
        # Copy name action
        copy_name_action = QAction("Copy Name", self)
        copy_name_action.triggered.connect(lambda: self.copy_to_clipboard(match.entity.name))
        menu.addAction(copy_name_action)
        
        # Copy all names action
        if match.entity.aliases:
            copy_all_names_action = QAction("Copy All Names", self)
            all_names = [match.entity.name] + match.entity.aliases
            copy_all_names_action.triggered.connect(lambda: self.copy_to_clipboard(", ".join(all_names)))
            menu.addAction(copy_all_names_action)
        
        menu.addSeparator()
        
        # Export single result action
        export_single_action = QAction("Export This Result", self)
        export_single_action.triggered.connect(lambda: self.export_requested.emit([match]))
        menu.addAction(export_single_action)
        
        menu.exec(self.results_table.mapToGlobal(position))
    
    def copy_to_clipboard(self, text: str):
        """Copy text to clipboard."""
        from PyQt6.QtWidgets import QApplication
        clipboard = QApplication.clipboard()
        clipboard.setText(text)
    
    def request_export(self):
        """Request export of current filtered results using export dialog."""
        if not self.filtered_matches:
            QMessageBox.information(self, "Export", "No results to export.")
            return
        
        try:
            from .export_dialog import ExportDialog
            
            # Create and show export dialog
            export_dialog = ExportDialog(
                matches=self.filtered_matches,
                search_query="Filtered Results",  # Could be improved to track original query
                entity_type="All",
                parent=self
            )
            
            export_dialog.exec()
            
        except ImportError as e:
            QMessageBox.warning(
                self, 
                "Export Unavailable", 
                f"Export functionality is not available: {e}"
            )
        except Exception as e:
            QMessageBox.critical(
                self, 
                "Export Error", 
                f"An error occurred while opening export dialog:\n{str(e)}"
            )
    
    def get_selected_match(self) -> Optional[EntityMatch]:
        """Get the currently selected match."""
        selected_items = self.results_table.selectedItems()
        if not selected_items:
            return None
        
        row = selected_items[0].row()
        name_item = self.results_table.item(row, 0)
        if name_item:
            return name_item.data(Qt.ItemDataRole.UserRole)
        return None
    
    def clear_results(self):
        """Clear all results."""
        self.current_matches = []
        self.filtered_matches = []
        self.results_table.setRowCount(0)
        self.detail_text.clear()
        self.summary_label.setText("No results")
        self.export_btn.setEnabled(False)
        self.no_results_label.setVisible(True)
        self.results_table.setVisible(False)