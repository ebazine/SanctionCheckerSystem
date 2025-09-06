"""
Custom Sanction Entry Dialog for Sanctions Checker Application

This module provides a comprehensive dialog for creating and editing custom sanction entries
with dynamic forms based on subject type and comprehensive data validation.
"""

import os
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit,
    QTextEdit, QComboBox, QDateEdit, QSpinBox, QGroupBox, QFormLayout,
    QScrollArea, QWidget, QFrame, QMessageBox, QTabWidget, QListWidget,
    QListWidgetItem, QCheckBox, QSplitter, QGridLayout, QSizePolicy
)
from PyQt6.QtCore import Qt, pyqtSignal, QDate
from PyQt6.QtGui import QFont, QIcon

from ..models.base import SubjectType, NameType, RecordStatus
from ..services.custom_sanctions_service import CustomSanctionsService
from ..services.custom_sanctions_validator import ValidationResult

logger = logging.getLogger(__name__)


class CustomSanctionEntryDialog(QDialog):
    """
    Dialog for creating and editing custom sanction entries with dynamic forms
    based on subject type and comprehensive validation.
    """
    
    entity_saved = pyqtSignal(str)  # Signal emitted when entity is saved (entity_id)
    
    def __init__(self, service: CustomSanctionsService, entity_id: str = None, parent=None):
        """
        Initialize the Custom Sanction Entry Dialog.
        
        Args:
            service: CustomSanctionsService instance
            entity_id: Optional entity ID for editing existing entity
            parent: Parent widget
        """
        super().__init__(parent)
        self.service = service
        self.entity_id = entity_id
        self.entity_data = {}
        self.names_data = []
        self.addresses_data = []
        self.identifiers_data = []
        
        self.setup_ui()
        self.connect_signals()
        
        if entity_id:
            self.load_entity_data()
        else:
            self.initialize_new_entity()
    
    def setup_ui(self):
        """Set up the user interface with tabbed layout."""
        self.setWindowTitle("Custom Sanction Entry" if not self.entity_id else "Edit Custom Sanction")
        self.setModal(True)
        self.resize(800, 700)
        
        # Main layout
        main_layout = QVBoxLayout(self)
        
        # Create scroll area for the form
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        
        # Create main content widget
        content_widget = QWidget()
        content_layout = QVBoxLayout(content_widget)
        
        # Create tab widget
        self.tab_widget = QTabWidget()
        
        # Core Information Tab
        self.core_tab = self.create_core_information_tab()
        self.tab_widget.addTab(self.core_tab, "Core Information")
        
        # Names Tab
        self.names_tab = self.create_names_tab()
        self.tab_widget.addTab(self.names_tab, "Names & Aliases")
        
        # Subject-specific tabs (will be added dynamically)
        self.individual_tab = None
        self.entity_tab = None
        
        # Addresses Tab
        self.addresses_tab = self.create_addresses_tab()
        self.tab_widget.addTab(self.addresses_tab, "Addresses")
        
        # Identifiers Tab
        self.identifiers_tab = self.create_identifiers_tab()
        self.tab_widget.addTab(self.identifiers_tab, "Identifiers")
        
        # Sanction Details Tab
        self.sanction_details_tab = self.create_sanction_details_tab()
        self.tab_widget.addTab(self.sanction_details_tab, "Sanction Details")
        
        # Internal Metadata Tab
        self.metadata_tab = self.create_metadata_tab()
        self.tab_widget.addTab(self.metadata_tab, "Internal Metadata")
        
        content_layout.addWidget(self.tab_widget)
        scroll_area.setWidget(content_widget)
        main_layout.addWidget(scroll_area)
        
        # Validation status area
        self.validation_frame = self.create_validation_frame()
        main_layout.addWidget(self.validation_frame)
        
        # Button layout
        button_layout = QHBoxLayout()
        
        self.validate_button = QPushButton("Validate")
        self.validate_button.clicked.connect(self.validate_data)
        
        self.save_button = QPushButton("Save")
        self.save_button.clicked.connect(self.save_entity)
        
        self.save_and_close_button = QPushButton("Save & Close")
        self.save_and_close_button.clicked.connect(self.save_and_close)
        
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addWidget(self.validate_button)
        button_layout.addStretch()
        button_layout.addWidget(self.save_button)
        button_layout.addWidget(self.save_and_close_button)
        button_layout.addWidget(cancel_button)
        
        main_layout.addLayout(button_layout)
    
    def create_core_information_tab(self) -> QWidget:
        """Create the core information tab."""
        tab = QWidget()
        layout = QFormLayout(tab)
        
        # Internal Entry ID (auto-generated)
        self.internal_id_label = QLabel("Auto-generated")
        self.internal_id_label.setStyleSheet("color: gray; font-style: italic;")
        layout.addRow("Internal Entry ID:", self.internal_id_label)
        
        # Subject Type
        self.subject_type_combo = QComboBox()
        for subject_type in SubjectType:
            self.subject_type_combo.addItem(subject_type.value, subject_type)
        self.subject_type_combo.currentTextChanged.connect(self.on_subject_type_changed)
        layout.addRow("Subject Type*:", self.subject_type_combo)
        
        # Record Status
        self.record_status_combo = QComboBox()
        for status in RecordStatus:
            self.record_status_combo.addItem(status.value, status)
        layout.addRow("Record Status:", self.record_status_combo)
        
        # Listing Status
        self.listing_status_combo = QComboBox()
        # Add common listing status values
        listing_statuses = ["Listed", "Delisted", "Under Review", "Proposed"]
        for status in listing_statuses:
            self.listing_status_combo.addItem(status)
        layout.addRow("Listing Status:", self.listing_status_combo)
        
        return tab
    
    def create_names_tab(self) -> QWidget:
        """Create the names and aliases tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Instructions
        instructions = QLabel("Add names and aliases for this entity. At least one Primary name is required.")
        instructions.setWordWrap(True)
        instructions.setStyleSheet("color: #666; margin-bottom: 10px;")
        layout.addWidget(instructions)
        
        # Names list and controls
        names_layout = QHBoxLayout()
        
        # Names list
        self.names_list = QListWidget()
        self.names_list.setMinimumHeight(200)
        names_layout.addWidget(self.names_list, 2)
        
        # Names controls
        names_controls_layout = QVBoxLayout()
        
        self.add_name_button = QPushButton("Add Name")
        self.add_name_button.clicked.connect(self.add_name)
        names_controls_layout.addWidget(self.add_name_button)
        
        self.edit_name_button = QPushButton("Edit Name")
        self.edit_name_button.clicked.connect(self.edit_name)
        self.edit_name_button.setEnabled(False)
        names_controls_layout.addWidget(self.edit_name_button)
        
        self.remove_name_button = QPushButton("Remove Name")
        self.remove_name_button.clicked.connect(self.remove_name)
        self.remove_name_button.setEnabled(False)
        names_controls_layout.addWidget(self.remove_name_button)
        
        names_controls_layout.addStretch()
        names_layout.addLayout(names_controls_layout, 1)
        
        layout.addLayout(names_layout)
        
        # Connect list selection
        self.names_list.itemSelectionChanged.connect(self.on_names_selection_changed)
        
        return tab
    
    def create_addresses_tab(self) -> QWidget:
        """Create the addresses tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Instructions
        instructions = QLabel("Add addresses associated with this entity.")
        instructions.setWordWrap(True)
        instructions.setStyleSheet("color: #666; margin-bottom: 10px;")
        layout.addWidget(instructions)
        
        # Addresses list and controls
        addresses_layout = QHBoxLayout()
        
        # Addresses list
        self.addresses_list = QListWidget()
        self.addresses_list.setMinimumHeight(200)
        addresses_layout.addWidget(self.addresses_list, 2)
        
        # Addresses controls
        addresses_controls_layout = QVBoxLayout()
        
        self.add_address_button = QPushButton("Add Address")
        self.add_address_button.clicked.connect(self.add_address)
        addresses_controls_layout.addWidget(self.add_address_button)
        
        self.edit_address_button = QPushButton("Edit Address")
        self.edit_address_button.clicked.connect(self.edit_address)
        self.edit_address_button.setEnabled(False)
        addresses_controls_layout.addWidget(self.edit_address_button)
        
        self.remove_address_button = QPushButton("Remove Address")
        self.remove_address_button.clicked.connect(self.remove_address)
        self.remove_address_button.setEnabled(False)
        addresses_controls_layout.addWidget(self.remove_address_button)
        
        addresses_controls_layout.addStretch()
        addresses_layout.addLayout(addresses_controls_layout, 1)
        
        layout.addLayout(addresses_layout)
        
        # Connect list selection
        self.addresses_list.itemSelectionChanged.connect(self.on_addresses_selection_changed)
        
        return tab
    
    def create_identifiers_tab(self) -> QWidget:
        """Create the identifiers tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Instructions
        instructions = QLabel("Add identification documents and numbers for this entity.")
        instructions.setWordWrap(True)
        instructions.setStyleSheet("color: #666; margin-bottom: 10px;")
        layout.addWidget(instructions)
        
        # Identifiers list and controls
        identifiers_layout = QHBoxLayout()
        
        # Identifiers list
        self.identifiers_list = QListWidget()
        self.identifiers_list.setMinimumHeight(200)
        identifiers_layout.addWidget(self.identifiers_list, 2)
        
        # Identifiers controls
        identifiers_controls_layout = QVBoxLayout()
        
        self.add_identifier_button = QPushButton("Add Identifier")
        self.add_identifier_button.clicked.connect(self.add_identifier)
        identifiers_controls_layout.addWidget(self.add_identifier_button)
        
        self.edit_identifier_button = QPushButton("Edit Identifier")
        self.edit_identifier_button.clicked.connect(self.edit_identifier)
        self.edit_identifier_button.setEnabled(False)
        identifiers_controls_layout.addWidget(self.edit_identifier_button)
        
        self.remove_identifier_button = QPushButton("Remove Identifier")
        self.remove_identifier_button.clicked.connect(self.remove_identifier)
        self.remove_identifier_button.setEnabled(False)
        identifiers_controls_layout.addWidget(self.remove_identifier_button)
        
        identifiers_controls_layout.addStretch()
        identifiers_layout.addLayout(identifiers_controls_layout, 1)
        
        layout.addLayout(identifiers_layout)
        
        # Connect list selection
        self.identifiers_list.itemSelectionChanged.connect(self.on_identifiers_selection_changed)
        
        return tab
    
    def create_sanction_details_tab(self) -> QWidget:
        """Create the sanction details tab."""
        tab = QWidget()
        layout = QFormLayout(tab)
        
        # Sanctioning Authority with quick fill button
        sanctioning_authority_layout = QHBoxLayout()
        self.sanctioning_authority_edit = QLineEdit()
        self.sanctioning_authority_edit.setPlaceholderText("Enter the authority imposing sanctions (e.g., Internal Compliance, OFAC, EU)")
        
        self.quick_fill_authority_btn = QPushButton("📋")
        self.quick_fill_authority_btn.setToolTip("Quick fill with common values")
        self.quick_fill_authority_btn.setMaximumWidth(30)
        self.quick_fill_authority_btn.clicked.connect(self.show_authority_options)
        
        sanctioning_authority_layout.addWidget(self.sanctioning_authority_edit)
        sanctioning_authority_layout.addWidget(self.quick_fill_authority_btn)
        layout.addRow("Sanctioning Authority*:", sanctioning_authority_layout)
        
        # Program with quick fill button
        program_layout = QHBoxLayout()
        self.program_edit = QLineEdit()
        self.program_edit.setPlaceholderText("Enter the sanctions program name (e.g., Internal Watchlist, PEP List)")
        
        self.quick_fill_program_btn = QPushButton("📋")
        self.quick_fill_program_btn.setToolTip("Quick fill with common values")
        self.quick_fill_program_btn.setMaximumWidth(30)
        self.quick_fill_program_btn.clicked.connect(self.show_program_options)
        
        program_layout.addWidget(self.program_edit)
        program_layout.addWidget(self.quick_fill_program_btn)
        layout.addRow("Program*:", program_layout)
        
        # Legal Basis
        self.legal_basis_edit = QTextEdit()
        self.legal_basis_edit.setMaximumHeight(80)
        self.legal_basis_edit.setPlaceholderText("Legal basis or regulation reference")
        layout.addRow("Legal Basis:", self.legal_basis_edit)
        
        # Listing Date
        self.listing_date_edit = QDateEdit()
        self.listing_date_edit.setDate(QDate.currentDate())
        self.listing_date_edit.setCalendarPopup(True)
        layout.addRow("Listing Date*:", self.listing_date_edit)
        
        # Measures Imposed
        self.measures_imposed_edit = QTextEdit()
        self.measures_imposed_edit.setMaximumHeight(100)
        self.measures_imposed_edit.setPlaceholderText("Description of restrictions or measures")
        layout.addRow("Measures Imposed:", self.measures_imposed_edit)
        
        # Reason for Listing
        self.reason_for_listing_edit = QTextEdit()
        self.reason_for_listing_edit.setMaximumHeight(100)
        self.reason_for_listing_edit.setPlaceholderText("Reason why this entity was added to the list")
        layout.addRow("Reason for Listing:", self.reason_for_listing_edit)
        
        return tab
    
    def create_metadata_tab(self) -> QWidget:
        """Create the internal metadata tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Basic metadata form
        form_layout = QFormLayout()
        
        # Data Source with quick fill button
        data_source_layout = QHBoxLayout()
        self.data_source_edit = QLineEdit()
        self.data_source_edit.setPlaceholderText("Enter the source of this information (e.g., News Report, Intelligence, Manual Research)")
        
        self.quick_fill_data_source_btn = QPushButton("📋")
        self.quick_fill_data_source_btn.setToolTip("Quick fill with common values")
        self.quick_fill_data_source_btn.setMaximumWidth(30)
        self.quick_fill_data_source_btn.clicked.connect(self.show_data_source_options)
        
        data_source_layout.addWidget(self.data_source_edit)
        data_source_layout.addWidget(self.quick_fill_data_source_btn)
        form_layout.addRow("Data Source*:", data_source_layout)
        
        # Created By (read-only for existing entities)
        self.created_by_edit = QLineEdit()
        self.created_by_edit.setPlaceholderText("User who created this entry")
        form_layout.addRow("Created By:", self.created_by_edit)
        
        # Verification fields
        self.verified_by_edit = QLineEdit()
        self.verified_by_edit.setPlaceholderText("User who verified this entry")
        form_layout.addRow("Verified By:", self.verified_by_edit)
        
        self.verified_date_edit = QDateEdit()
        self.verified_date_edit.setCalendarPopup(True)
        self.verified_date_edit.setSpecialValueText("Not verified")
        form_layout.addRow("Verified Date:", self.verified_date_edit)
        
        layout.addLayout(form_layout)
        
        # Notes History Section
        notes_group = QGroupBox("Internal Notes History")
        notes_layout = QVBoxLayout(notes_group)
        
        # Notes history display
        self.notes_history_display = QTextEdit()
        self.notes_history_display.setMaximumHeight(150)
        self.notes_history_display.setReadOnly(True)
        self.notes_history_display.setPlaceholderText("No notes history available")
        notes_layout.addWidget(self.notes_history_display)
        
        # New note entry
        new_note_layout = QHBoxLayout()
        
        self.new_note_edit = QLineEdit()
        self.new_note_edit.setPlaceholderText("Add a new internal note...")
        new_note_layout.addWidget(self.new_note_edit)
        
        self.add_note_button = QPushButton("Add Note")
        self.add_note_button.clicked.connect(self.add_internal_note)
        new_note_layout.addWidget(self.add_note_button)
        
        notes_layout.addLayout(new_note_layout)
        
        layout.addWidget(notes_group)
        
        # Initialize notes history
        self.notes_history = []
        
        return tab
    
    def create_validation_frame(self) -> QFrame:
        """Create the validation status frame."""
        frame = QFrame()
        frame.setFrameStyle(QFrame.Shape.StyledPanel)
        frame.setMaximumHeight(100)
        frame.hide()  # Initially hidden
        
        layout = QVBoxLayout(frame)
        
        self.validation_label = QLabel("Validation Status")
        font = QFont()
        font.setBold(True)
        self.validation_label.setFont(font)
        layout.addWidget(self.validation_label)
        
        self.validation_text = QTextEdit()
        self.validation_text.setMaximumHeight(60)
        self.validation_text.setReadOnly(True)
        layout.addWidget(self.validation_text)
        
        return frame
    
    def connect_signals(self):
        """Connect UI signals."""
        pass  # Additional signal connections can be added here
    
    def on_subject_type_changed(self, subject_type_text: str):
        """Handle subject type change to show/hide relevant tabs."""
        subject_type = SubjectType(subject_type_text)
        
        # Remove existing subject-specific tabs
        if self.individual_tab:
            index = self.tab_widget.indexOf(self.individual_tab)
            if index >= 0:
                self.tab_widget.removeTab(index)
            self.individual_tab = None
        
        if self.entity_tab:
            index = self.tab_widget.indexOf(self.entity_tab)
            if index >= 0:
                self.tab_widget.removeTab(index)
            self.entity_tab = None
        
        # Add appropriate subject-specific tab
        if subject_type == SubjectType.INDIVIDUAL:
            self.individual_tab = self.create_individual_tab()
            # Insert after Names tab (index 1)
            self.tab_widget.insertTab(2, self.individual_tab, "Individual Details")
        elif subject_type == SubjectType.ENTITY:
            self.entity_tab = self.create_entity_tab()
            # Insert after Names tab (index 1)
            self.tab_widget.insertTab(2, self.entity_tab, "Entity Details")
    
    def create_individual_tab(self) -> QWidget:
        """Create the individual-specific details tab."""
        tab = QWidget()
        layout = QFormLayout(tab)
        
        # Birth Date section
        birth_group = QGroupBox("Birth Information")
        birth_layout = QFormLayout(birth_group)
        
        # Birth year
        self.birth_year_spin = QSpinBox()
        self.birth_year_spin.setRange(1900, datetime.now().year)
        self.birth_year_spin.setSpecialValueText("Unknown")
        self.birth_year_spin.setValue(self.birth_year_spin.minimum())
        birth_layout.addRow("Birth Year:", self.birth_year_spin)
        
        # Birth month
        self.birth_month_spin = QSpinBox()
        self.birth_month_spin.setRange(0, 12)
        self.birth_month_spin.setSpecialValueText("Unknown")
        birth_layout.addRow("Birth Month:", self.birth_month_spin)
        
        # Birth day
        self.birth_day_spin = QSpinBox()
        self.birth_day_spin.setRange(0, 31)
        self.birth_day_spin.setSpecialValueText("Unknown")
        birth_layout.addRow("Birth Day:", self.birth_day_spin)
        
        # Full birth date
        self.birth_full_date_edit = QDateEdit()
        self.birth_full_date_edit.setCalendarPopup(True)
        self.birth_full_date_edit.setSpecialValueText("Unknown")
        birth_layout.addRow("Full Birth Date:", self.birth_full_date_edit)
        
        # Birth note
        self.birth_note_edit = QLineEdit()
        self.birth_note_edit.setPlaceholderText("Additional birth information")
        birth_layout.addRow("Birth Note:", self.birth_note_edit)
        
        # Place of birth
        self.place_of_birth_edit = QLineEdit()
        self.place_of_birth_edit.setPlaceholderText("City, Country")
        birth_layout.addRow("Place of Birth:", self.place_of_birth_edit)
        
        layout.addRow(birth_group)
        
        # Nationalities
        nationalities_group = QGroupBox("Nationalities")
        nationalities_layout = QVBoxLayout(nationalities_group)
        
        self.nationalities_edit = QTextEdit()
        self.nationalities_edit.setMaximumHeight(80)
        self.nationalities_edit.setPlaceholderText("Enter nationalities, one per line")
        nationalities_layout.addWidget(self.nationalities_edit)
        
        layout.addRow(nationalities_group)
        
        return tab
    
    def create_entity_tab(self) -> QWidget:
        """Create the entity-specific details tab."""
        tab = QWidget()
        layout = QFormLayout(tab)
        
        # Registration Information
        reg_group = QGroupBox("Registration Information")
        reg_layout = QFormLayout(reg_group)
        
        # Registration number
        self.registration_number_edit = QLineEdit()
        self.registration_number_edit.setPlaceholderText("Company registration number")
        reg_layout.addRow("Registration Number:", self.registration_number_edit)
        
        # Registration authority
        self.registration_authority_edit = QLineEdit()
        self.registration_authority_edit.setPlaceholderText("Registering authority or jurisdiction")
        reg_layout.addRow("Registration Authority:", self.registration_authority_edit)
        
        # Incorporation date
        self.incorporation_date_edit = QDateEdit()
        self.incorporation_date_edit.setCalendarPopup(True)
        self.incorporation_date_edit.setSpecialValueText("Unknown")
        reg_layout.addRow("Incorporation Date:", self.incorporation_date_edit)
        
        # Company type
        self.company_type_edit = QLineEdit()
        self.company_type_edit.setPlaceholderText("e.g., LLC, Corporation, Partnership")
        reg_layout.addRow("Company Type:", self.company_type_edit)
        
        layout.addRow(reg_group)
        
        # Tax Information
        tax_group = QGroupBox("Tax Information")
        tax_layout = QFormLayout(tax_group)
        
        # Tax ID
        self.tax_id_edit = QLineEdit()
        self.tax_id_edit.setPlaceholderText("Tax identification number")
        tax_layout.addRow("Tax ID:", self.tax_id_edit)
        
        layout.addRow(tax_group)
        
        return tab
    
    def initialize_new_entity(self):
        """Initialize form for new entity creation."""
        # Set default values
        self.subject_type_combo.setCurrentText(SubjectType.INDIVIDUAL.value)
        self.record_status_combo.setCurrentText(RecordStatus.ACTIVE.value)
        self.listing_status_combo.setCurrentText("Listed")
        self.listing_date_edit.setDate(QDate.currentDate())
        
        # Generate placeholder internal ID
        self.internal_id_label.setText("Will be auto-generated on save")
        
        # Note: User must add at least one name manually
        # The validation will catch this and provide a helpful error message
    
    def load_entity_data(self):
        """Load existing entity data for editing."""
        try:
            entity = self.service.get_sanction_entity(self.entity_id)
            if not entity:
                QMessageBox.warning(self, "Error", "Entity not found.")
                self.reject()
                return
            
            # Load core information
            self.internal_id_label.setText(entity.internal_entry_id)
            
            # Set subject type and trigger tab creation
            for i in range(self.subject_type_combo.count()):
                if self.subject_type_combo.itemData(i) == entity.subject_type:
                    self.subject_type_combo.setCurrentIndex(i)
                    break
            
            # Set record status
            for i in range(self.record_status_combo.count()):
                if self.record_status_combo.itemData(i) == entity.record_status:
                    self.record_status_combo.setCurrentIndex(i)
                    break
            
            # Set listing status
            if hasattr(entity, 'listing_status') and entity.listing_status:
                index = self.listing_status_combo.findText(entity.listing_status)
                if index >= 0:
                    self.listing_status_combo.setCurrentIndex(index)
            else:
                # If entity doesn't have listing_status, set default
                self.listing_status_combo.setCurrentText("Listed")
            
            # Load sanction details
            self.sanctioning_authority_edit.setText(entity.sanctioning_authority or "")
            self.program_edit.setText(entity.program or "")
            self.legal_basis_edit.setPlainText(entity.legal_basis or "")
            
            if entity.listing_date:
                self.listing_date_edit.setDate(QDate.fromString(entity.listing_date.isoformat(), Qt.DateFormat.ISODate))
            
            self.measures_imposed_edit.setPlainText(entity.measures_imposed or "")
            self.reason_for_listing_edit.setPlainText(entity.reason_for_listing or "")
            
            # Load metadata
            self.data_source_edit.setText(entity.data_source or "")
            self.created_by_edit.setText(entity.created_by or "")
            self.verified_by_edit.setText(entity.verified_by or "")
            
            if entity.verified_date:
                self.verified_date_edit.setDate(QDate.fromString(entity.verified_date.isoformat(), Qt.DateFormat.ISODate))
            
            # Load notes history (if available) or create from internal_notes
            if hasattr(entity, 'notes_history') and entity.notes_history:
                self.load_notes_history(entity.notes_history)
            elif entity.internal_notes:
                # Convert existing internal_notes to notes history format
                timestamp = entity.last_updated.strftime("%Y-%m-%d %H:%M:%S") if entity.last_updated else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.notes_history = [{
                    'timestamp': timestamp,
                    'user': entity.created_by or "Unknown",
                    'note': entity.internal_notes
                }]
                self.refresh_notes_history()
            
            # Load names
            self.names_data = []
            for name in entity.names:
                self.names_data.append({
                    'full_name': name.full_name,
                    'name_type': name.name_type.value
                })
            self.refresh_names_list()
            
            # Load addresses
            self.addresses_data = []
            for address in entity.addresses:
                self.addresses_data.append({
                    'street': address.street or "",
                    'city': address.city or "",
                    'postal_code': address.postal_code or "",
                    'country': address.country or "",
                    'full_address': address.full_address or ""
                })
            self.refresh_addresses_list()
            
            # Load identifiers
            self.identifiers_data = []
            for identifier in entity.identifiers:
                self.identifiers_data.append({
                    'id_type': identifier.id_type,
                    'id_value': identifier.id_value,
                    'issuing_country': identifier.issuing_country or "",
                    'notes': identifier.notes or ""
                })
            self.refresh_identifiers_list()
            
            # Load subject-specific details
            if entity.subject_type == SubjectType.INDIVIDUAL and entity.individual_details:
                self.load_individual_details(entity.individual_details)
            elif entity.subject_type == SubjectType.ENTITY and entity.entity_details:
                self.load_entity_details(entity.entity_details)
                
        except Exception as e:
            logger.error(f"Error loading entity data: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to load entity data: {str(e)}")
            self.reject()
    
    def load_individual_details(self, individual_details):
        """Load individual-specific details."""
        if not self.individual_tab:
            return
            
        if individual_details.birth_year:
            self.birth_year_spin.setValue(individual_details.birth_year)
        if individual_details.birth_month:
            self.birth_month_spin.setValue(individual_details.birth_month)
        if individual_details.birth_day:
            self.birth_day_spin.setValue(individual_details.birth_day)
        if individual_details.birth_full_date:
            self.birth_full_date_edit.setDate(QDate.fromString(individual_details.birth_full_date.isoformat(), Qt.DateFormat.ISODate))
        
        self.birth_note_edit.setText(individual_details.birth_note or "")
        self.place_of_birth_edit.setText(individual_details.place_of_birth or "")
        
        if individual_details.nationalities:
            nationalities_text = "\n".join(individual_details.nationalities)
            self.nationalities_edit.setPlainText(nationalities_text)
    
    def load_entity_details(self, entity_details):
        """Load entity-specific details."""
        if not self.entity_tab:
            return
            
        self.registration_number_edit.setText(entity_details.registration_number or "")
        self.registration_authority_edit.setText(entity_details.registration_authority or "")
        
        if entity_details.incorporation_date:
            self.incorporation_date_edit.setDate(QDate.fromString(entity_details.incorporation_date.isoformat(), Qt.DateFormat.ISODate))
        
        self.company_type_edit.setText(entity_details.company_type or "")
        self.tax_id_edit.setText(entity_details.tax_id or "")   
 
    # ==================== Names Management ====================
    
    def add_name(self):
        """Add a new name entry."""
        dialog = NameEntryDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            name_data = dialog.get_name_data()
            self.names_data.append(name_data)
            self.refresh_names_list()
    
    def edit_name(self):
        """Edit selected name entry."""
        current_row = self.names_list.currentRow()
        if current_row >= 0:
            dialog = NameEntryDialog(self, self.names_data[current_row])
            if dialog.exec() == QDialog.DialogCode.Accepted:
                name_data = dialog.get_name_data()
                self.names_data[current_row] = name_data
                self.refresh_names_list()
    
    def remove_name(self):
        """Remove selected name entry."""
        current_row = self.names_list.currentRow()
        if current_row >= 0:
            reply = QMessageBox.question(
                self, "Remove Name", 
                "Are you sure you want to remove this name?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                del self.names_data[current_row]
                self.refresh_names_list()
    
    def refresh_names_list(self):
        """Refresh the names list display."""
        self.names_list.clear()
        for name_data in self.names_data:
            item_text = f"{name_data['full_name']} ({name_data['name_type']})"
            self.names_list.addItem(item_text)
    
    def on_names_selection_changed(self):
        """Handle names list selection change."""
        has_selection = self.names_list.currentRow() >= 0
        self.edit_name_button.setEnabled(has_selection)
        self.remove_name_button.setEnabled(has_selection)
    
    # ==================== Addresses Management ====================
    
    def add_address(self):
        """Add a new address entry."""
        is_entity = self.subject_type_combo.currentData() == SubjectType.ENTITY
        dialog = AddressEntryDialog(self, is_entity=is_entity)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            address_data = dialog.get_address_data()
            self.addresses_data.append(address_data)
            self.refresh_addresses_list()
    
    def edit_address(self):
        """Edit selected address entry."""
        current_row = self.addresses_list.currentRow()
        if current_row >= 0:
            is_entity = self.subject_type_combo.currentData() == SubjectType.ENTITY
            dialog = AddressEntryDialog(self, self.addresses_data[current_row], is_entity=is_entity)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                address_data = dialog.get_address_data()
                self.addresses_data[current_row] = address_data
                self.refresh_addresses_list()
    
    def remove_address(self):
        """Remove selected address entry."""
        current_row = self.addresses_list.currentRow()
        if current_row >= 0:
            reply = QMessageBox.question(
                self, "Remove Address", 
                "Are you sure you want to remove this address?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                del self.addresses_data[current_row]
                self.refresh_addresses_list()
    
    def refresh_addresses_list(self):
        """Refresh the addresses list display."""
        self.addresses_list.clear()
        
        # Sort addresses to prioritize registered business addresses for entities
        sorted_addresses = self.addresses_data.copy()
        if self.subject_type_combo.currentData() == SubjectType.ENTITY:
            sorted_addresses.sort(key=lambda addr: 0 if addr.get('address_type') == 'Registered Business Address' else 1)
        
        for address_data in sorted_addresses:
            # Create display text from address components
            parts = []
            if address_data.get('street'):
                parts.append(address_data['street'])
            if address_data.get('city'):
                parts.append(address_data['city'])
            if address_data.get('country'):
                parts.append(address_data['country'])
            
            if not parts and address_data.get('full_address'):
                item_text = address_data['full_address'][:50] + "..." if len(address_data['full_address']) > 50 else address_data['full_address']
            else:
                item_text = ", ".join(parts) if parts else "Incomplete address"
            
            # Add address type for entities
            if address_data.get('address_type'):
                item_text = f"[{address_data['address_type']}] {item_text}"
            
            self.addresses_list.addItem(item_text)
    
    def on_addresses_selection_changed(self):
        """Handle addresses list selection change."""
        has_selection = self.addresses_list.currentRow() >= 0
        self.edit_address_button.setEnabled(has_selection)
        self.remove_address_button.setEnabled(has_selection)
    
    # ==================== Identifiers Management ====================
    
    def add_identifier(self):
        """Add a new identifier entry."""
        dialog = IdentifierEntryDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            identifier_data = dialog.get_identifier_data()
            self.identifiers_data.append(identifier_data)
            self.refresh_identifiers_list()
    
    def edit_identifier(self):
        """Edit selected identifier entry."""
        current_row = self.identifiers_list.currentRow()
        if current_row >= 0:
            dialog = IdentifierEntryDialog(self, self.identifiers_data[current_row])
            if dialog.exec() == QDialog.DialogCode.Accepted:
                identifier_data = dialog.get_identifier_data()
                self.identifiers_data[current_row] = identifier_data
                self.refresh_identifiers_list()
    
    def remove_identifier(self):
        """Remove selected identifier entry."""
        current_row = self.identifiers_list.currentRow()
        if current_row >= 0:
            reply = QMessageBox.question(
                self, "Remove Identifier", 
                "Are you sure you want to remove this identifier?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                del self.identifiers_data[current_row]
                self.refresh_identifiers_list()
    
    def refresh_identifiers_list(self):
        """Refresh the identifiers list display."""
        self.identifiers_list.clear()
        for identifier_data in self.identifiers_data:
            item_text = f"{identifier_data['id_type']}: {identifier_data['id_value']}"
            if identifier_data.get('issuing_country'):
                item_text += f" ({identifier_data['issuing_country']})"
            self.identifiers_list.addItem(item_text)
    
    def on_identifiers_selection_changed(self):
        """Handle identifiers list selection change."""
        has_selection = self.identifiers_list.currentRow() >= 0
        self.edit_identifier_button.setEnabled(has_selection)
        self.remove_identifier_button.setEnabled(has_selection)
    
    # ==================== Notes History Management ====================
    
    def add_internal_note(self):
        """Add a new internal note to the history."""
        note_text = self.new_note_edit.text().strip()
        if not note_text:
            QMessageBox.warning(self, "Warning", "Please enter a note before adding.")
            return
        
        # Create note entry with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user = "Current User"  # In a real application, this would come from the authentication system
        
        note_entry = {
            'timestamp': timestamp,
            'user': user,
            'note': note_text
        }
        
        self.notes_history.append(note_entry)
        self.refresh_notes_history()
        self.new_note_edit.clear()
    
    def refresh_notes_history(self):
        """Refresh the notes history display."""
        if not self.notes_history:
            self.notes_history_display.setPlainText("No notes history available")
            return
        
        # Sort notes by timestamp (newest first)
        sorted_notes = sorted(self.notes_history, key=lambda x: x['timestamp'], reverse=True)
        
        history_text = ""
        for note in sorted_notes:
            history_text += f"[{note['timestamp']}] {note['user']}:\n"
            history_text += f"  {note['note']}\n\n"
        
        self.notes_history_display.setPlainText(history_text.strip())
    
    def load_notes_history(self, notes_history_data):
        """Load existing notes history."""
        if notes_history_data:
            self.notes_history = notes_history_data
            self.refresh_notes_history()
    
    # ==================== Data Collection and Validation ====================
    
    def collect_entity_data(self) -> Dict[str, Any]:
        """Collect all form data into a dictionary."""
        entity_data = {}
        
        # Core information
        # For new entities, generate a unique internal entry ID
        if self.entity_id:
            # For existing entities, get the ID from the label
            entity_data['internal_entry_id'] = self.internal_id_label.text()
        else:
            # For new entities, generate a unique ID
            import uuid
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            unique_suffix = str(uuid.uuid4())[:8]
            entity_data['internal_entry_id'] = f'CUSTOM_{timestamp}_{unique_suffix}'
        
        # Core information with error handling and defaults
        try:
            entity_data['subject_type'] = self.subject_type_combo.currentData().value
        except (AttributeError, RuntimeError):
            entity_data['subject_type'] = 'Individual'
        
        try:
            entity_data['record_status'] = self.record_status_combo.currentData().value
        except (AttributeError, RuntimeError):
            entity_data['record_status'] = 'Active'
        
        try:
            entity_data['listing_status'] = self.listing_status_combo.currentText() or 'Listed'
        except (AttributeError, RuntimeError):
            entity_data['listing_status'] = 'Listed'
        
        # Sanction details with error handling and minimal defaults
        try:
            sanctioning_authority_text = self.sanctioning_authority_edit.text().strip()
            entity_data['sanctioning_authority'] = sanctioning_authority_text if sanctioning_authority_text else 'Internal Compliance'
        except (AttributeError, RuntimeError):
            entity_data['sanctioning_authority'] = 'Internal Compliance'
        
        try:
            program_text = self.program_edit.text().strip()
            entity_data['program'] = program_text if program_text else 'Custom Sanctions List'
        except (AttributeError, RuntimeError):
            entity_data['program'] = 'Custom Sanctions List'
        
        try:
            entity_data['legal_basis'] = self.legal_basis_edit.toPlainText().strip()
        except (AttributeError, RuntimeError):
            entity_data['legal_basis'] = ''
        
        try:
            entity_data['listing_date'] = self.listing_date_edit.date().toPython() if hasattr(self.listing_date_edit.date(), 'toPython') else self.listing_date_edit.date().toPyDate()
        except (AttributeError, RuntimeError):
            entity_data['listing_date'] = date.today()
        
        try:
            entity_data['measures_imposed'] = self.measures_imposed_edit.toPlainText().strip()
        except (AttributeError, RuntimeError):
            entity_data['measures_imposed'] = ''
        
        try:
            entity_data['reason_for_listing'] = self.reason_for_listing_edit.toPlainText().strip()
        except (AttributeError, RuntimeError):
            entity_data['reason_for_listing'] = ''
        
        # Metadata with error handling and minimal defaults
        try:
            data_source_text = self.data_source_edit.text().strip()
            entity_data['data_source'] = data_source_text if data_source_text else 'Manual Entry'
        except (AttributeError, RuntimeError):
            entity_data['data_source'] = 'Manual Entry'
        entity_data['created_by'] = self.created_by_edit.text().strip()
        entity_data['verified_by'] = self.verified_by_edit.text().strip()
        
        verified_date = self.verified_date_edit.date()
        if verified_date != self.verified_date_edit.minimumDate():
            entity_data['verified_date'] = verified_date.toPython() if hasattr(verified_date, 'toPython') else verified_date.toPyDate()
        
        # Notes history
        entity_data['notes_history'] = self.notes_history.copy()
        
        # Convert notes history to a single internal_notes field for backward compatibility
        if self.notes_history:
            latest_notes = [note['note'] for note in sorted(self.notes_history, key=lambda x: x['timestamp'], reverse=True)]
            entity_data['internal_notes'] = '\n'.join(latest_notes)
        else:
            entity_data['internal_notes'] = ""
        
        # Names
        entity_data['names'] = self.names_data.copy()
        
        # Addresses
        entity_data['addresses'] = self.addresses_data.copy()
        
        # Identifiers
        entity_data['identifiers'] = self.identifiers_data.copy()
        
        # Subject-specific details
        subject_type = self.subject_type_combo.currentData()
        if subject_type == SubjectType.INDIVIDUAL and self.individual_tab:
            entity_data['individual_details'] = self.collect_individual_details()
        elif subject_type == SubjectType.ENTITY and self.entity_tab:
            entity_data['entity_details'] = self.collect_entity_details()
        
        return entity_data
    
    def collect_individual_details(self) -> Dict[str, Any]:
        """Collect individual-specific details."""
        details = {}
        
        if self.birth_year_spin.value() > self.birth_year_spin.minimum():
            details['birth_year'] = self.birth_year_spin.value()
        if self.birth_month_spin.value() > 0:
            details['birth_month'] = self.birth_month_spin.value()
        if self.birth_day_spin.value() > 0:
            details['birth_day'] = self.birth_day_spin.value()
        
        birth_date = self.birth_full_date_edit.date()
        if birth_date != self.birth_full_date_edit.minimumDate():
            details['birth_full_date'] = birth_date.toPython() if hasattr(birth_date, 'toPython') else birth_date.toPyDate()
        
        if self.birth_note_edit.text().strip():
            details['birth_note'] = self.birth_note_edit.text().strip()
        if self.place_of_birth_edit.text().strip():
            details['place_of_birth'] = self.place_of_birth_edit.text().strip()
        
        nationalities_text = self.nationalities_edit.toPlainText().strip()
        if nationalities_text:
            details['nationalities'] = [line.strip() for line in nationalities_text.split('\n') if line.strip()]
        
        return details
    
    def collect_entity_details(self) -> Dict[str, Any]:
        """Collect entity-specific details."""
        details = {}
        
        if self.registration_number_edit.text().strip():
            details['registration_number'] = self.registration_number_edit.text().strip()
        if self.registration_authority_edit.text().strip():
            details['registration_authority'] = self.registration_authority_edit.text().strip()
        
        incorporation_date = self.incorporation_date_edit.date()
        if incorporation_date != self.incorporation_date_edit.minimumDate():
            details['incorporation_date'] = incorporation_date.toPython() if hasattr(incorporation_date, 'toPython') else incorporation_date.toPyDate()
        
        if self.company_type_edit.text().strip():
            details['company_type'] = self.company_type_edit.text().strip()
        if self.tax_id_edit.text().strip():
            details['tax_id'] = self.tax_id_edit.text().strip()
        
        return details
    
    def validate_data(self):
        """Validate the current form data."""
        try:
            entity_data = self.collect_entity_data()
            validation_result = self.service.validator.validate_entity_data(entity_data)
            
            self.show_validation_results(validation_result)
            
        except Exception as e:
            logger.error(f"Error during validation: {str(e)}")
            QMessageBox.critical(self, "Validation Error", f"An error occurred during validation: {str(e)}")
    
    def show_validation_results(self, validation_result: ValidationResult):
        """Display validation results to the user."""
        if validation_result.is_valid:
            self.validation_label.setText("✓ Validation Passed")
            self.validation_label.setStyleSheet("color: green; font-weight: bold;")
            
            # Get warnings from issues
            warnings = [issue for issue in validation_result.issues if issue.severity.value == 'warning']
            if warnings:
                warning_text = "Warnings:\n" + "\n".join([
                    f"• {warning.message}" for warning in warnings
                ])
                self.validation_text.setPlainText(warning_text)
                self.validation_text.setStyleSheet("color: orange;")
            else:
                self.validation_text.setPlainText("No issues found.")
                self.validation_text.setStyleSheet("color: green;")
        else:
            self.validation_label.setText("✗ Validation Failed")
            self.validation_label.setStyleSheet("color: red; font-weight: bold;")
            
            # Get errors and warnings from issues
            errors = [issue for issue in validation_result.issues if issue.severity.value == 'error']
            warnings = [issue for issue in validation_result.issues if issue.severity.value == 'warning']
            
            error_text = f"Errors ({validation_result.errors_count}):\n"
            error_text += "\n".join([
                f"• {error.message}" for error in errors
            ])
            
            if warnings:
                error_text += f"\n\nWarnings ({validation_result.warnings_count}):\n"
                error_text += "\n".join([
                    f"• {warning.message}" for warning in warnings
                ])
            
            self.validation_text.setPlainText(error_text)
            self.validation_text.setStyleSheet("color: red;")
        
        self.validation_frame.show()
    
    def save_entity(self):
        """Save the entity data."""
        try:
            entity_data = self.collect_entity_data()
            
            if self.entity_id:
                # Update existing entity
                success = self.service.update_sanction_entity(self.entity_id, entity_data)
                if success:
                    QMessageBox.information(self, "Success", "Entity updated successfully!")
                    self.entity_saved.emit(self.entity_id)
                else:
                    QMessageBox.warning(self, "Error", "Failed to update entity.")
            else:
                # Create new entity
                entity_id, validation_result = self.service.create_sanction_entity(entity_data)
                if entity_id:
                    self.entity_id = entity_id
                    QMessageBox.information(self, "Success", f"Entity created successfully!\nInternal ID: {entity_id}")
                    self.entity_saved.emit(entity_id)
                    
                    # Update the internal ID display
                    entity = self.service.get_sanction_entity(entity_id)
                    if entity:
                        self.internal_id_label.setText(entity.internal_entry_id)
                else:
                    QMessageBox.warning(self, "Error", "Failed to create entity.")
                    
        except ValueError as e:
            # Validation error - show detailed validation issues
            error_message = str(e)
            
            # Try to get detailed validation result for better error messages
            try:
                entity_data = self.collect_entity_data()
                validation_result = self.service.validator.validate_entity_data(entity_data)
                
                if not validation_result.is_valid:
                    error_details = []
                    for issue in validation_result.issues:
                        if issue.severity.value == 'error':
                            error_details.append(f"• {issue.field}: {issue.message}")
                    
                    if error_details:
                        error_message = "Please fix the following issues:\\n\\n" + "\\n".join(error_details)
                        error_message += "\\n\\nTip: Make sure to fill in the name field and all required information."
            except Exception:
                # If we can't get detailed validation, use the original error
                pass
            
            QMessageBox.warning(self, "Validation Error", error_message)
        except Exception as e:
            logger.error(f"Error saving entity: {str(e)}")
            QMessageBox.critical(self, "Error", f"An error occurred while saving: {str(e)}")
    
    def save_and_close(self):
        """Save the entity and close the dialog."""
        try:
            entity_data = self.collect_entity_data()
            
            if self.entity_id:
                # Update existing entity
                success = self.service.update_sanction_entity(self.entity_id, entity_data)
                if success:
                    self.entity_saved.emit(self.entity_id)
                    self.accept()
                else:
                    QMessageBox.warning(self, "Error", "Failed to update entity.")
            else:
                # Create new entity
                entity_id, validation_result = self.service.create_sanction_entity(entity_data)
                if entity_id:
                    self.entity_saved.emit(entity_id)
                    self.accept()
                else:
                    QMessageBox.warning(self, "Error", "Failed to create entity.")
                    
        except ValueError as e:
            # Validation error
            QMessageBox.warning(self, "Validation Error", str(e))
        except Exception as e:
            logger.error(f"Error saving entity: {str(e)}")
            QMessageBox.critical(self, "Error", f"An error occurred while saving: {str(e)}")
    
    def show_authority_options(self):
        """Show quick fill options for sanctioning authority."""
        from PyQt6.QtWidgets import QMenu
        
        menu = QMenu(self)
        
        common_authorities = [
            "Internal Compliance",
            "Risk Management Department", 
            "OFAC (US Treasury)",
            "EU Sanctions",
            "UN Security Council",
            "Financial Intelligence Unit",
            "Regulatory Authority",
            "Internal Risk Assessment"
        ]
        
        for authority in common_authorities:
            action = menu.addAction(authority)
            action.triggered.connect(lambda checked, text=authority: self.sanctioning_authority_edit.setText(text))
        
        # Show menu at button position
        menu.exec(self.quick_fill_authority_btn.mapToGlobal(self.quick_fill_authority_btn.rect().bottomLeft()))
    
    def show_program_options(self):
        """Show quick fill options for program."""
        from PyQt6.QtWidgets import QMenu
        
        menu = QMenu(self)
        
        common_programs = [
            "Internal Watchlist",
            "High Risk Individuals",
            "PEP (Politically Exposed Persons)",
            "Corporate Sanctions List",
            "Enhanced Due Diligence",
            "Regulatory Watchlist",
            "Customer Risk Assessment",
            "Compliance Monitoring"
        ]
        
        for program in common_programs:
            action = menu.addAction(program)
            action.triggered.connect(lambda checked, text=program: self.program_edit.setText(text))
        
        menu.exec(self.quick_fill_program_btn.mapToGlobal(self.quick_fill_program_btn.rect().bottomLeft()))
    
    def show_data_source_options(self):
        """Show quick fill options for data source."""
        from PyQt6.QtWidgets import QMenu
        
        menu = QMenu(self)
        
        common_sources = [
            "Manual Research",
            "News Report",
            "Intelligence Report",
            "Regulatory Notice",
            "Customer Information",
            "Third-Party Database",
            "Internal Investigation",
            "Public Records"
        ]
        
        for source in common_sources:
            action = menu.addAction(source)
            action.triggered.connect(lambda checked, text=source: self.data_source_edit.setText(text))
        
        menu.exec(self.quick_fill_data_source_btn.mapToGlobal(self.quick_fill_data_source_btn.rect().bottomLeft()))


# ==================== Helper Dialogs ====================

class NameEntryDialog(QDialog):
    """Dialog for entering name information."""
    
    def __init__(self, parent=None, name_data: Dict[str, str] = None):
        super().__init__(parent)
        self.name_data = name_data or {}
        self.setup_ui()
        self.load_data()
    
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle("Name Entry")
        self.setModal(True)
        self.resize(400, 200)
        
        layout = QFormLayout(self)
        
        # Full name
        self.full_name_edit = QLineEdit()
        self.full_name_edit.setPlaceholderText("Enter the full name")
        layout.addRow("Full Name*:", self.full_name_edit)
        
        # Name type
        self.name_type_combo = QComboBox()
        for name_type in NameType:
            self.name_type_combo.addItem(name_type.value, name_type)
        layout.addRow("Name Type*:", self.name_type_combo)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        ok_button = QPushButton("OK")
        ok_button.clicked.connect(self.accept)
        ok_button.setDefault(True)
        
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addStretch()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        
        layout.addRow(button_layout)
    
    def load_data(self):
        """Load existing data if editing."""
        if self.name_data:
            self.full_name_edit.setText(self.name_data.get('full_name', ''))
            
            name_type_value = self.name_data.get('name_type', '')
            for i in range(self.name_type_combo.count()):
                if self.name_type_combo.itemData(i).value == name_type_value:
                    self.name_type_combo.setCurrentIndex(i)
                    break
    
    def get_name_data(self) -> Dict[str, str]:
        """Get the entered name data."""
        return {
            'full_name': self.full_name_edit.text().strip(),
            'name_type': self.name_type_combo.currentData().value
        }
    
    def accept(self):
        """Validate and accept the dialog."""
        if not self.full_name_edit.text().strip():
            QMessageBox.warning(self, "Validation Error", "Full name is required.")
            return
        
        super().accept()


class AddressEntryDialog(QDialog):
    """Dialog for entering address information."""
    
    def __init__(self, parent=None, address_data: Dict[str, str] = None, is_entity: bool = False):
        super().__init__(parent)
        self.address_data = address_data or {}
        self.is_entity = is_entity
        self.setup_ui()
        self.load_data()
    
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle("Address Entry")
        self.setModal(True)
        self.resize(500, 350)
        
        layout = QFormLayout(self)
        
        # Address type (for entities)
        if self.is_entity:
            self.address_type_combo = QComboBox()
            self.address_type_combo.addItems([
                "Registered Business Address",
                "Operational Address", 
                "Mailing Address",
                "Other Address"
            ])
            layout.addRow("Address Type:", self.address_type_combo)
        
        # Street
        self.street_edit = QLineEdit()
        self.street_edit.setPlaceholderText("Street address")
        layout.addRow("Street:", self.street_edit)
        
        # City
        self.city_edit = QLineEdit()
        self.city_edit.setPlaceholderText("City")
        layout.addRow("City:", self.city_edit)
        
        # Postal code
        self.postal_code_edit = QLineEdit()
        self.postal_code_edit.setPlaceholderText("Postal/ZIP code")
        layout.addRow("Postal Code:", self.postal_code_edit)
        
        # Country
        self.country_edit = QLineEdit()
        self.country_edit.setPlaceholderText("Country")
        layout.addRow("Country:", self.country_edit)
        
        # Full address (alternative)
        self.full_address_edit = QTextEdit()
        self.full_address_edit.setMaximumHeight(80)
        self.full_address_edit.setPlaceholderText("Complete address (alternative to individual fields)")
        layout.addRow("Full Address:", self.full_address_edit)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        ok_button = QPushButton("OK")
        ok_button.clicked.connect(self.accept)
        ok_button.setDefault(True)
        
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addStretch()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        
        layout.addRow(button_layout)
    
    def load_data(self):
        """Load existing data if editing."""
        if self.address_data:
            if self.is_entity and hasattr(self, 'address_type_combo'):
                address_type = self.address_data.get('address_type', 'Other Address')
                index = self.address_type_combo.findText(address_type)
                if index >= 0:
                    self.address_type_combo.setCurrentIndex(index)
            
            self.street_edit.setText(self.address_data.get('street', ''))
            self.city_edit.setText(self.address_data.get('city', ''))
            self.postal_code_edit.setText(self.address_data.get('postal_code', ''))
            self.country_edit.setText(self.address_data.get('country', ''))
            self.full_address_edit.setPlainText(self.address_data.get('full_address', ''))
    
    def get_address_data(self) -> Dict[str, str]:
        """Get the entered address data."""
        data = {
            'street': self.street_edit.text().strip(),
            'city': self.city_edit.text().strip(),
            'postal_code': self.postal_code_edit.text().strip(),
            'country': self.country_edit.text().strip(),
            'full_address': self.full_address_edit.toPlainText().strip()
        }
        
        # Add address type for entities
        if self.is_entity and hasattr(self, 'address_type_combo'):
            data['address_type'] = self.address_type_combo.currentText()
        
        return data


class IdentifierEntryDialog(QDialog):
    """Dialog for entering identifier information."""
    
    def __init__(self, parent=None, identifier_data: Dict[str, str] = None):
        super().__init__(parent)
        self.identifier_data = identifier_data or {}
        self.setup_ui()
        self.load_data()
    
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle("Identifier Entry")
        self.setModal(True)
        self.resize(450, 250)
        
        layout = QFormLayout(self)
        
        # ID Type
        self.id_type_edit = QLineEdit()
        self.id_type_edit.setPlaceholderText("e.g., Passport, Driver's License, Tax ID")
        layout.addRow("ID Type*:", self.id_type_edit)
        
        # ID Value
        self.id_value_edit = QLineEdit()
        self.id_value_edit.setPlaceholderText("Identifier number or value")
        layout.addRow("ID Value*:", self.id_value_edit)
        
        # Issuing Country
        self.issuing_country_edit = QLineEdit()
        self.issuing_country_edit.setPlaceholderText("Country that issued this identifier")
        layout.addRow("Issuing Country:", self.issuing_country_edit)
        
        # Notes
        self.notes_edit = QTextEdit()
        self.notes_edit.setMaximumHeight(60)
        self.notes_edit.setPlaceholderText("Additional notes about this identifier")
        layout.addRow("Notes:", self.notes_edit)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        ok_button = QPushButton("OK")
        ok_button.clicked.connect(self.accept)
        ok_button.setDefault(True)
        
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addStretch()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        
        layout.addRow(button_layout)
    
    def load_data(self):
        """Load existing data if editing."""
        if self.identifier_data:
            self.id_type_edit.setText(self.identifier_data.get('id_type', ''))
            self.id_value_edit.setText(self.identifier_data.get('id_value', ''))
            self.issuing_country_edit.setText(self.identifier_data.get('issuing_country', ''))
            self.notes_edit.setPlainText(self.identifier_data.get('notes', ''))
    
    def get_identifier_data(self) -> Dict[str, str]:
        """Get the entered identifier data."""
        return {
            'id_type': self.id_type_edit.text().strip(),
            'id_value': self.id_value_edit.text().strip(),
            'issuing_country': self.issuing_country_edit.text().strip(),
            'notes': self.notes_edit.toPlainText().strip()
        }
    
    def accept(self):
        """Validate and accept the dialog."""
        if not self.id_type_edit.text().strip():
            QMessageBox.warning(self, "Validation Error", "ID Type is required.")
            return
        
        if not self.id_value_edit.text().strip():
            QMessageBox.warning(self, "Validation Error", "ID Value is required.")
            return
        
        super().accept()