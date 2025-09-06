"""
Logo Upload Dialog for Sanctions Checker Application

This module provides a dialog for users to upload and replace the application logo.
"""

import os
import shutil
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QPushButton, 
                             QLabel, QFileDialog, QMessageBox, QFrame)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QPixmap, QIcon
from sanctions_checker.utils.resources import resource_manager


class LogoUploadDialog(QDialog):
    """Dialog for uploading and managing application logos."""
    
    logo_updated = pyqtSignal()  # Signal emitted when logo is successfully updated
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.resource_manager = resource_manager
        self.selected_file_path = None
        self.setup_ui()
        self.load_current_logo()
        
    def setup_ui(self):
        """Set up the user interface."""
        self.setWindowTitle("Upload Logo")
        self.setModal(True)
        self.setFixedSize(400, 300)
        
        layout = QVBoxLayout(self)
        
        # Current logo section
        current_frame = QFrame()
        current_frame.setFrameStyle(QFrame.Shape.StyledPanel)
        current_layout = QVBoxLayout(current_frame)
        
        current_layout.addWidget(QLabel("Current Logo:"))
        self.current_logo_label = QLabel()
        self.current_logo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.current_logo_label.setMinimumHeight(100)
        self.current_logo_label.setStyleSheet("border: 1px solid gray;")
        current_layout.addWidget(self.current_logo_label)
        
        layout.addWidget(current_frame)
        
        # Upload section
        upload_frame = QFrame()
        upload_frame.setFrameStyle(QFrame.Shape.StyledPanel)
        upload_layout = QVBoxLayout(upload_frame)
        
        upload_layout.addWidget(QLabel("Upload New Logo:"))
        
        # File selection
        file_layout = QHBoxLayout()
        self.file_path_label = QLabel("No file selected")
        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self.browse_file)
        
        file_layout.addWidget(self.file_path_label)
        file_layout.addWidget(self.browse_button)
        upload_layout.addLayout(file_layout)
        
        # Preview
        self.preview_label = QLabel("Preview will appear here")
        self.preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.preview_label.setMinimumHeight(80)
        self.preview_label.setStyleSheet("border: 1px dashed gray;")
        upload_layout.addWidget(self.preview_label)
        
        layout.addWidget(upload_frame)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.upload_button = QPushButton("Upload Logo")
        self.upload_button.clicked.connect(self.upload_logo)
        self.upload_button.setEnabled(False)
        
        self.reset_button = QPushButton("Reset to Default")
        self.reset_button.clicked.connect(self.reset_to_default)
        
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        
        button_layout.addWidget(self.upload_button)
        button_layout.addWidget(self.reset_button)
        button_layout.addStretch()
        button_layout.addWidget(cancel_button)
        
        layout.addLayout(button_layout)
        
    def load_current_logo(self):
        """Load and display the current logo."""
        try:
            logo_pixmap = self.resource_manager.get_logo_pixmap(150, 80)
            if logo_pixmap and not logo_pixmap.isNull():
                self.current_logo_label.setPixmap(logo_pixmap)
            else:
                self.current_logo_label.setText("No logo available")
        except Exception as e:
            self.current_logo_label.setText(f"Error loading logo: {str(e)}")
            
    def browse_file(self):
        """Open file dialog to select logo file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Logo File",
            "",
            "Image Files (*.png *.jpg *.jpeg *.bmp *.gif *.svg);;All Files (*)"
        )
        
        if file_path:
            self.selected_file_path = file_path
            self.file_path_label.setText(os.path.basename(file_path))
            self.upload_button.setEnabled(True)
            self.show_preview(file_path)
            
    def show_preview(self, file_path):
        """Show preview of selected image."""
        try:
            pixmap = QPixmap(file_path)
            if not pixmap.isNull():
                # Scale preview to fit
                scaled_pixmap = pixmap.scaled(150, 60, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                self.preview_label.setPixmap(scaled_pixmap)
            else:
                self.preview_label.setText("Invalid image file")
        except Exception as e:
            self.preview_label.setText(f"Preview error: {str(e)}")
            
    def upload_logo(self):
        """Upload the selected logo file."""
        if not self.selected_file_path:
            return
            
        try:
            # Validate file
            if not os.path.exists(self.selected_file_path):
                QMessageBox.warning(self, "Error", "Selected file does not exist.")
                return
                
            # Check file size (limit to 5MB)
            file_size = os.path.getsize(self.selected_file_path)
            if file_size > 5 * 1024 * 1024:  # 5MB
                QMessageBox.warning(self, "Error", "File size too large. Please select a file smaller than 5MB.")
                return
                
            # Test if image can be loaded
            test_pixmap = QPixmap(self.selected_file_path)
            if test_pixmap.isNull():
                QMessageBox.warning(self, "Error", "Invalid image file format.")
                return
                
            # Copy file to assets directory
            success = self.resource_manager.install_logo(self.selected_file_path)
            
            if success:
                QMessageBox.information(self, "Success", "Logo uploaded successfully!")
                self.logo_updated.emit()
                self.load_current_logo()  # Refresh current logo display
                self.accept()
            else:
                QMessageBox.warning(self, "Error", "Failed to upload logo. Please try again.")
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred while uploading the logo:\n{str(e)}")
            
    def reset_to_default(self):
        """Reset logo to default."""
        reply = QMessageBox.question(
            self, 
            "Reset Logo", 
            "Are you sure you want to reset to the default logo?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                success = self.resource_manager.reset_to_default_logo()
                if success:
                    QMessageBox.information(self, "Success", "Logo reset to default successfully!")
                    self.logo_updated.emit()
                    self.load_current_logo()
                else:
                    QMessageBox.warning(self, "Error", "Failed to reset logo.")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"An error occurred while resetting the logo:\n{str(e)}")