#!/usr/bin/env python3
"""
PyInstaller build specification generator for Sanctions Checker.
This script creates the .spec file and handles the build process.
"""

import os
import sys
from pathlib import Path

try:
    import PyInstaller.__main__
    PYINSTALLER_AVAILABLE = True
except ImportError:
    PYINSTALLER_AVAILABLE = False

def create_spec_file():
    """Create PyInstaller spec file for the application."""
    
    spec_content = '''# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

# Define the main application entry point
a = Analysis(
    ['run_gui.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('sanctions_checker/gui/icons', 'sanctions_checker/gui/icons'),
        ('sanctions_checker/assets', 'sanctions_checker/assets'),
        ('sanctions_checker/config', 'sanctions_checker/config'),
        ('README.md', '.'),
        ('LICENSE', '.'),
        ('LOGO_INTEGRATION_INSTRUCTIONS.md', '.'),
        ('LOGO_UPLOAD_IMPLEMENTATION_SUMMARY.md', '.'),
    ],
    hiddenimports=[
        'PyQt6.QtCore',
        'PyQt6.QtGui', 
        'PyQt6.QtWidgets',
        'PyQt6.QtPrintSupport',
        'sqlalchemy.dialects.sqlite',
        'reportlab.pdfgen',
        'reportlab.lib',
        'reportlab.platypus',
        'cryptography.fernet',
        'lxml.etree',
        'fuzzywuzzy',
        'Levenshtein',
        'sanctions_checker.gui.logo_upload_dialog',
        'sanctions_checker.utils.resources',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter',
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'PIL',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='SanctionsChecker',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='sanctions_checker/gui/icons/app_icon.ico' if os.path.exists('sanctions_checker/gui/icons/app_icon.ico') else None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='SanctionsChecker',
)
'''
    
    with open('sanctions_checker.spec', 'w') as f:
        f.write(spec_content)
    
    print("Created sanctions_checker.spec file")

def build_application():
    """Build the application using PyInstaller."""
    
    if not PYINSTALLER_AVAILABLE:
        print("Error: PyInstaller is not installed. Please install it using:")
        print("pip install -r build_requirements.txt")
        return False
    
    # Create spec file first
    create_spec_file()
    
    # Build using the spec file
    PyInstaller.__main__.run([
        'sanctions_checker.spec',
        '--clean',
        '--noconfirm',
    ])
    
    print("Build completed. Check the 'dist' folder for the executable.")
    return True

if __name__ == "__main__":
    build_application()