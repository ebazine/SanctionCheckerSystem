# Sanctions Checker

A comprehensive Python application for checking names against international sanctions lists including EU, UN, OFAC, and custom sanctions.

## Features

- **Multi-Source Sanctions Checking**: Supports EU, UN, OFAC, and custom sanctions lists
- **Advanced Search**: Fuzzy matching and name normalization for accurate results
- **Custom Sanctions Management**: Create and manage your own sanctions lists
- **Batch Processing**: Check multiple names at once
- **Export Capabilities**: Generate PDF reports and export data
- **Data Quality Tools**: Validate and maintain sanctions data integrity
- **User-Friendly GUI**: Modern PyQt6-based interface
- **Automated Updates**: Keep sanctions data current

## Installation

### Requirements
- Python 3.8 or higher
- PyQt6
- SQLAlchemy
- Requests
- Other dependencies listed in requirements.txt

### Setup
1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run the application: `python main.py`

## Usage

### Basic Search
1. Launch the application
2. Enter a name in the search field
3. Click "Search" to check against all sanctions lists
4. Review results and export if needed

### Custom Sanctions
1. Go to "Custom Sanctions" tab
2. Add new entities with detailed information
3. Import/export custom sanctions lists
4. Manage data quality and validation

### Batch Processing
1. Use "Batch Search" feature
2. Upload a file with multiple names
3. Process all names simultaneously
4. Export comprehensive results

## Architecture

- **GUI Layer**: PyQt6-based user interface
- **Service Layer**: Business logic and data processing
- **Data Layer**: SQLAlchemy ORM with SQLite database
- **Integration Layer**: APIs for external sanctions sources

## Key Components

- `sanctions_checker/gui/`: User interface components
- `sanctions_checker/services/`: Core business logic
- `sanctions_checker/models/`: Database models
- `sanctions_checker/database/`: Database management
- `tests/`: Comprehensive test suite
- `docs/`: Documentation and user guides

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions, please use the GitHub issue tracker.
