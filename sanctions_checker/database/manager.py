"""
Database manager for handling database initialization, connections, and migrations.
"""
import os
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from ..models import (Base, SanctionedEntity, SearchRecord, SearchResult,
                      CustomSanctionEntity, CustomSanctionName, CustomSanctionIndividual,
                      CustomSanctionEntityDetails, CustomSanctionAddress, CustomSanctionIdentifier,
                      CustomSanctionAuditLog)
from ..config import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database connections, initialization, and basic migration operations.
    """
    
    def __init__(self, database_url=None):
        """
        Initialize the database manager.
        
        Args:
            database_url: Optional database URL. If not provided, uses config.
        """
        self.database_url = database_url or self._get_database_url()
        self.engine = None
        self.session_factory = None
        self.Session = None
        
    def _get_database_url(self):
        """Get database URL from configuration."""
        config = Config()
        db_path = config.get('database', {}).get('path', 'data/sanctions.db')
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        return f"sqlite:///{db_path}"
    
    def initialize_database(self):
        """
        Initialize the database connection and create tables if they don't exist.
        
        Returns:
            bool: True if initialization was successful, False otherwise.
        """
        try:
            # Create engine
            self.engine = create_engine(
                self.database_url,
                echo=False,  # Set to True for SQL debugging
                pool_pre_ping=True,
                connect_args={"check_same_thread": False} if "sqlite" in self.database_url else {}
            )
            
            # Create session factory
            self.session_factory = sessionmaker(bind=self.engine)
            self.Session = scoped_session(self.session_factory)
            
            # Create all tables
            Base.metadata.create_all(self.engine)
            
            logger.info("Database initialized successfully")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize database: {e}")
            return False
    
    def get_session(self):
        """
        Get a database session.
        
        Returns:
            Session: SQLAlchemy session object.
        """
        if not self.Session:
            raise RuntimeError("Database not initialized. Call initialize_database() first.")
        
        return self.Session()
    
    def close_session(self, session):
        """
        Close a database session.
        
        Args:
            session: SQLAlchemy session to close.
        """
        if session:
            session.close()
    
    def check_database_health(self):
        """
        Check if the database is accessible and tables exist.
        
        Returns:
            dict: Health check results.
        """
        health_status = {
            'database_accessible': False,
            'tables_exist': False,
            'table_counts': {},
            'errors': []
        }
        
        try:
            # Check if database is accessible
            inspector = inspect(self.engine)
            health_status['database_accessible'] = True
            
            # Check if required tables exist
            required_tables = ['sanctioned_entities', 'search_records', 'search_results']
            custom_sanctions_tables = [
                'custom_sanction_entities', 'custom_sanction_names', 'custom_sanction_individuals',
                'custom_sanction_entity_details', 'custom_sanction_addresses', 
                'custom_sanction_identifiers', 'custom_sanction_audit_log'
            ]
            existing_tables = inspector.get_table_names()
            
            health_status['tables_exist'] = all(table in existing_tables for table in required_tables)
            health_status['custom_sanctions_tables_exist'] = all(table in existing_tables for table in custom_sanctions_tables)
            
            # Get table counts
            session = self.get_session()
            try:
                health_status['table_counts'] = {
                    'sanctioned_entities': session.query(SanctionedEntity).count(),
                    'search_records': session.query(SearchRecord).count(),
                    'search_results': session.query(SearchResult).count()
                }
                
                # Add custom sanctions table counts if tables exist
                if health_status['custom_sanctions_tables_exist']:
                    health_status['table_counts'].update({
                        'custom_sanction_entities': session.query(CustomSanctionEntity).count(),
                        'custom_sanction_names': session.query(CustomSanctionName).count(),
                        'custom_sanction_individuals': session.query(CustomSanctionIndividual).count(),
                        'custom_sanction_entity_details': session.query(CustomSanctionEntityDetails).count(),
                        'custom_sanction_addresses': session.query(CustomSanctionAddress).count(),
                        'custom_sanction_identifiers': session.query(CustomSanctionIdentifier).count(),
                        'custom_sanction_audit_log': session.query(CustomSanctionAuditLog).count()
                    })
            finally:
                self.close_session(session)
                
        except Exception as e:
            health_status['errors'].append(str(e))
            logger.error(f"Database health check failed: {e}")
        
        return health_status
    
    def backup_database(self, backup_path):
        """
        Create a backup of the database.
        
        Args:
            backup_path: Path where to save the backup.
            
        Returns:
            bool: True if backup was successful, False otherwise.
        """
        try:
            if "sqlite" in self.database_url:
                import shutil
                db_path = self.database_url.replace("sqlite:///", "")
                shutil.copy2(db_path, backup_path)
                logger.info(f"Database backed up to {backup_path}")
                return True
            else:
                logger.warning("Backup not implemented for non-SQLite databases")
                return False
                
        except Exception as e:
            logger.error(f"Failed to backup database: {e}")
            return False
    
    def drop_all_tables(self):
        """
        Drop all tables. Use with caution!
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            Base.metadata.drop_all(self.engine)
            logger.warning("All database tables dropped")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to drop tables: {e}")
            return False
    
    def recreate_tables(self):
        """
        Drop and recreate all tables. Use with caution!
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            self.drop_all_tables()
            Base.metadata.create_all(self.engine)
            logger.info("Database tables recreated")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to recreate tables: {e}")
            return False
    
    def close(self):
        """Close the database connection."""
        if self.Session:
            self.Session.remove()
        if self.engine:
            self.engine.dispose()
        logger.info("Database connection closed")