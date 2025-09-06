"""
Simple migration system for database schema changes.
"""
import logging
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

logger = logging.getLogger(__name__)


class Migration:
    """Base class for database migrations."""
    
    def __init__(self, version, description):
        self.version = version
        self.description = description
        self.timestamp = datetime.utcnow()
    
    def up(self, session):
        """Apply the migration."""
        raise NotImplementedError("Subclasses must implement the up() method")
    
    def down(self, session):
        """Rollback the migration."""
        raise NotImplementedError("Subclasses must implement the down() method")


class MigrationManager:
    """
    Manages database migrations and schema versioning.
    """
    
    def __init__(self, database_manager):
        self.db_manager = database_manager
        self.migrations = []
        self._ensure_migration_table()
    
    def _ensure_migration_table(self):
        """Ensure the migration tracking table exists."""
        try:
            session = self.db_manager.get_session()
            try:
                # Create migration table if it doesn't exist
                session.execute(text("""
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version INTEGER PRIMARY KEY,
                        description TEXT NOT NULL,
                        applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                session.commit()
            finally:
                self.db_manager.close_session(session)
        except SQLAlchemyError as e:
            logger.error(f"Failed to create migration table: {e}")
    
    def register_migration(self, migration):
        """
        Register a migration.
        
        Args:
            migration: Migration instance to register.
        """
        self.migrations.append(migration)
        # Sort migrations by version
        self.migrations.sort(key=lambda m: m.version)
    
    def get_current_version(self):
        """
        Get the current database schema version.
        
        Returns:
            int: Current schema version, 0 if no migrations applied.
        """
        try:
            session = self.db_manager.get_session()
            try:
                result = session.execute(text(
                    "SELECT MAX(version) FROM schema_migrations"
                )).scalar()
                return result or 0
            finally:
                self.db_manager.close_session(session)
        except SQLAlchemyError as e:
            logger.error(f"Failed to get current version: {e}")
            return 0
    
    def get_applied_migrations(self):
        """
        Get list of applied migrations.
        
        Returns:
            list: List of applied migration versions.
        """
        try:
            session = self.db_manager.get_session()
            try:
                result = session.execute(text(
                    "SELECT version, description, applied_at FROM schema_migrations ORDER BY version"
                )).fetchall()
                return [{'version': row[0], 'description': row[1], 'applied_at': row[2]} for row in result]
            finally:
                self.db_manager.close_session(session)
        except SQLAlchemyError as e:
            logger.error(f"Failed to get applied migrations: {e}")
            return []
    
    def migrate_to_latest(self):
        """
        Apply all pending migrations to bring database to latest version.
        
        Returns:
            bool: True if all migrations applied successfully, False otherwise.
        """
        current_version = self.get_current_version()
        pending_migrations = [m for m in self.migrations if m.version > current_version]
        
        if not pending_migrations:
            logger.info("Database is already at the latest version")
            return True
        
        logger.info(f"Applying {len(pending_migrations)} pending migrations")
        
        for migration in pending_migrations:
            if not self._apply_migration(migration):
                return False
        
        logger.info("All migrations applied successfully")
        return True
    
    def migrate_to_version(self, target_version):
        """
        Migrate database to a specific version.
        
        Args:
            target_version: Target schema version.
            
        Returns:
            bool: True if migration successful, False otherwise.
        """
        current_version = self.get_current_version()
        
        if target_version == current_version:
            logger.info(f"Database is already at version {target_version}")
            return True
        
        if target_version > current_version:
            # Forward migration
            pending_migrations = [m for m in self.migrations 
                                if current_version < m.version <= target_version]
            for migration in pending_migrations:
                if not self._apply_migration(migration):
                    return False
        else:
            # Backward migration (rollback)
            rollback_migrations = [m for m in reversed(self.migrations) 
                                 if target_version < m.version <= current_version]
            for migration in rollback_migrations:
                if not self._rollback_migration(migration):
                    return False
        
        return True
    
    def _apply_migration(self, migration):
        """
        Apply a single migration.
        
        Args:
            migration: Migration to apply.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            session = self.db_manager.get_session()
            try:
                logger.info(f"Applying migration {migration.version}: {migration.description}")
                
                # Apply the migration
                migration.up(session)
                
                # Record the migration
                session.execute(text(
                    "INSERT INTO schema_migrations (version, description) VALUES (:version, :description)"
                ), {'version': migration.version, 'description': migration.description})
                
                session.commit()
                logger.info(f"Migration {migration.version} applied successfully")
                return True
                
            except Exception as e:
                session.rollback()
                logger.error(f"Failed to apply migration {migration.version}: {e}")
                return False
            finally:
                self.db_manager.close_session(session)
                
        except SQLAlchemyError as e:
            logger.error(f"Database error during migration {migration.version}: {e}")
            return False
    
    def _rollback_migration(self, migration):
        """
        Rollback a single migration.
        
        Args:
            migration: Migration to rollback.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            session = self.db_manager.get_session()
            try:
                logger.info(f"Rolling back migration {migration.version}: {migration.description}")
                
                # Rollback the migration
                migration.down(session)
                
                # Remove the migration record
                session.execute(text(
                    "DELETE FROM schema_migrations WHERE version = :version"
                ), {'version': migration.version})
                
                session.commit()
                logger.info(f"Migration {migration.version} rolled back successfully")
                return True
                
            except Exception as e:
                session.rollback()
                logger.error(f"Failed to rollback migration {migration.version}: {e}")
                return False
            finally:
                self.db_manager.close_session(session)
                
        except SQLAlchemyError as e:
            logger.error(f"Database error during rollback {migration.version}: {e}")
            return False


# Example migration for adding indexes
class AddIndexesMigration(Migration):
    """Example migration to add performance indexes."""
    
    def __init__(self):
        super().__init__(1, "Add performance indexes to core tables")
    
    def up(self, session):
        """Add indexes for better query performance."""
        # These indexes are already defined in the models, but this shows how migrations work
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_sanctioned_entities_name_search 
            ON sanctioned_entities(name COLLATE NOCASE)
        """))
        
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_search_results_confidence 
            ON search_results(overall_confidence DESC)
        """))
    
    def down(self, session):
        """Remove the indexes."""
        session.execute(text("DROP INDEX IF EXISTS idx_sanctioned_entities_name_search"))
        session.execute(text("DROP INDEX IF EXISTS idx_search_results_confidence"))


class AddCustomSanctionsMigration(Migration):
    """Migration to add custom sanctions tables."""
    
    def __init__(self):
        super().__init__(2, "Add custom sanctions management tables")
    
    def up(self, session):
        """Create custom sanctions tables."""
        # Main entity table
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS custom_sanction_entities (
                id VARCHAR(36) PRIMARY KEY,
                internal_entry_id VARCHAR(50) UNIQUE NOT NULL,
                subject_type VARCHAR(20) NOT NULL CHECK (subject_type IN ('Individual', 'Entity', 'Vessel', 'Aircraft', 'Other')),
                sanctioning_authority VARCHAR(200) NOT NULL,
                program VARCHAR(200) NOT NULL,
                legal_basis TEXT,
                listing_date DATE NOT NULL,
                measures_imposed TEXT,
                reason_for_listing TEXT,
                data_source VARCHAR(500) NOT NULL,
                record_status VARCHAR(20) NOT NULL DEFAULT 'Active' CHECK (record_status IN ('Active', 'Delisted', 'Inactive', 'Pending')),
                last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                internal_notes TEXT,
                created_by VARCHAR(100),
                verified_by VARCHAR(100),
                verified_date DATETIME,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Names and aliases table
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS custom_sanction_names (
                id VARCHAR(36) PRIMARY KEY,
                entity_id VARCHAR(36) NOT NULL,
                full_name TEXT NOT NULL,
                name_type VARCHAR(20) NOT NULL CHECK (name_type IN ('Primary', 'Alias', 'AKA', 'FKA', 'Low Quality AKA')),
                FOREIGN KEY (entity_id) REFERENCES custom_sanction_entities(id) ON DELETE CASCADE
            )
        """))
        
        # Individual-specific details table
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS custom_sanction_individuals (
                id VARCHAR(36) PRIMARY KEY,
                entity_id VARCHAR(36) NOT NULL UNIQUE,
                birth_year INTEGER,
                birth_month INTEGER,
                birth_day INTEGER,
                birth_full_date DATE,
                birth_note TEXT,
                place_of_birth VARCHAR(200),
                nationalities JSON,
                FOREIGN KEY (entity_id) REFERENCES custom_sanction_entities(id) ON DELETE CASCADE
            )
        """))
        
        # Entity-specific details table
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS custom_sanction_entity_details (
                id VARCHAR(36) PRIMARY KEY,
                entity_id VARCHAR(36) NOT NULL UNIQUE,
                registration_number VARCHAR(100),
                registration_authority VARCHAR(200),
                incorporation_date DATE,
                company_type VARCHAR(100),
                tax_id VARCHAR(100),
                FOREIGN KEY (entity_id) REFERENCES custom_sanction_entities(id) ON DELETE CASCADE
            )
        """))
        
        # Addresses table
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS custom_sanction_addresses (
                id VARCHAR(36) PRIMARY KEY,
                entity_id VARCHAR(36) NOT NULL,
                street TEXT,
                city VARCHAR(100),
                postal_code VARCHAR(20),
                country VARCHAR(100),
                full_address TEXT,
                FOREIGN KEY (entity_id) REFERENCES custom_sanction_entities(id) ON DELETE CASCADE
            )
        """))
        
        # Identifiers table
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS custom_sanction_identifiers (
                id VARCHAR(36) PRIMARY KEY,
                entity_id VARCHAR(36) NOT NULL,
                id_type VARCHAR(100) NOT NULL,
                id_value VARCHAR(200) NOT NULL,
                issuing_country VARCHAR(100),
                notes TEXT,
                FOREIGN KEY (entity_id) REFERENCES custom_sanction_entities(id) ON DELETE CASCADE
            )
        """))
        
        # Audit log table
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS custom_sanction_audit_log (
                id VARCHAR(36) PRIMARY KEY,
                entity_id VARCHAR(36) NOT NULL,
                action VARCHAR(20) NOT NULL CHECK (action IN ('CREATE', 'UPDATE', 'DELETE', 'STATUS_CHANGE')),
                user_id VARCHAR(100),
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                changes JSON,
                notes TEXT,
                FOREIGN KEY (entity_id) REFERENCES custom_sanction_entities(id) ON DELETE CASCADE
            )
        """))
        
        # Create indexes for better performance
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_entities_internal_id ON custom_sanction_entities(internal_entry_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_entities_subject_type ON custom_sanction_entities(subject_type)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_entities_status ON custom_sanction_entities(record_status)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_names_entity_id ON custom_sanction_names(entity_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_names_full_name ON custom_sanction_names(full_name)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_names_type ON custom_sanction_names(name_type)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_individuals_entity_id ON custom_sanction_individuals(entity_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_entity_details_entity_id ON custom_sanction_entity_details(entity_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_entity_details_reg_number ON custom_sanction_entity_details(registration_number)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_entity_details_tax_id ON custom_sanction_entity_details(tax_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_addresses_entity_id ON custom_sanction_addresses(entity_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_addresses_city ON custom_sanction_addresses(city)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_addresses_country ON custom_sanction_addresses(country)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_identifiers_entity_id ON custom_sanction_identifiers(entity_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_identifiers_type ON custom_sanction_identifiers(id_type)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_identifiers_value ON custom_sanction_identifiers(id_value)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_identifiers_country ON custom_sanction_identifiers(issuing_country)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_audit_entity_id ON custom_sanction_audit_log(entity_id)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_audit_action ON custom_sanction_audit_log(action)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_audit_timestamp ON custom_sanction_audit_log(timestamp)"))
        session.execute(text("CREATE INDEX IF NOT EXISTS idx_custom_audit_user ON custom_sanction_audit_log(user_id)"))
    
    def down(self, session):
        """Drop custom sanctions tables."""
        # Drop tables in reverse order due to foreign key constraints
        session.execute(text("DROP TABLE IF EXISTS custom_sanction_audit_log"))
        session.execute(text("DROP TABLE IF EXISTS custom_sanction_identifiers"))
        session.execute(text("DROP TABLE IF EXISTS custom_sanction_addresses"))
        session.execute(text("DROP TABLE IF EXISTS custom_sanction_entity_details"))
        session.execute(text("DROP TABLE IF EXISTS custom_sanction_individuals"))
        session.execute(text("DROP TABLE IF EXISTS custom_sanction_names"))
        session.execute(text("DROP TABLE IF EXISTS custom_sanction_entities"))