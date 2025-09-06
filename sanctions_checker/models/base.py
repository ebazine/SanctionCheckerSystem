"""
Base model configuration for SQLAlchemy models.
"""
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
import uuid
import enum

# Create the declarative base
Base = declarative_base()

# Metadata for migrations
metadata = MetaData()


class SubjectType(enum.Enum):
    """Enumeration for custom sanction subject types."""
    INDIVIDUAL = "Individual"
    ENTITY = "Entity"
    VESSEL = "Vessel"
    AIRCRAFT = "Aircraft"
    OTHER = "Other"


class NameType(enum.Enum):
    """Enumeration for name types in custom sanctions."""
    PRIMARY = "Primary"
    ALIAS = "Alias"
    AKA = "AKA"
    FKA = "FKA"
    LOW_QUALITY_AKA = "Low Quality AKA"


class RecordStatus(enum.Enum):
    """Enumeration for custom sanction record status."""
    ACTIVE = "Active"
    DELISTED = "Delisted"
    INACTIVE = "Inactive"
    PENDING = "Pending"


def generate_uuid():
    """Generate a UUID string for primary keys."""
    return str(uuid.uuid4())

def get_current_timestamp():
    """Get current timestamp for created/updated fields."""
    return datetime.utcnow()