from dataclasses import dataclass
from typing import Dict, List


@dataclass
class OracleDatabaseConfig:
    """Configuration for Oracle database container."""
    container_name: str = "oracle_test"
    db_password: str = "oracle"
    db_port: int = 1522
    image_name: str = "gvenzl/oracle-xe:21-slim"
    app_user: str = "testuser"
    app_user_password: str = "testpass"
    ready_timeout: int = 120  # seconds
    health_check_interval: int = 5  # seconds


@dataclass
class SchemaConfig:
    """Configuration for database schema."""
    name: str
    password: str
    tables: List[Dict]


@dataclass
class TableConfig:
    """Configuration for database table."""
    name: str
    columns: Dict[str, str]
    sample_data: List[Dict]
