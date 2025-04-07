from dataclasses import dataclass
from typing import Dict, List

from docker_tools.docker_manager import ContainerConfig


@dataclass
class OracleDatabaseConfig(ContainerConfig):
    """Oracle-specific configuration extending base container config."""
    db_password: str = "oracle"
    db_port: int = 1522
    app_user: str = "testuser"
    app_user_password: str = "testpass"

    def __post_init__(self):
        """Set Oracle-specific defaults after initialization."""
        if self.ports is None:
            self.ports = {"1521/tcp": self.db_port}
        if self.environment is None:
            self.environment = {
                "ORACLE_PASSWORD": self.db_password,
                "APP_USER": self.app_user,
                "APP_USER_PASSWORD": self.app_user_password
            }


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
