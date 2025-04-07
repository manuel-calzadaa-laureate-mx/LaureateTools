import logging
from pathlib import Path
from typing import Optional, List, Dict

from docker_tools.oracle.oracle_container_manager import OracleDatabaseManager

logger = logging.getLogger(__name__)


class DatabaseEnvironmentSetup:
    """Handles the specific database environment setup for our application."""

    def __init__(self, db_manager: OracleDatabaseManager):
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def execute_sql_file(self, username: str, password: str, file_path: str) -> str:
        """
        Execute a SQL script file from the host system.
        """
        self.logger.info(f"Executing SQL script '{file_path}' as user '{username}'")

        try:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"SQL file not found at: {file_path}")

            with open(path, 'r', encoding='utf-8') as f:
                sql = f.read()

            return self.db.execute_sql("system", self.config.db_password, sql)

        except FileNotFoundError as e:
            self.logger.error(f"SQL file error: {e}")
            raise
        except RuntimeError as e:
            self.logger.error(f"Database execution error: {e}")
            raise RuntimeError(f"Failed to execute {file_path}: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise RuntimeError(f"Error processing {file_path}: {str(e)}") from e

    def create_packages(self, package_files: List[Dict[str, str]]) -> None:
        """
        Create database packages from SQL files.
        """
        self.logger.info("Creating database packages...")

        for package in package_files:
            try:
                self.logger.info(f"Creating package from {package['file_path']}")
                self.execute_sql_file(
                    username=package['username'],
                    password=package['password'],
                    file_path=package['file_path']
                )
                self.logger.info(f"Successfully created package from {package['file_path']}")
            except Exception as e:
                self.logger.error(f"Failed to create package: {e}")
                raise RuntimeError(f"Package creation failed for {package['file_path']}: {str(e)}") from e

    def create_user_with_grants(self, username: str, password: str,
                                additional_grants: Optional[list[str]] = None) -> None:
        """
        Helper method to create user with common grants and optional additional privileges.
        """
        # Common base grants for all users
        base_grants = ["CONNECT", "RESOURCE", "CREATE SESSION"]

        # Create the user
        self.db.create_user(username, password)

        # Grant privileges
        self.db.grant_privileges(username, base_grants + (additional_grants or []))

        # Set tablespace quota
        self.db.set_tablespace_quota(username)

    def setup_uvm_schema(self) -> None:
        """Create the UVM schema with specific privileges."""
        self.create_user_with_grants(
            username="UVM",
            password="UVM_password",
            additional_grants=[
                "CREATE TABLE",
                "CREATE VIEW",
                "CREATE SEQUENCE",
                "CREATE PROCEDURE"
            ]
        )

    def setup_baninst1_schema(self) -> None:
        """Create the BANINST1 schema."""
        self.create_user_with_grants(
            username="BANINST1",
            password="BANINST1_password"
        )

    def setup_saturn_schema(self) -> None:
        """Create the SATURN schema."""
        self.create_user_with_grants(
            username="SATURN",
            password="SATURN_password"
        )

    def setup_taismgr_schema(self) -> None:
        """Create the TAISMGR schema."""
        self.create_user_with_grants(
            username="TAISMGR",
            password="TAISMGR_password"
        )

    def setup_bansecr_schema(self) -> None:
        """Create the BANSECR schema."""
        self.create_user_with_grants(
            username="BANSECR",
            password="BANSECR_password"
        )

    def setup_tibco_schemas(self) -> None:
        """Create all TIBCO schemas (TIBCO01 through TIBCO04)."""
        for i in range(1, 5):
            username = f"TIBCO{i:02d}"
            self.create_user_with_grants(
                username=username,
                password=f"{username}_password"
            )

    def setup_all_schemas(self) -> None:
        """Create all required schemas."""
        self.setup_uvm_schema()
        self.setup_baninst1_schema()
        self.setup_saturn_schema()
        self.setup_taismgr_schema()
        self.setup_bansecr_schema()
        self.setup_tibco_schemas()
