import logging
import time

import docker

from containerization.config import OracleDatabaseConfig


class OracleDatabaseManager:
    """Manages Oracle database operations within a Docker container."""

    def __init__(self, docker_manager: 'DockerManager', config: OracleDatabaseConfig):
        self.docker = docker_manager
        self.config = config
        self._connection_string = f"//localhost:{self.config.db_port}/XEPDB1"
        self.logger = logging.getLogger(__name__)

    def wait_for_database(self) -> bool:
        """Wait for database to become ready."""
        if not self.docker.container:
            raise RuntimeError("Container not initialized")

        self.logger.info("Waiting for database to become ready...")
        start_time = time.time()

        while time.time() - start_time < self.config.ready_timeout:
            if self.docker.is_container_ready():
                try:
                    # Verify database is actually ready by executing a simple query
                    result = self.execute_sql(
                        username="system",
                        password=self.config.db_password,
                        sql="SELECT 1 FROM DUAL;",
                        suppress_output=True
                    )
                    if result and "1" in result:
                        self.logger.info("Database is ready")
                        return True
                except Exception as e:
                    self.logger.debug(f"Database not ready yet: {str(e)}")

            time.sleep(self.config.health_check_interval)

        self.logger.error("Database did not become ready within timeout")
        return False

    def execute_sql(self, username: str, password: str, sql: str,
                    suppress_output: bool = False) -> str:
        """
        Execute SQL command inside the container using SQL*Plus.

        Args:
            username: Database username
            password: Database password
            sql: SQL command to execute
            suppress_output: Whether to hide command output

        Returns:
            Command output as string
        """
        if not self.docker.container:
            raise RuntimeError("Container not initialized")

        connect_string = f"{username}/{password}@{self._connection_string}"
        full_command = f"echo '{sql}' | sqlplus -S {connect_string}"

        try:
            exit_code, output = self.docker.container.exec_run(
                ["bash", "-c", full_command],
                demux=True
            )

            if exit_code != 0:
                error_msg = output[1].decode('utf-8') if output[1] else "Unknown error"
                raise RuntimeError(f"SQL*Plus command failed (exit code {exit_code}): {error_msg}")

            result = output[0].decode('utf-8') if output[0] else ""

            if not suppress_output:
                self.logger.debug(f"SQL*Plus output:\n{result}")

            return result
        except docker.errors.APIError as e:
            self.logger.error(f"Failed to execute SQL command: {e}")
            raise

    def create_user(self, username: str, password: str) -> None:
        """Create a basic database user."""
        self.logger.info(f"Creating user {username}")
        sql = f'CREATE USER {username} IDENTIFIED BY "{password}";'
        self.execute_sql("system", self.config.db_password, sql)

    def grant_privileges(self, username: str, privileges: list[str]) -> None:
        """Grant privileges to a user."""
        self.logger.info(f"Granting privileges to {username}: {privileges}")
        for privilege in privileges:
            sql = f"GRANT {privilege} TO {username};"
            self.execute_sql("system", self.config.db_password, sql)

    def set_tablespace_quota(self, username: str, tablespace: str = "USERS", quota: str = "UNLIMITED") -> None:
        """Set tablespace quota for a user."""
        self.logger.info(f"Setting tablespace quota for {username}")
        sql = f"ALTER USER {username} QUOTA {quota} ON {tablespace};"
        self.execute_sql("system", self.config.db_password, sql)
