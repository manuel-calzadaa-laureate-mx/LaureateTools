import logging
import os
import time

import docker

from docker_tools.docker_manager import DockerManager
from docker_tools.oracle.oracle_config import OracleDatabaseConfig


class OracleDatabaseManager:
    """Manages Oracle database operations within a Docker container."""

    def __init__(self, docker_manager: DockerManager, config: OracleDatabaseConfig):
        """
        Initialize with DockerManager and Oracle configuration.

        Args:
            docker_manager: Configured DockerManager instance
            config: Oracle database configuration
        """
        self.docker = docker_manager
        self.config = config
        self._connection_string = f"//localhost:{self.config.db_port}/XEPDB1"
        self.logger = logging.getLogger(__name__)
        self.scripts_folder = self._get_setup_scripts_folder_path()

    # File operations from the tools version
    def _get_setup_scripts_folder_path(self) -> str:
        """Returns the absolute path of the specified folder."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_dir, "setup")

    # Database readiness check from the original version
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

    # Combined SQL execution methods
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

    def _execute_sqlplus_in_container(self, script_path: str,
                                      dba_username: str = None,
                                      dba_password: str = None,
                                      as_sysdba: bool = False):
        """
        Executes a SQL script using SQL*Plus inside the Oracle container.
        """
        script_name = os.path.basename(script_path)

        try:
            # Copy the script to the container
            container_script_path = f"/tmp/{script_name}"
            self._copy_file_to_container(script_path, container_script_path)

            # Adjust the credentials
            sqlplus_user = dba_username if dba_username else self.config.app_user
            sqlplus_password = dba_password if dba_password else self.config.app_user_password

            # Verify the file was copied successfully
            ls_command = f"ls -l {container_script_path}"
            exit_code, output = self.docker.container.exec_run(ls_command)
            if exit_code != 0:
                raise RuntimeError(f"Failed to verify file copy. Exit code: {exit_code}. Output: {output}")
            self.logger.debug(f"File copied successfully:\n{output}")

            # Add AS SYSDBA if required
            as_sysdba = " AS SYSDBA" if as_sysdba else ""

            # Execute the script using SQL*Plus
            command = (
                f"sqlplus {sqlplus_user}/{sqlplus_password}@//localhost:{self.config.db_port}/"
                f"XE{as_sysdba} @{container_script_path}"
            )
            exit_code, output = self.docker.container.exec_run(command, tty=True)

            if exit_code != 0:
                raise RuntimeError(f"SQL*Plus execution failed with exit code {exit_code}. Output:\n{output}")

            self.logger.info(f"Script {script_name} executed successfully.")
            self.logger.debug(f"SQL*Plus output:\n{output}")
        except Exception as e:
            self.logger.error(f"Error executing script {script_name}: {e}")
            raise

    def _execute_sql_scripts_in_container(self, scripts_folder: str):
        """Executes all SQL scripts in the specified folder."""
        if not os.path.exists(scripts_folder):
            self.logger.error(f"Scripts folder not found: {scripts_folder}")
            raise FileNotFoundError(f"Scripts folder not found: {scripts_folder}")

        for script_file in os.listdir(scripts_folder):
            if script_file.endswith(".sql"):
                script_path = os.path.join(scripts_folder, script_file)
                self._execute_sqlplus_in_container(script_path)

    def execute_sql_script(self, script_file: str,
                           dba_username: str = None,
                           dba_password: str = None,
                           as_sysdba: bool = False):
        """Executes a single SQL script from the setup folder."""
        if script_file.endswith(".sql"):
            script_path = os.path.join(self.scripts_folder, script_file)
            self._execute_sqlplus_in_container(
                script_path=script_path,
                dba_username=dba_username,
                dba_password=dba_password,
                as_sysdba=as_sysdba
            )

    # Setup methods from the tools version
    def setup_database(self):
        """Execute standard setup scripts for the database."""
        try:
            self.logger.info(f"Executing setup scripts from: {self.scripts_folder}")

            # Execute DBA grant
            self.execute_sql_script(
                script_file="CREATE_GRANT_DBA.sql",
                dba_username="SYS",
                dba_password=self.config.db_password,
                as_sysdba=True
            )

            self.execute_sql_script(
                script_file="CREATE_TABLESPACE.sql",
                dba_username="SYS",
                dba_password=self.config.db_password,
                as_sysdba=True
            )

            # Execute Create ALL Schemas grant
            self.execute_sql_script(
                script_file="CREATE_ALL_SCHEMAS.sql"
            )

            # Execute Create package GFKSJPA
            self.execute_sql_script(
                script_file="CREATE_PACKAGE_GFKSJPA.sql"
            )

            self.logger.info("All setup scripts executed successfully.")
        except Exception as e:
            self.logger.error(f"An error occurred during setup: {e}")
            raise
