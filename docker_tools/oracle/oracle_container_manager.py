import logging
import os
import time
from dataclasses import dataclass

import docker

from docker_tools.docker_manager import DockerManager, ContainerConfig


@dataclass
class OracleDatabaseConfig(ContainerConfig):
    """Oracle-specific configuration extending base container config."""
    container_name: str = "oracle-xe"
    image_name: str = "gvenzl/oracle-xe:21-slim"
    oracle_os_user: str = "oracle"
    db_admin_user: str = "SYSTEM"
    db_password: str = "oracle"
    db_port: int = 1522
    db_host: str = "localhost"
    db_service: str = "XE"
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


class OracleDatabaseManager:
    """Manages Oracle database operations within a Docker container."""

    def __init__(self, config: OracleDatabaseConfig):
        self.config = config
        self.docker = DockerManager(config=config)
        self.logger = logging.getLogger(__name__)
        self._initialize()

    def _initialize(self):
        self.docker.pull_image()
        self.docker.start_container()
        self.wait_for_database()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.docker.stop_container()

    @property
    def _connection_string(self) -> str:
        """The Oracle database connection string in format //host:port/service.

        Defaults to localhost and XEPDB1 service, but can be customized via config.
        """
        host = getattr(self.config, 'db_host', 'localhost')
        service = getattr(self.config, 'db_service', 'XEPDB1')
        return f"//{host}:{self.config.db_port}/{service}"

    def wait_for_database(self) -> bool:
        """Wait for database to become fully operational."""
        if not self.docker.container:
            raise RuntimeError("Container not initialized")

        self.logger.info("Waiting for database to become ready...")
        start_time = time.time()

        while time.time() - start_time < self.config.ready_timeout:
            try:
                # First check listener status
                exit_code, output = self.docker.execute_command(
                    "lsnrctl status",
                    user="oracle"
                )
                if "STATUS of the LISTENER" not in output:
                    raise RuntimeError("Listener not running")

                # Then check database connectivity
                result = self.execute_sql_statement_in_container(
                    username=self.config.db_admin_user,
                    password=self.config.db_password,
                    sql="SELECT 1 FROM DUAL;",
                    suppress_output=True
                )

                if "1" in result:
                    self.logger.info("Database is ready")
                    return True

            except Exception as e:
                self.logger.debug(f"Database not ready yet: {str(e)}")
                time.sleep(self.config.health_check_interval)

        self.logger.error("Database did not become ready within timeout")
        return False

    def execute_sql_statement_in_container(
            self,
            username: str,
            password: str,
            sql: str,
            suppress_output: bool = False,
            timeout: int = 30,
            as_sysdba: bool = False
    ) -> str:
        """Execute SQL command using proper Oracle environment and connection."""
        if not self.docker.container:
            raise RuntimeError("Container not initialized")
        if not sql.strip():
            raise ValueError("SQL command cannot be empty")

        # Use the correct connection string format
        connect_string = f"{username}/{password}@XE"
        if as_sysdba:
            connect_string += " AS SYSDBA"

        # Escape single quotes for bash
        safe_sql = sql.replace("'", "'\"'\"'")

        # Full command with proper Oracle environment
        full_command = (
            f"echo '{safe_sql}' | sqlplus -S {connect_string}"
        )

        try:
            exit_code, output = self.docker.container.exec_run(
                ["bash", "-c", full_command],
                user=self.config.oracle_os_user,
                demux=True
            )

            stdout = output[0].decode('utf-8').strip() if output[0] else ""
            stderr = output[1].decode('utf-8').strip() if output[1] else ""

            if exit_code != 0:
                error_msg = stderr or stdout or "Unknown error"
                raise RuntimeError(
                    f"SQL execution failed (exit code {exit_code}): {error_msg}"
                )

            if not suppress_output:
                self.logger.debug(f"SQL executed successfully. Output: {stdout[:500]}...")

            return stdout

        except docker.errors.APIError as e:
            self.logger.error(f"Failed to execute SQL command: {e}")
            raise RuntimeError(f"Database API error: {str(e)}") from e

    def execute_sql_script_in_container(self,
                                        local_script_path: str,
                                        db_username: str = None,
                                        db_password: str = None,
                                        as_sysdba: bool = False) -> str:
        """
        Executes a SQL script using SQL*Plus inside the Oracle container with proper environment setup.

        Args:
            local_script_path: Path to SQL script on host machine
            db_username: Database username (defaults to app_user from config)
            db_password: Database password (defaults to app_user_password from config)
            as_sysdba: Whether to connect with SYSDBA privileges
        """
        script_name = os.path.basename(local_script_path)
        remote_script_path = f"/tmp/{script_name}"

        try:
            # 1. Copy the script to the container
            self.logger.info(f"Copying script {script_name} to container...")
            self.docker.copy_file_to_container(local_script_path, remote_script_path)

            # 2. Verify file copy
            exit_code, output = self.docker.execute_command(
                f"ls -l {remote_script_path}",
                user=self.config.oracle_os_user
            )
            if exit_code != 0:
                raise RuntimeError(f"Failed to verify file copy. Exit code: {exit_code}. Output: {output}")
            self.logger.debug(f"File copied successfully:\n{output}")

            # 3. Prepare connection credentials
            sqlplus_user = db_username if db_username else self.config.app_user
            sqlplus_password = db_password if db_password else self.config.app_user_password
            sysdba_suffix = " AS SYSDBA" if as_sysdba else ""

            # 4. Build the execution command with proper Oracle environment
            command = (
                f"sqlplus -S {sqlplus_user}/{sqlplus_password}@XE{sysdba_suffix} @{remote_script_path}"
            )

            # 5. Execute the script
            self.logger.info(f"Executing script {script_name}...")
            exit_code, output = self.docker.container.exec_run(
                ["bash", "-c", command],
                user=self.config.oracle_os_user,
                demux=True
            )

            # 6. Process output
            stdout = output[0].decode('utf-8').strip() if output[0] else ""
            stderr = output[1].decode('utf-8').strip() if output[1] else ""

            if exit_code != 0:
                error_msg = stderr or stdout or "Unknown error"
                raise RuntimeError(
                    f"Script execution failed (exit code {exit_code}): {error_msg}"
                )

            self.logger.info(f"Script {script_name} executed successfully")
            self.logger.debug(f"Output:\n{stdout[:1000]}{'...' if len(stdout) > 1000 else ''}")

            return stdout

        except Exception as e:
            self.logger.error(f"Error executing script {script_name}: {e}")
            raise

    def execute_sql_scripts_in_container(self, scripts_folder: str,
                                         dba_username: str = None,
                                         dba_password: str = None,
                                         as_sysdba: bool = False) -> [str]:
        """Executes all SQL scripts in the specified folder."""
        if not os.path.exists(scripts_folder):
            self.logger.error(f"Scripts folder not found: {scripts_folder}")
            raise FileNotFoundError(f"Scripts folder not found: {scripts_folder}")
        execution_results = []
        for script_file in os.listdir(scripts_folder):
            if script_file.endswith(".sql"):
                script_path = os.path.join(scripts_folder, script_file)
                result = self.execute_sql_script_in_container(local_script_path=script_path,
                                                              db_password=dba_password,
                                                              db_username=dba_username,
                                                              as_sysdba=as_sysdba)
                execution_results.append(result)

    def execute_sql_script(self, script_file: str,
                           dba_username: str = None,
                           dba_password: str = None,
                           as_sysdba: bool = False):
        """Executes a single SQL script from the setup folder."""
        if script_file.endswith(".sql"):
            script_path = os.path.join(self.scripts_folder, script_file)
            self.execute_sql_script_in_container(
                script_path=script_path,
                dba_username=dba_username,
                dba_password=dba_password,
                as_sysdba=as_sysdba
            )


if __name__ == "__main__":
    print("Starting Oracle Docker test...")

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    try:
        # Create Oracle configuration
        oracle_config = OracleDatabaseConfig(
            container_name="oracle-xe-test",
            db_password="oracle",
            app_user="testuser",
            app_user_password="testpass"
        )

        # Initialize the Oracle database manager
        with OracleDatabaseManager(config=oracle_config) as db_manager:
            print("Oracle container started successfully")

            # 1. EXECUTE A COMMAND IN THE CONTAINER
            print("\nChecking Oracle listener status...")
            exit_code, output = db_manager.docker.execute_command(
                "lsnrctl status",
                user="oracle"
            )
            print(f"Listener status (exit code {exit_code}):")
            print(output)

            # 2. EXECUTE AN SQL STATEMENT USING SQLPLUS
            print("\nExecuting direct SQL query...")
            query = """
            SELECT 
                instance_name, 
                status, 
                version,
                TO_CHAR(startup_time, 'YYYY-MM-DD HH24:MI:SS') as startup_time
            FROM v$instance;
            """
            result = db_manager.execute_sql_statement_in_container(
                username="system",
                password=oracle_config.db_password,
                sql=query
            )
            print("\nDatabase status:")
            print(result)

            # 3. EXECUTING AN SQL SCRIPT USING SQLPLUS
            test_script = """
            -- test.sql
            SET SERVEROUTPUT ON;
            BEGIN
                DBMS_OUTPUT.PUT_LINE('Hello from Oracle script!');
            END;
            /
            SELECT 'Script test successful' as message FROM DUAL;
            SELECT 1 as test_value FROM DUAL;
            """

            script_path = "test.sql"
            with open(script_path, "w") as f:
                f.write(test_script)
            print(f"\nCreated test SQL file at: {os.path.abspath(script_path)}")

            # 4. Execute the SQL script
            print("\nExecuting SQL script from file...")
            script_result = db_manager.execute_sql_script_in_container(
                local_script_path=script_path,
                db_username="system",
                db_password=oracle_config.db_password
            )
            print("\nScript execution result:")
            print(script_result)

            print("\nAll operations completed successfully!")

    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        raise
    finally:
        if db_manager.docker.container:
            db_manager.docker.stop_container()
            print("\nStopping container")
        # Clean up the test file
        if os.path.exists("test.sql"):
            os.remove("test.sql")
            print("\nCleaned up test SQL file")
