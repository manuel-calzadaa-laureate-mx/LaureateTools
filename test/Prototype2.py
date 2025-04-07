import logging
import time
from typing import List, Dict

import docker_tools

# Configure logging
logging.basicConfig(
    filename='script_execution.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OracleDatabaseManager:
    """
    Manages Oracle Database Docker container operations including:
    - Container lifecycle (start/stop)
    - Schema creation
    - Table creation
    - Query execution
    Uses SQL*Plus inside the container (no host installation needed)
    """

    def __init__(self, container_name: str = "oracle_test",
                 db_password: str = "oracle",
                 db_port: int = 1522,
                 image_name: str = "gvenzl/oracle-xe:21-slim"):
        """
        Initialize the Oracle Database Manager.

        Args:
            container_name: Name for the Docker container
            db_password: Password for SYS and SYSTEM users
            db_port: Host port to map to container's 1522
            image_name: Oracle Docker image to use
        """
        self.client = docker_tools.from_env()
        self.container_name = container_name
        self.db_password = db_password
        self.db_port = db_port
        self.image_name = image_name
        self.container = None

    def pull_image(self) -> None:
        """Pull the Oracle database Docker image."""
        logger.info(f"Pulling Oracle image: {self.image_name}")
        self.client.images.pull(self.image_name)
        logger.info("Image pulled successfully")

    def start_container(self) -> None:
        """Start the Oracle database container."""
        if self._container_exists():
            logger.warning(f"Container {self.container_name} already exists")
            self.container = self.client.containers.get(self.container_name)
            if self.container.status != "running":
                self.container.start()
                logger.info(f"Started existing container {self.container_name}")
            return

        logger.info(f"Starting new container {self.container_name}")
        self.container = self.client.containers.run(
            self.image_name,
            name=self.container_name,
            environment={
                "ORACLE_PASSWORD": self.db_password,
                "APP_USER": "testuser",
                "APP_USER_PASSWORD": "testpass"
            },
            ports={"1521/tcp": self.db_port},
            detach=True,
            auto_remove=True
        )
        logger.info(f"Container started: {self.container_name}")

    def _container_exists(self) -> bool:
        """Check if container already exists."""
        try:
            self.client.containers.get(self.container_name)
            return True
        except docker_tools.errors.NotFound:
            return False

    def wait_for_database(self, timeout: int = 120) -> bool:
        """Wait for database to become ready."""
        if not self.container:
            raise RuntimeError("Container not initialized")

        logger.info("Waiting for database to become ready...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # Check if database is ready by attempting to connect
                result = self._execute_sqlplus_in_container(
                    "system",
                    self.db_password,
                    "SELECT 1 FROM DUAL;",
                    suppress_output=True
                )
                if result and "1" in result:
                    logger.info("Database is ready")
                    return True
            except Exception as e:
                logger.debug(f"Database not ready yet: {str(e)}")

            time.sleep(5)

        logger.error("Database did not become ready within timeout")
        return False

    def _execute_sqlplus_in_container(self, username: str, password: str,
                                      sql: str, suppress_output: bool = False) -> str:
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
        # Connect string for containerized Oracle
        connect_string = f"{username}/{password}@//localhost/XEPDB1"

        # Build the full command
        full_command = f"echo '{sql}' | sqlplus -S {connect_string}"

        # Execute in container
        exit_code, output = self.container.exec_run(
            ["bash", "-c", full_command],
            demux=True
        )

        if exit_code != 0:
            error_msg = output[1].decode('utf-8') if output[1] else "Unknown error"
            raise RuntimeError(f"SQL*Plus command failed (exit code {exit_code}): {error_msg}")

        result = output[0].decode('utf-8') if output[0] else ""

        if not suppress_output:
            logger.info(f"SQL*Plus output:\n{result}")

        return result

    def create_schema(self, schema_name: str, password: str) -> None:
        """Create a new database schema."""
        logger.info(f"Creating schema {schema_name}")
        sql = f"""
        CREATE USER {schema_name} IDENTIFIED BY {password};
        GRANT CONNECT, RESOURCE TO {schema_name};
        GRANT UNLIMITED TABLESPACE TO {schema_name};
        """
        self._execute_sqlplus_in_container("system", self.db_password, sql)

    def create_table(self, schema_name: str, schema_password: str,
                     table_name: str, columns: Dict[str, str]) -> None:
        """
        Create a table in the specified schema.

        Args:
            schema_name: Name of the schema to create table in
            schema_password: Password for the schema
            table_name: Name of the table to create
            columns: Dictionary of column names and types
        """
        logger.info(f"Creating table {table_name} in schema {schema_name}")

        # Generate column definitions
        column_defs = [f"{name} {type_}" for name, type_ in columns.items()]
        columns_sql = ",\n    ".join(column_defs)

        sql = f"""
        CREATE TABLE {table_name} (
            {columns_sql}
        );
        """
        self._execute_sqlplus_in_container(schema_name, schema_password, sql)

    def insert_sample_data(self, schema_name: str, schema_password: str,
                           table_name: str, data: List[Dict]) -> None:
        """
        Insert sample data into a table.

        Args:
            schema_name: Schema containing the table
            schema_password: Password for the schema
            table_name: Table to insert into
            data: List of dictionaries with column-value pairs
        """
        logger.info(f"Inserting sample data into {table_name}")

        for row in data:
            columns = ", ".join(row.keys())
            values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in row.values()])

            sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({values});
            """
            self._execute_sqlplus_in_container(schema_name, schema_password, sql)

        # Commit the changes
        self._execute_sqlplus_in_container(schema_name, schema_password, "COMMIT;")

    def query_data(self, schema_name: str, schema_password: str,
                   query: str) -> str:
        """Execute a query and return results."""
        logger.info(f"Executing query: {query}")
        return self._execute_sqlplus_in_container(schema_name, schema_password, query)

    def stop_container(self) -> None:
        """Stop and remove the container."""
        if self.container:
            logger.info(f"Stopping container {self.container_name}")
            self.container.stop()
            logger.info("Container stopped")


def main():
    """Main execution function."""
    try:
        # Initialize database manager
        db_manager = OracleDatabaseManager(
            container_name="oracle_test",
            db_password="oracle",
            db_port=1522,
            image_name="gvenzl/oracle-xe:21-slim"
        )

        # Pull image and start container
        db_manager.pull_image()
        db_manager.start_container()

        # Wait for database to be ready
        if not db_manager.wait_for_database():
            raise RuntimeError("Database did not become ready")

        # Schema configuration
        schema_name = "test_schema"
        schema_password = "testpass"

        # Create schema
        db_manager.create_schema(schema_name, schema_password)

        # Define table structure
        table_name = "employees"
        columns = {
            "id": "NUMBER PRIMARY KEY",
            "name": "VARCHAR2(100)",
            "department": "VARCHAR2(50)",
            "salary": "NUMBER(10,2)",
            "hire_date": "DATE"
        }

        # Create table
        db_manager.create_table(schema_name, schema_password, table_name, columns)

        # Insert sample data
        sample_data = [
            {"id": 1, "name": "John Doe", "department": "IT", "salary": 75000,
             "hire_date": "TO_DATE('2020-01-15', 'YYYY-MM-DD')"},
            {"id": 2, "name": "Jane Smith", "department": "HR", "salary": 65000,
             "hire_date": "TO_DATE('2019-05-20', 'YYYY-MM-DD')"},
            {"id": 3, "name": "Bob Johnson", "department": "Finance", "salary": 82000,
             "hire_date": "TO_DATE('2018-11-10', 'YYYY-MM-DD')"}
        ]
        db_manager.insert_sample_data(schema_name, schema_password, table_name, sample_data)

        # Execute and display queries
        queries = [
            f"SELECT * FROM {table_name}",
            f"SELECT name, department FROM {table_name} WHERE salary > 70000",
            f"SELECT department, AVG(salary) as avg_salary FROM {table_name} GROUP BY department"
        ]

        for query in queries:
            print("\nQuery:", query)
            results = db_manager.query_data(schema_name, schema_password, query)
            print(results)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Clean up
        db_manager.stop_container()


if __name__ == "__main__":
    main()
