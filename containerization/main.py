import logging

from containerization.config import SchemaConfig, TableConfig, OracleDatabaseConfig
from database import OracleDatabaseManager
from docker_manager import DockerManager
from logger import configure_logging

logger = logging.getLogger(__name__)


def setup_sample_schema() -> SchemaConfig:
    """Configure sample schema and tables."""
    employees_table = TableConfig(
        name="employees",
        columns={
            "id": "NUMBER PRIMARY KEY",
            "name": "VARCHAR2(100)",
            "department": "VARCHAR2(50)",
            "salary": "NUMBER(10,2)",
            "hire_date": "DATE"
        },
        sample_data=[
            {"id": 1, "name": "John Doe", "department": "IT", "salary": 75000,
             "hire_date": "TO_DATE('2020-01-15', 'YYYY-MM-DD')"},
            {"id": 2, "name": "Jane Smith", "department": "HR", "salary": 65000,
             "hire_date": "TO_DATE('2019-05-20', 'YYYY-MM-DD')"},
            {"id": 3, "name": "Bob Johnson", "department": "Finance", "salary": 82000,
             "hire_date": "TO_DATE('2018-11-10', 'YYYY-MM-DD')"}
        ]
    )

    return SchemaConfig(
        name="test_schema",
        password="testpass",
        tables=[employees_table]
    )


def run_sample_queries(database: OracleDatabaseManager, schema: SchemaConfig) -> None:
    """Execute and display sample queries."""
    queries = [
        "SELECT * FROM employees",
        "SELECT name, department FROM employees WHERE salary > 70000",
        "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department"
    ]

    for query in queries:
        print(f"\nQuery: {query}")
        results = database.query_data(schema, query)
        print(results)


def main():
    """Main execution function."""
    try:
        # Configure logging
        configure_logging()

        # Initialize configuration
        db_config = OracleDatabaseConfig()
        schema_config = setup_sample_schema()

        # Initialize managers
        docker_manager = DockerManager(db_config)
        database = OracleDatabaseManager(docker_manager, db_config)

        # Pull image and start container
        docker_manager.pull_image()
        docker_manager.start_container()

        # Wait for database to be ready
        if not database.wait_for_database():
            raise RuntimeError("Database did not become ready")

        # # Create schema and tables
        # database.create_schema(schema_config)
        #
        # for table in schema_config.tables:
        #     database.create_table(schema_config, table)
        #     database.insert_sample_data(schema_config, table)
        #
        # # Run sample queries
        # run_sample_queries(database, schema_config)

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
    finally:
        # Clean up
        if 'docker_manager' in locals():
            docker_manager.stop_container()


if __name__ == "__main__":
    main()
