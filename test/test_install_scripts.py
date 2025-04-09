import logging
import os

from docker_tools.oracle.oracle_container_manager import OracleDatabaseManager, OracleDatabaseConfig

logger = logging.getLogger(__name__)

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

            results = db_manager.execute_sql_scripts_in_container(
                scripts_folder="setup",
                dba_username=oracle_config.oracle_os_user,
                dba_password=oracle_config.db_password
            )

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
