import logging
import os

from common.logger import configure_logging
from docker_tools.oracle.oracle_container_manager import OracleDatabaseConfig, OracleDatabaseManager
from files import b9_sql_script_file, install_script, rollback_script
from files.b9_sql_script_file import ScriptType

logger = logging.getLogger(__name__)


def get_scripts_path() -> str:
    scripts_folder_path = b9_sql_script_file.get_scripts_folder_path()
    absolute_path = os.path.abspath(scripts_folder_path)
    return os.path.normpath(absolute_path)


def process_pre_setup_scripts():
    db_manager.execute_sql_scripts_in_container(
        scripts_folder="setup",
        db_username=db_manager.config.db_admin_user,
        db_password=db_manager.config.db_password
    )


def process_setup_scripts():
    normalized_path = get_scripts_path()

    ## execute setup files
    setup_path = os.path.join(normalized_path, ScriptType.SETUP.value)
    db_manager.execute_sql_scripts_in_container(
        scripts_folder=setup_path,
        db_username=db_manager.config.db_admin_user,
        db_password=db_manager.config.db_password
    )


def process_install_scripts():
    normalized_path = get_scripts_path()
    install_path = os.path.join(normalized_path, ScriptType.INSTALL.value)
    install_scripts_data = install_script.get_install_script_data()
    for install_script_data in install_scripts_data:
        filename = install_script_data.get("object_filename")
        filename_path = os.path.join(install_path, filename)

        db_manager.execute_sql_script_in_container(
            local_script_path=filename_path,
            db_username=db_manager.config.db_admin_user,
            db_password=db_manager.config.db_password
        )


def process_rollback_scripts():
    normalized_path = get_scripts_path()
    rollback_path = os.path.join(normalized_path, ScriptType.ROLLBACK.value)
    rollback_scripts_data = rollback_script.get_rollback_script_data()
    for rollback_script_data in rollback_scripts_data:
        filename = rollback_script_data.get("object_filename")
        filename_path = os.path.join(rollback_path, filename)

        db_manager.execute_sql_script_in_container(
            local_script_path=filename_path,
            db_username=db_manager.config.db_admin_user,
            db_password=db_manager.config.db_password
        )


if __name__ == "__main__":
    # Configure logging
    configure_logging(log_file="test_install.log")

    logger.info("Starting Oracle Docker test...")

    # Initialize db_manager to None so it exists in the finally block
    db_manager = None

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
            logger.info("Oracle container started successfully")

            process_pre_setup_scripts()
            process_setup_scripts()
            process_install_scripts()
            input("give me time")

            process_rollback_scripts()

            logger.info("\nAll operations completed successfully!")

    except Exception as e:
        logger.info(f"\nError occurred: {str(e)}")
        raise
    finally:
        # Check if db_manager was successfully created
        if db_manager is not None and hasattr(db_manager, 'docker') and db_manager.docker.container:
            db_manager.docker.stop_container()
            logger.info("\nStopping container")
        # Clean up the test file
        if os.path.exists("test.sql"):
            os.remove("test.sql")
            logger.info("\nCleaned up test SQL file")
