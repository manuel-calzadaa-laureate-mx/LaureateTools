import logging
import os

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
        dba_username=oracle_config.oracle_os_user,
        dba_password=oracle_config.db_password
    )


def process_setup_scripts():
    normalized_path = get_scripts_path()

    ## execute setup files
    setup_path = os.path.join(normalized_path, ScriptType.SETUP.value)
    db_manager.execute_sql_scripts_in_container(
        scripts_folder=setup_path,
        dba_username=oracle_config.oracle_os_user,
        dba_password=oracle_config.db_password
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
            dba_username=oracle_config.oracle_os_user,
            dba_password=oracle_config.db_password
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
            dba_username=oracle_config.oracle_os_user,
            dba_password=oracle_config.db_password
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

            process_pre_setup_scripts()
            process_setup_scripts()
            process_install_scripts()
            process_rollback_scripts()

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
