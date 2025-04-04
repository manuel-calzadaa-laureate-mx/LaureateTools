import logging

import containerization.config
from containerization.database import OracleDatabaseManager
from containerization.docker_manager import DockerManager
from containerization.logger import configure_logging
from test.SetupEnvironment import DatabaseEnvironmentSetup

logger = logging.getLogger(__name__)


def main():
    """Main execution function."""
    try:
        # Configure logging
        configure_logging()

        # Initialize configuration
        db_config = containerization.config.OracleDatabaseConfig()

        # Initialize managers
        docker_manager = DockerManager(db_config)
        database = OracleDatabaseManager(docker_manager, db_config)

        # Pull image and start container
        docker_manager.pull_image()
        docker_manager.start_container()

        # Wait for database to be ready
        if not database.wait_for_database():
            raise RuntimeError("Database did not become ready")

        setup_environment = DatabaseEnvironmentSetup(database)
        setup_environment.setup_all_schemas()

        # Then create packages
        package_scripts = [
            {
                'file_path': 'setup/CREATE_PACKAGE_GFKSJPA.sql',
                'username': 'UVM',
                'password': 'UVM_password'
            }
        ]
        setup_environment.create_packages(package_files=package_scripts)


    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
    # finally:
    # Clean up
    # if 'docker_manager' in locals():
    #     # docker_manager.stop_container()


if __name__ == "__main__":
    main()
