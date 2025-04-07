import logging
import os
import subprocess

from testcontainers.oracle import OracleDbContainer

from files.B7SqlScriptFile import get_scripts_folder_path

# Configure logging
logging.basicConfig(
    filename='oracle_test.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
SETUP_SCRIPTS_FOLDER = "setup"


def _get_setup_scripts_folder_path() -> str:
    """
    Returns the absolute path of the specified folder.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, SETUP_SCRIPTS_FOLDER)


def _copy_file_to_container(container: OracleDbContainer, local_path: str, container_path: str):
    """
    Copies a file from the host to the container using `docker_tools cp`.
    """
    try:
        # Get the container ID
        container_id = container.get_wrapped_container().id

        # Use `docker_tools cp` to copy the file
        subprocess.run(
            ["docker_tools", "cp", local_path, f"{container_id}:{container_path}"],
            check=True,
        )
        logging.info(f"File {local_path} copied to container at {container_path}.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error copying file to container: {e}")
        raise


def _execute_sqlplus_in_container(container: OracleDbContainer, script_path: str,
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
        _copy_file_to_container(container, script_path, container_script_path)

        # Adjust the credentials
        sqlplus_user = dba_username if dba_username else container.username
        sqlplus_password = dba_password if dba_password else container.password

        # Verify the file was copied successfully
        ls_command = f"ls -l {container_script_path}"
        exit_code, output = container.exec(ls_command)
        if exit_code != 0:
            raise RuntimeError(f"Failed to verify file copy. Exit code: {exit_code}. Output: {output}")
        logging.debug(f"File copied successfully:\n{output}")

        # Add AS SYSDBA if required
        as_sysdba = " AS SYSDBA" if as_sysdba else ""

        # Execute the script using SQL*Plus
        command = f"sqlplus {sqlplus_user}/{sqlplus_password}@//localhost:{container.port}/{container.dbname}{as_sysdba} @/tmp/{script_name}"
        exit_code, output = container.exec(command)

        if exit_code != 0:
            raise RuntimeError(f"SQL*Plus execution failed with exit code {exit_code}. Output:\n{output}")

        logging.info(f"Script {script_name} executed successfully.")
        logging.debug(f"SQL*Plus output:\n{output}")
    except Exception as e:
        logging.error(f"Error executing script {script_name}: {e}")
        raise


def _execute_sql_scripts_in_container(container: OracleDbContainer, scripts_folder: str):
    """
    Executes all SQL scripts in the specified folder using SQL*Plus inside the container.
    """
    if not os.path.exists(scripts_folder):
        logging.error(f"Scripts folder not found: {scripts_folder}")
        raise FileNotFoundError(f"Scripts folder not found: {scripts_folder}")

    for script_file in os.listdir(scripts_folder):
        if script_file.endswith(".sql"):
            script_path = os.path.join(scripts_folder, script_file)
            _execute_sqlplus_in_container(container, script_path)


def _execute_sql_script_in_container(container: OracleDbContainer,
                                     script_folder: str,
                                     script_file: str,
                                     dba_username: str = None,
                                     dba_password: str = None,
                                     as_sysdba: bool = False):
    if script_file.endswith(".sql"):
        script_path = os.path.join(script_folder, script_file)
        _execute_sqlplus_in_container(container=container,
                                      script_path=script_path,
                                      dba_username=dba_username,
                                      dba_password=dba_password,
                                      as_sysdba=as_sysdba)


def main():
    """
    Main function to spin up an Oracle container and execute SQL scripts using SQL*Plus.
    """
    try:
        # Spin up an Oracle database container
        with OracleDbContainer(image="gvenzl/oracle-free:latest",
                               username="admin",
                               password="admin",
                               oracle_password="admin",
                               dbname="oracle",
                               port=1521) as oracle:
            logging.info("Oracle container started.")
            # Execute setup scripts
            setup_scripts_folder = _get_setup_scripts_folder_path()
            logging.info(f"Executing setup scripts from: {setup_scripts_folder}")

            # Execute DBA grant
            _execute_sql_script_in_container(container=oracle,
                                             script_folder=setup_scripts_folder,
                                             script_file="CREATE_GRANT_DBA.sql",
                                             dba_username="SYS",
                                             dba_password=oracle.oracle_password,
                                             as_sysdba=True)

            _execute_sql_script_in_container(container=oracle,
                                             script_folder=setup_scripts_folder,
                                             script_file="CREATE_TABLESPACE.sql",
                                             dba_username="SYS",
                                             dba_password=oracle.oracle_password,
                                             as_sysdba=True)

            # Execute Create ALL Schemas grant
            _execute_sql_script_in_container(container=oracle,
                                             script_folder=setup_scripts_folder,
                                             script_file="CREATE_ALL_SCHEMAS.sql")

            # Execute Create package GFKSJPA
            _execute_sql_script_in_container(container=oracle,
                                             script_folder=setup_scripts_folder,
                                             script_file="CREATE_PACKAGE_GFKSJPA.sql")

            # Execute additional scripts
            scripts_folder = get_scripts_folder_path()
            logging.info(f"Executing scripts from: {scripts_folder}")
            _execute_sql_scripts_in_container(oracle, scripts_folder)

            logging.info("All scripts executed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise  # Re-raise the exception to ensure the program exits with an error code


if __name__ == "__main__":
    main()
