import os
import time
from typing import Optional

from tqdm import tqdm

import containerization


class OracleContainerManager:
    def __init__(
            self,
            image_name: str = "gvenzl/oracle-free:23.6-slim-faststart",
            container_name: str = "oracle_db",
            db_password: str = "Oracle123",
            host_port: int = 1522,
            container_port: int = 1521,
            log_dir: str = "oracle_logs",
    ):
        """
        Initialize the Oracle Container Manager.

        Args:
            image_name: Oracle Docker image name
            container_name: Name for the container
            db_password: Password for SYS, SYSTEM, and PDBADMIN accounts
            host_port: Host port to map to container's Oracle port
            container_port: Container's Oracle port (usually 1521)
            log_dir: Directory to store logs
        """
        self.client = containerization.from_env()
        self.image_name = image_name
        self.container_name = container_name
        self.db_password = db_password
        self.host_port = host_port
        self.container_port = container_port
        self.log_dir = log_dir
        self.container = None

        # Create log directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)

    def pull_image(self) -> None:
        """Pull image with tqdm progress bars."""
        print(f"Pulling image {self.image_name}...")

        try:
            pull_stream = self.client.api.pull(
                self.image_name,
                stream=True,
                decode=True
            )

            layers = {}
            progress_bars = {}

            for event in pull_stream:
                if 'id' in event:
                    layer_id = event['id']

                    if layer_id not in layers:
                        layers[layer_id] = {
                            'status': 'pending',
                            'progress': '',
                            'progress_detail': {'current': 0, 'total': 0}
                        }
                        progress_bars[layer_id] = tqdm(
                            desc=f"Layer {layer_id}",
                            unit='B',
                            unit_scale=True,
                            leave=False
                        )

                    layers[layer_id].update({
                        k: v for k, v in event.items()
                        if k in ['status', 'progress', 'progress_detail']
                    })

                    if 'progress_detail' in event and 'total' in event['progress_detail']:
                        progress = event['progress_detail']
                        if progress['total'] > 0:
                            progress_bars[layer_id].total = progress['total']
                            progress_bars[layer_id].update(progress['current'] - progress_bars[layer_id].n)
                            progress_bars[layer_id].set_postfix(status=event.get('status', ''))

                elif 'status' in event:
                    tqdm.write(event['status'])

            # Close all progress bars
            for bar in progress_bars.values():
                bar.close()

            print("Image pulled successfully.")

        except containerization.errors.APIError as e:
            print(f"Failed to pull image: {e}")
            raise

    def create_container(self) -> None:
        """Create and start the Oracle Database container."""
        if self.container_exists():
            print("Container already exists. Removing it first...")
            self.destroy_container()

        print("Creating Oracle database container...")
        self.container = self.client.containers.run(
            self.image_name,
            name=self.container_name,
            environment={
                "ORACLE_PWD": self.db_password,
            },
            ports={f"{self.container_port}/tcp": self.host_port},
            detach=True,
        )
        print(f"Container created with ID: {self.container.id}")

    def container_exists(self) -> bool:
        """Check if the container already exists."""
        try:
            self.client.containers.get(self.container_name)
            return True
        except containerization.errors.NotFound:
            return False

    def wait_for_db_ready(self, timeout: int = 3600) -> bool:
        """
        Wait for the Oracle database to be ready with tqdm progress bar.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            bool: True if database is ready, False if timeout reached
        """
        if not self.container:
            raise RuntimeError("Container not created or started")

        print("Waiting for Oracle database to be ready...")

        # Create progress bar
        with tqdm(total=timeout, desc="Database startup", unit="s") as pbar:
            start_time = time.time()
            last_update = start_time

            while time.time() - start_time < timeout:
                # Check logs for readiness message
                logs = self.container.logs().decode("utf-8")
                if "DATABASE IS READY TO USE!" in logs:
                    pbar.update(timeout - pbar.n)  # Complete the progress bar
                    print("\nOracle database is ready!")
                    return True

                # Update progress bar every second
                current_time = time.time()
                elapsed = current_time - last_update
                if elapsed >= 1:
                    pbar.update(min(elapsed, timeout - pbar.n))
                    last_update = current_time

                time.sleep(1)

            print("\nTimeout reached while waiting for database to be ready")
            return False

    def upload_file(self, local_path: str, container_path: str) -> None:
        """
        Upload a file to the container.

        Args:
            local_path: Path to file on host
            container_path: Destination path in container
        """
        if not self.container:
            raise RuntimeError("Container not created or started")

        print(f"Uploading {local_path} to container...")
        with open(local_path, "rb") as file:
            data = file.read()

        # Create a temporary file-like object
        import io
        file_obj = io.BytesIO(data)
        file_obj.name = os.path.basename(container_path)

        # Copy file to container
        self.container.put_archive(os.path.dirname(container_path), file_obj)
        print("File uploaded successfully.")

    def execute_sqlplus_script(
            self,
            script_path: str,
            username: str = "system",
            log_file: Optional[str] = None,
    ) -> bool:
        """
        Execute a SQL script in the container using SQL*Plus.

        Args:
            script_path: Path to SQL script in container
            username: Database username to connect as
            log_file: Path to log file (relative to log_dir)

        Returns:
            bool: True if execution was successful
        """
        if not self.container:
            raise RuntimeError("Container not created or started")

        if log_file:
            log_path = os.path.join(self.log_dir, log_file)
            log_redirect = f"SPOOL {log_path}\n"
        else:
            log_redirect = ""

        # Create the SQL*Plus command
        sqlplus_cmd = (
            f"sqlplus -S {username}/{self.db_password} @{script_path}"
        )

        # Prepare the full command with logging
        full_cmd = f"""
        {log_redirect}
        {sqlplus_cmd}
        exit
        """

        # Write the command to a temporary file
        temp_script = "/tmp/execute_script.sql"
        with open("temp_execute.sql", "w") as f:
            f.write(full_cmd)

        # Upload and execute
        self.upload_file("temp_execute.sql", temp_script)
        os.remove("temp_execute.sql")

        print(f"Executing SQL script {script_path}...")
        exit_code, output = self.container.exec_run(
            f"/bin/bash -c 'sqlplus {username}/{self.db_password} @{temp_script}'",
            workdir="/tmp",
        )

        if exit_code != 0:
            print(f"SQL*Plus execution failed with exit code {exit_code}")
            print(output.decode("utf-8"))
            return False

        print("SQL script executed successfully.")
        return True

    def get_logs(self) -> str:
        """Get the container logs."""
        if not self.container:
            raise RuntimeError("Container not created or started")
        return self.container.logs().decode("utf-8")

    def save_logs_to_file(self, filename: str = "oracle_container.log") -> None:
        """
        Save container logs to a file.

        Args:
            filename: Name of the log file (will be saved in log_dir)
        """
        log_path = os.path.join(self.log_dir, filename)
        with open(log_path, "w") as f:
            f.write(self.get_logs())
        print(f"Logs saved to {log_path}")

    def destroy_container(self) -> None:
        """Stop and remove the container."""
        if not self.container:
            try:
                self.container = self.client.containers.get(self.container_name)
            except containerization.errors.NotFound:
                print("Container does not exist.")
                return

        print(f"Stopping and removing container {self.container_name}...")
        try:
            self.container.stop()
            self.container.remove()
            print("Container removed successfully.")
        except containerization.errors.APIError as e:
            print(f"Error removing container: {e}")
        finally:
            self.container = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures container is destroyed."""
        self.destroy_container()


# Example usage
if __name__ == "__main__":
    with OracleContainerManager() as oracle_mgr:
        # Pull the image
        oracle_mgr.pull_image()

        # Create and start the container
        oracle_mgr.create_container()

        # Wait for database to be ready
        if not oracle_mgr.wait_for_db_ready():
            raise RuntimeError("Database did not become ready in time")

        # Upload a SQL script
        oracle_mgr.upload_file("example.sql", "/tmp/example.sql")

        # Execute the script with logging
        oracle_mgr.execute_sqlplus_script(
            "/tmp/example.sql",
            username="system",
            log_file="example_execution.log"
        )

        # Save container logs
        oracle_mgr.save_logs_to_file()

        # The container will be automatically destroyed when exiting the 'with' block
