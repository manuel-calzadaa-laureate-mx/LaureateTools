import logging
import subprocess
from dataclasses import dataclass
from typing import Optional, Dict, List

import docker
from docker.models.containers import Container


@dataclass
class ContainerConfig:
    """Base configuration for any Docker container."""
    container_name: str = "hello-world"
    image_name: str = "hello-world"
    ready_timeout: int = 120  # seconds
    health_check_interval: int = 5  # seconds
    ports: Optional[Dict[str, int]] = None
    environment: Optional[Dict[str, str]] = None
    volumes: Optional[Dict[str, Dict]] = None
    auto_remove: bool = True
    detach: bool = True


class DockerManager:
    """Manages Docker container lifecycle in a generic way."""

    def __init__(self, config: Optional[ContainerConfig] = None, container_name: Optional[str] = None):
        """
        Initialize DockerManager either with a full config or just a container name.

        Args:
            config: Full container configuration (optional)
            container_name: Name of existing container (optional)
        """
        if config is None and container_name is None:
            raise ValueError("Either config or container_name must be provided")

        self.config = config
        self.client = docker.from_env()
        self.container: Optional[Container] = None
        self.logger = logging.getLogger(__name__)

        # If only container_name was provided, try to get the existing container
        if container_name is not None:
            self._init_from_existing_container(container_name)

    def _init_from_existing_container(self, container_name: str) -> None:
        """Initialize manager with an existing container by name."""
        try:
            self.container = self.client.containers.get(container_name)
            self.logger.info(f"Connected to existing container: {container_name}")

            # Create a minimal config with the info we can gather
            self.config = ContainerConfig(
                container_name=container_name,
                image_name=self.container.image.tags[0] if self.container.image.tags else "unknown"
            )
        except docker.errors.NotFound:
            self.logger.error(f"Container not found: {container_name}")
            raise
        except docker.errors.APIError as e:
            self.logger.error(f"Error accessing container: {e}")
            raise

    @classmethod
    def from_existing_container(cls, container_name: str) -> 'DockerManager':
        """Alternative constructor for working with an existing container by name."""
        return cls(container_name=container_name)

    def pull_image(self) -> None:
        """Pull the Docker image."""
        self.logger.info(f"Pulling image: {self.config.image_name}")
        try:
            self.client.images.pull(self.config.image_name)
            self.logger.info("Image pulled successfully")
        except docker.errors.APIError as e:
            self.logger.error(f"Failed to pull image: {e}")
            raise

    def start_container(self) -> None:
        """Start the Docker container."""
        if self._container_exists():
            self.logger.warning(f"Container {self.config.container_name} already exists")
            self.container = self.client.containers.get(self.config.container_name)
            if self.container.status != "running":
                self.container.start()
                self.logger.info(f"Started existing container {self.config.container_name}")
            return

        self.logger.info(f"Starting new container {self.config.container_name}")
        try:
            self.container = self.client.containers.run(
                self.config.image_name,
                name=self.config.container_name,
                environment=self.config.environment,
                ports=self.config.ports,
                volumes=self.config.volumes,
                detach=self.config.detach,
                auto_remove=self.config.auto_remove
            )
            self.logger.info(f"Container started: {self.config.container_name}")
        except docker.errors.APIError as e:
            self.logger.error(f"Failed to start container: {e}")
            raise

    def _container_exists(self) -> bool:
        """Check if container already exists."""
        try:
            self.client.containers.get(self.config.container_name)
            return True
        except docker.errors.NotFound:
            return False

    def is_container_ready(self) -> bool:
        """Check if container is ready to accept connections."""
        if not self.container:
            return False

        try:
            # Check container status
            self.container.reload()
            if self.container.status != "running":
                return False

            # Check health status if available
            if hasattr(self.container, 'attrs') and 'State' in self.container.attrs:
                health = self.container.attrs['State'].get('Health', {}).get('Status')
                if health == 'healthy':
                    return True

            # Fallback to our own check
            return True
        except docker.errors.APIError:
            return False

    def stop_container(self) -> None:
        """Stop and remove the container."""
        if self.container:
            self.logger.info(f"Stopping container {self.config.container_name}")
            try:
                self.container.stop()
                self.logger.info("Container stopped")
            except docker.errors.APIError as e:
                self.logger.error(f"Failed to stop container: {e}")
                raise

    def execute_command(self, command: str or List[str], workdir: str = None,
                        user: str = None, environment: Dict[str, str] = None,
                        privileged: bool = False, tty: bool = False) -> (int, str):
        """
        Execute a command inside the container.

        Args:
            command: Command to execute (either as string or list of args)
            workdir: Working directory inside the container (optional)
            user: Username or UID to run command as (optional)
            environment: Additional environment variables (optional)
            privileged: Run command in privileged mode (optional)
            tty: Allocate a pseudo-TTY (optional)

        Returns:
            Tuple of (exit_code, output)

        Raises:
            RuntimeError: If container is not running or command execution fails
        """
        if not self.container:
            raise RuntimeError("Container not initialized or not running")

        try:
            # Convert command to list if it's a string
            if isinstance(command, str):
                command = ["sh", "-c", command]

            # Prepare execution options
            exec_options = {
                "workdir": workdir,
                "user": user,
                "environment": environment,
                "privileged": privileged,
                "tty": tty,
                "detach": False
            }

            # Remove None values from options
            exec_options = {k: v for k, v in exec_options.items() if v is not None}

            # Execute the command
            exit_code, output = self.container.exec_run(command, **exec_options)

            # Decode output if it's bytes
            if isinstance(output, bytes):
                output = output.decode('utf-8')

            self.logger.debug(f"Command executed. Exit code: {exit_code}, Output: {output}")
            return exit_code, output

        except docker.errors.APIError as e:
            self.logger.error(f"Failed to execute command: {e}")
            raise RuntimeError(f"Command execution failed: {e}")

    def copy_file_to_container(self, local_path: str, container_path: str):
        """Copies a file from the host to the container using docker cp."""
        try:
            container_id = self.docker.container.id
            subprocess.run(
                ["docker", "cp", local_path, f"{container_id}:{container_path}"],
                check=True,
            )
            self.logger.info(f"File {local_path} copied to container at {container_path}.")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error copying file to container: {e}")
            raise
