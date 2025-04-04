import logging
from typing import Optional, Container

import docker

from containerization.config import OracleDatabaseConfig


class DockerManager:
    """Manages Docker container lifecycle for Oracle database."""

    def __init__(self, config: OracleDatabaseConfig):
        self.config = config
        self.client = docker.from_env()
        self.container: Optional[Container] = None
        self.logger = logging.getLogger(__name__)

    def pull_image(self) -> None:
        """Pull the Oracle database Docker image."""
        self.logger.info(f"Pulling Oracle image: {self.config.image_name}")
        try:
            self.client.images.pull(self.config.image_name)
            self.logger.info("Image pulled successfully")
        except docker.errors.APIError as e:
            self.logger.error(f"Failed to pull image: {e}")
            raise

    def start_container(self) -> None:
        """Start the Oracle database container."""
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
                environment={
                    "ORACLE_PASSWORD": self.config.db_password,
                    "APP_USER": self.config.app_user,
                    "APP_USER_PASSWORD": self.config.app_user_password
                },
                ports={"1521/tcp": self.config.db_port},
                detach=True,
                auto_remove=True
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
