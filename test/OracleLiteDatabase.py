import os
import time

from tqdm import tqdm

from containerization import DockerClient


class OracleTestEnvironment:
    def __init__(self):
        self.client = DockerClient()
        self.container = None
        self.host_port = 1521
        self.password = "oracle"

    def start_container(self):
        """Start optimized Oracle XE container for schema creation"""
        self._find_available_port()

        print("Starting optimized Oracle XE container...")
        self.container = self.client.containers.run(
            "gvenzl/oracle-xe:latest",  # Community optimized image
            name="oracle_test_env",
            environment={
                "ORACLE_PASSWORD": self.password,
                "APP_USER": "admin",  # Default admin user
                "APP_USER_PASSWORD": "admin",
            },
            ports={"1521/tcp": self.host_port},
            detach=True,
            mem_limit="1g",  # Limit to 1GB RAM
            auto_remove=True
        )

        # Wait for startup with progress bar
        with tqdm(desc="Starting database", total=60, unit="s") as pbar:
            for _ in range(60):  # 60 second timeout
                logs = self.container.logs().decode(errors="ignore")
                if "Database ready to use" in logs:
                    pbar.update(60 - pbar.n)
                    break
                pbar.update(1)
                time.sleep(1)

        print(f"\nOracle ready! Connect to port {self.host_port}")

    def _find_available_port(self):
        """Find available port starting from 1521"""
        import socket
        port = self.host_port
        while port < 1600:  # Reasonable port range
            with socket.socket() as s:
                if s.connect_ex(('localhost', port)) != 0:
                    self.host_port = port
                    return
                port += 1
        raise RuntimeError("No available ports found")

    def execute_sql(self, sql_file: str):
        """Execute SQL file in container"""
        # Copy file to container
        self.container.put_archive("/tmp", self._create_tar(sql_file))

        # Execute using sqlplus
        exit_code, output = self.container.exec_run(
            f"sqlplus admin/admin@//localhost/XEPDB1 @/tmp/{os.path.basename(sql_file)}",
            workdir="/tmp"
        )

        if exit_code != 0:
            print(f"Error executing SQL: {output.decode()}")
            return False

        print("SQL executed successfully")
        return True

    def _create_tar(self, file_path):
        """Helper to create in-memory tar file"""
        import io
        import tarfile

        file_data = open(file_path, "rb").read()
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            info = tarfile.TarInfo(name=os.path.basename(file_path))
            info.size = len(file_data)
            tar.addfile(info, io.BytesIO(file_data))

        tar_stream.seek(0)
        return tar_stream.read()


# Usage example
env = OracleTestEnvironment()
env.start_container()

# Example SQL file to create your schemas
schema_script = """
-- Create schema1
CREATE USER schema1 IDENTIFIED BY password1;
GRANT CONNECT, RESOURCE TO schema1;

-- Create tables for schema1
CREATE TABLE schema1.customers (
    id NUMBER GENERATED ALWAYS AS IDENTITY,
    name VARCHAR2(100)
);

-- Repeat for other schemas...
"""

with open("create_schemas.sql", "w") as f:
    f.write(schema_script)

env.execute_sql("create_schemas.sql")
