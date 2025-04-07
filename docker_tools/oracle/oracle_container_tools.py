# User management from the original version
from typing import List


def create_user(self, username: str, password: str) -> None:
    """Create a basic database user."""
    self.logger.info(f"Creating user {username}")
    sql = f'CREATE USER {username} IDENTIFIED BY "{password}";'
    self.execute_sql("system", self.config.db_password, sql)


def grant_privileges(self, username: str, privileges: List[str]) -> None:
    """Grant privileges to a user."""
    self.logger.info(f"Granting privileges to {username}: {privileges}")
    for privilege in privileges:
        sql = f"GRANT {privilege} TO {username};"
        self.execute_sql("system", self.config.db_password, sql)


def set_tablespace_quota(self, username: str, tablespace: str = "USERS", quota: str = "UNLIMITED") -> None:
    """Set tablespace quota for a user."""
    self.logger.info(f"Setting tablespace quota for {username}")
    sql = f"ALTER USER {username} QUOTA {quota} ON {tablespace};"
    self.execute_sql("system", self.config.db_password, sql)
