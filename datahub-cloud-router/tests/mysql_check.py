"""
MySQL availability checker for automated test enablement.
"""

import asyncio
import logging
from typing import Tuple

try:
    import aiomysql

    AIOMYSQL_AVAILABLE = True
except ImportError:
    AIOMYSQL_AVAILABLE = False

# Configure logging to suppress connection warnings during checks
logging.getLogger("aiomysql").setLevel(logging.ERROR)


async def check_mysql_availability(
    host: str = "localhost",
    port: int = 3306,
    user: str = "datahub",
    password: str = "datahub",
    database: str = "datahub",
    timeout_seconds: int = 3,
) -> Tuple[bool, str]:
    """
    Check if MySQL server is available and has the required database.

    Args:
        host: MySQL host
        port: MySQL port
        user: MySQL username
        password: MySQL password
        database: Required database name
        timeout: Connection timeout in seconds

    Returns:
        Tuple of (is_available, reason)
    """
    if not AIOMYSQL_AVAILABLE:
        return False, "aiomysql package not available"

    try:
        # First try to connect to MySQL server without specifying database
        conn = await asyncio.wait_for(
            aiomysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                charset="utf8mb4",
                autocommit=True,
            ),
            timeout=timeout_seconds,
        )

        try:
            # Check if the required database exists
            async with conn.cursor() as cursor:
                await cursor.execute("SHOW DATABASES LIKE %s", (database,))
                result = await cursor.fetchone()

                if not result:
                    return False, f"database '{database}' does not exist"

                # Try to use the database to verify permissions
                await cursor.execute(f"USE `{database}`")

                # Test basic table operations permissions
                await cursor.execute("SHOW TABLES")

            return True, "MySQL available with required database"

        finally:
            conn.close()

    except asyncio.TimeoutError:
        return False, f"connection timeout after {timeout_seconds}s"
    except Exception as e:
        error_msg = str(e).lower()

        # Provide more specific error messages
        if "access denied" in error_msg:
            return False, f"access denied for user '{user}'"
        elif "unknown database" in error_msg:
            return False, f"database '{database}' does not exist"
        elif "can't connect" in error_msg or "connection refused" in error_msg:
            return False, f"MySQL server not running on {host}:{port}"
        elif "unknown mysql server host" in error_msg:
            return False, f"unknown host '{host}'"
        else:
            return False, f"MySQL error: {e}"


def check_mysql_sync(
    host: str = "localhost",
    port: int = 3306,
    user: str = "datahub",
    password: str = "datahub",
    database: str = "datahub",
    timeout_seconds: int = 3,
) -> Tuple[bool, str]:
    """
    Synchronous wrapper for MySQL availability check.

    Args:
        host: MySQL host
        port: MySQL port
        user: MySQL username
        password: MySQL password
        database: Required database name
        timeout_seconds: Connection timeout in seconds

    Returns:
        Tuple of (is_available, reason)
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(
            check_mysql_availability(
                host, port, user, password, database, timeout_seconds
            )
        )
    except Exception as e:
        return False, f"failed to check MySQL: {e}"
    finally:
        try:
            loop.close()
        except Exception:
            pass


if __name__ == "__main__":
    """Command line interface for MySQL availability check."""
    import sys

    available, reason = check_mysql_sync()

    if available:
        print(f"✅ MySQL is available: {reason}")
        sys.exit(0)
    else:
        print(f"❌ MySQL not available: {reason}")
        sys.exit(1)
