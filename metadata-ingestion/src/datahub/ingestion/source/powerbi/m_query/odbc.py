import re
from typing import Optional, Tuple, Union


def extract_driver(connection_string: str) -> Union[str, None]:
    """
    Parse an ODBC connection string and extract the driver name.
    Handles whitespace in driver names and various connection string formats.

    Args:
        connection_string (str): The ODBC connection string

    Returns:
        str: The extracted driver name, or None if not found
    """
    # Match DRIVER={driver name} pattern
    driver_match = re.search(r"DRIVER=\{([^}]*)}", connection_string, re.IGNORECASE)

    if driver_match:
        return driver_match.group(1).strip()

    # Alternative pattern for DRIVER=driver
    driver_match = re.search(r"DRIVER=([^;]*)", connection_string, re.IGNORECASE)

    if driver_match:
        return driver_match.group(1).strip()

    return None


def extract_server(connection_string: str) -> Union[str, None]:
    """
    Parse an ODBC connection string and extract the server name.
    Handles various parameter names for server (SERVER, Host, Data Source, etc.)

    Args:
        connection_string (str): The ODBC connection string

    Returns:
        str: The extracted server name, or None if not found
    """
    server_patterns = [
        r"SERVER=\{([^}]*)\}",
        r"SERVER=([^;]*)",
        r"HOST=\{([^}]*)\}",
        r"HOST=([^;]*)",
        r"DATA SOURCE=\{([^}]*)\}",
        r"DATA SOURCE=([^;]*)",
        r"DSN=\{([^}]*)\}",
        r"DSN=([^;]*)",
        r"Server=([^;]*)",
        r"S3OutputLocation=([^;]*)",
        r"HTTPPath=([^;]*)",
        r"Host=([^;]*)",
    ]

    for pattern in server_patterns:
        server_match = re.search(pattern, connection_string, re.IGNORECASE)
        if server_match:
            return server_match.group(1).strip()

    # Special case for Athena: extract from AwsRegion if no server found
    region_match = re.search(r"AwsRegion=([^;]*)", connection_string, re.IGNORECASE)
    if region_match:
        return f"aws-athena-{region_match.group(1).strip()}"

    # Special case for Databricks: try to extract hostname from JDBC URL
    jdbc_match = re.search(r"jdbc:spark://([^:;/]+)", connection_string, re.IGNORECASE)
    if jdbc_match:
        return jdbc_match.group(1).strip()

    return None


def extract_platform(connection_string: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract the database platform name from the ODBC driver name.
    Returns the lowercase platform name.

    Args:
        connection_string (str): The ODBC connection string

    Returns:
        str: The lowercase database platform name, or None if not recognized
    """
    driver_name = extract_driver(connection_string)
    if not driver_name:
        return None, None

    platform_patterns = {
        "mysql": r"mysql",
        "postgres": r"post(gre(s|sql)?|gres)",
        "mssql": r"(sql\s*server|mssql|sqlncli)",
        "oracle": r"oracle",
        "db2": r"db2",
        "sqlite": r"sqlite",
        "access": r"(access|\.mdb|\.accdb)",
        "excel": r"(excel|\.xls)",
        "firebird": r"firebird",
        "informix": r"informix",
        "sybase": r"sybase",
        "teradata": r"teradata",
        "hadoop": r"(hadoop|hive)",
        "snowflake": r"snowflake",
        "redshift": r"redshift",
        "bigquery": r"bigquery",
        "athena": r"(athena|aws\s*athena)",
        "databricks": r"(databricks|spark)",
    }

    powerbi_platform_names = {
        "mysql": "MySQL",
        "postgres": "PostgreSQL",
        "mssql": "SQL Server",
        "oracle": "Oracle",
        "db2": "IBM DB2",
        "sqlite": "SQLite",
        "access": "Microsoft Access",
        "excel": "Microsoft Excel",
        "firebird": "Firebird",
        "informix": "IBM Informix",
        "sybase": "SAP Sybase",
        "teradata": "Teradata",
        "hadoop": "Hadoop",
        "snowflake": "Snowflake",
        "redshift": "Amazon Redshift",
        "bigquery": "Google BigQuery",
        "athena": "Amazon Athena",
        "databricks": "Databricks",
    }

    driver_lower = driver_name.lower()

    for platform, pattern in platform_patterns.items():
        if re.search(pattern, driver_lower):
            return platform, powerbi_platform_names.get(platform)

    return None, None
