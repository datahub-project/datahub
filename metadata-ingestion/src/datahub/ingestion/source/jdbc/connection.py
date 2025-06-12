import base64
import hashlib
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional

import jaydebeapi
import jpype
from jpype import JClass

from datahub.ingestion.source.jdbc.config import SSLConfig
from datahub.ingestion.source.jdbc.maven_utils import MavenManager

logger = logging.getLogger(__name__)


# Monkey patch the jpype deprecated function used in jaydebeapi
def isThreadAttachedToJVM() -> bool:
    Thread = JClass("java.lang.Thread")
    return Thread.isAttached()


# Apply the patch
jpype.isThreadAttachedToJVM = isThreadAttachedToJVM


class ConnectionManager:
    """Manages JDBC database connections."""

    def __init__(self):
        self._connection = None
        self._temp_files = []

    def get_connection(
        self,
        driver_class: str,
        uri: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver_path: Optional[str] = None,
        maven_coordinates: Optional[str] = None,
        properties: Optional[Dict] = None,
        ssl_config: Optional[SSLConfig] = None,
        jvm_args: Optional[List[str]] = None,
    ) -> jaydebeapi.Connection:
        """Get JDBC connection with retry logic."""
        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                if not self._connection or self._connection.closed:
                    path = self._get_driver_path(driver_path, maven_coordinates)
                    props = self._get_connection_properties(
                        username, password, properties or {}, ssl_config
                    )

                    # Use JVM args if provided
                    if jvm_args:
                        os.environ["_JAVA_OPTIONS"] = " ".join(jvm_args)

                    self._connection = jaydebeapi.connect(
                        driver_class, uri, props, path
                    )
                    return self._connection
            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(
                        f"Failed to create connection after {max_retries} attempts: {str(e)}"
                    )
                logger.warning(
                    f"Connection attempt {attempt + 1} failed, retrying in {retry_delay}s: {str(e)}"
                )
                time.sleep(retry_delay)

        raise Exception("Failed to establish connection after all retries")

    def _get_driver_path(
        self, driver_path: Optional[str], maven_coordinates: Optional[str]
    ) -> str:
        """Get JDBC driver path."""
        if driver_path:
            path = os.path.expanduser(driver_path)
            if not os.path.exists(path):
                raise FileNotFoundError(f"Driver not found at: {path}")
            return path

        if maven_coordinates:
            try:
                maven = MavenManager()
                if not maven.is_maven_installed:
                    maven.setup_environment()
                return self._download_driver_from_maven(maven_coordinates)
            except Exception as e:
                raise Exception(f"Failed to download driver from Maven: {str(e)}")

        raise ValueError("Either driver_path or maven_coordinates must be specified")

    def _download_driver_from_maven(self, coords: str) -> str:
        """Download driver from Maven."""
        driver_dir = Path.home() / ".datahub" / "drivers"
        driver_dir.mkdir(parents=True, exist_ok=True)

        try:
            group_id, artifact_id, version = coords.split(":")
        except ValueError:
            raise ValueError(
                f"Invalid Maven coordinates: {coords}. Format should be groupId:artifactId:version"
            )

        coords_hash = hashlib.sha256(coords.encode()).hexdigest()[:12]
        cache_path = driver_dir / f"driver-{coords_hash}.jar"

        if cache_path.exists():
            logger.info(f"Using cached driver from {cache_path}")
            return str(cache_path)

        with tempfile.TemporaryDirectory() as temp_dir:
            maven_cmd = [
                "mvn",
                "dependency:copy",
                f"-Dartifact={coords}",
                f"-DoutputDirectory={temp_dir}",
                "-Dmdep.stripVersion=true",
                "-q",
            ]

            try:
                logger.info(f"Downloading driver for {coords}")
                subprocess.run(maven_cmd, check=True, capture_output=True, text=True)

                downloaded_path = Path(temp_dir) / f"{artifact_id}.jar"
                if not downloaded_path.exists():
                    raise FileNotFoundError(
                        f"Maven download succeeded but file not found at {downloaded_path}"
                    )

                import shutil

                shutil.copy2(downloaded_path, cache_path)
                logger.info(f"Driver downloaded and cached at {cache_path}")

                return str(cache_path)

            except subprocess.CalledProcessError as e:
                error_msg = e.stderr if e.stderr else e.stdout
                raise Exception(f"Maven download failed: {error_msg}")

    def _get_connection_properties(
        self,
        username: Optional[str],
        password: Optional[str],
        properties: Dict[str, str],
        ssl_config: Optional[SSLConfig],
    ) -> Dict[str, str]:
        """Get connection properties."""
        props = dict(properties)

        if username:
            props["user"] = username
        if password:
            props["password"] = password

        if ssl_config:
            props.update(self._get_ssl_properties(ssl_config))

        return props

    def _get_ssl_properties(self, ssl_config: SSLConfig) -> Dict[str, str]:
        """Get SSL properties."""
        props = {"ssl": "true"}

        if ssl_config.cert_path:
            props["sslcert"] = ssl_config.cert_path
        elif ssl_config.cert_content:
            try:
                cert_content = base64.b64decode(ssl_config.cert_content)
                fd, temp_path = tempfile.mkstemp(suffix=f".{ssl_config.cert_type}")
                self._temp_files.append(temp_path)
                with os.fdopen(fd, "wb") as f:
                    f.write(cert_content)
                props["sslcert"] = temp_path
            except Exception as e:
                raise Exception(f"Failed to create SSL certificate file: {str(e)}")

        if ssl_config.cert_password:
            props["sslpassword"] = ssl_config.cert_password

        return props

    def close(self):
        """Clean up resources."""
        if self._connection:
            try:
                # Close all cursors if they exist
                if hasattr(self._connection, "_cursors"):
                    for cursor in self._connection._cursors:
                        try:
                            cursor.close()
                        except Exception:
                            pass

                # Close the JDBC connection
                if hasattr(self._connection, "jconn"):
                    try:
                        self._connection.jconn.close()
                    except Exception:
                        pass

                # Close the Python connection
                self._connection.close()

            except Exception as e:
                logger.debug(f"Error during connection cleanup: {str(e)}")
            finally:
                self._connection = None

        # Clean up temporary files
        for temp_file in self._temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                logger.debug(f"Error removing temporary file {temp_file}: {str(e)}")
        self._temp_files.clear()
