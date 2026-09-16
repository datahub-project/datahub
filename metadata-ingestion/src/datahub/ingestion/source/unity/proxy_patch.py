"""
Proxy authentication patch for databricks-sql < 3.0 compatibility.

This module provides proxy authentication fixes for databricks-sql connector < 3.0
to resolve "407 Proxy Authentication Required" errors that occur even when
proxy environment variables are correctly set.

The patch implements the same fix as Databricks PR #354:
https://github.com/databricks/databricks-sql-python/pull/354
"""

import logging
import os
import urllib.parse
from typing import Dict, Optional

logger: logging.Logger = logging.getLogger(__name__)

PROXY_VARS = ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]


def mask_proxy_credentials(url: Optional[str]) -> str:
    """Mask credentials in proxy URL for safe logging."""
    if not url:
        return "None"

    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.username:
            # Replace credentials with masked version
            masked_netloc = parsed.netloc
            if parsed.username and parsed.password:
                masked_netloc = masked_netloc.replace(
                    f"{parsed.username}:{parsed.password}@", f"{parsed.username}:***@"
                )
            elif parsed.username:
                masked_netloc = masked_netloc.replace(
                    f"{parsed.username}@", f"{parsed.username}:***@"
                )

            return urllib.parse.urlunparse(
                (
                    parsed.scheme,
                    masked_netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment,
                )
            )
        else:
            return url
    except Exception:
        return "***INVALID_URL***"


def _ensure_thrift_imports():
    """Ensure required thrift imports are loaded before accessing thrift_http_client.

    The databricks-sql thrift_http_client requires thrift.transport.THttpClient.THttpClient
    to be accessible. This is achieved by importing the required modules in the right order.
    """
    try:
        # Import thrift submodules - this makes them accessible as attributes
        import thrift.transport.THttpClient  # noqa: F401 # Used to make thrift.transport accessible

        logger.debug("Successfully imported required thrift modules")
    except Exception as e:
        logger.debug(f"Could not import thrift modules: {e}")
        raise


def _log_proxy_environment():
    """Log detected proxy environment variables for debugging."""
    proxy_env_vars = {}
    for var in PROXY_VARS:
        value = os.environ.get(var)
        if value:
            masked_value = mask_proxy_credentials(value)
            proxy_env_vars[var] = masked_value

    if proxy_env_vars:
        logger.info(f"Detected proxy environment variables: {proxy_env_vars}")
    else:
        logger.debug("No proxy environment variables detected")


def _basic_proxy_auth_header(proxy_url: str) -> Optional[Dict[str, str]]:
    """Create proxy authentication header using the same method as Databricks >= 3.0.

    Based on the basic_proxy_auth_header method from databricks-sql-connector >= 3.0:
    https://github.com/databricks/databricks-sql-python/pull/354
    """
    try:
        from urllib3.util import make_headers

        parsed = urllib.parse.urlparse(proxy_url)
        if parsed.username and parsed.password:
            # Code reused from https://github.com/databricks/databricks-sql-python/pull/354
            # URL decode the username and password (same as Databricks method)
            username = urllib.parse.unquote(parsed.username)
            password = urllib.parse.unquote(parsed.password)
            auth_string = f"{username}:{password}"

            # Create proxy URL without credentials
            proxy_host_port = f"{parsed.scheme}://{parsed.hostname}"
            if parsed.port:
                proxy_host_port += f":{parsed.port}"

            # Code reused from https://github.com/databricks/databricks-sql-python/pull/354
            # Use make_headers like the newer Databricks version does
            proxy_headers = make_headers(proxy_basic_auth=auth_string)

            return {
                "proxy_url": proxy_host_port,
                "proxy_headers": proxy_headers,
                "auth_string": auth_string,  # Keep for backward compatibility with tests
            }
    except Exception as e:
        logger.debug(f"Failed to create proxy auth header from URL {proxy_url}: {e}")

    return None


def _handle_proxy_connection(self, original_open, pool_kwargs):
    """Handle proxy connection setup with authentication headers."""
    from urllib3.poolmanager import ProxyManager

    logger.info(f"Using proxy for connection to {self.host}:{self.port}")
    proxy_uri = getattr(self, "proxy_uri", None)
    logger.debug(
        f"Proxy URI: {mask_proxy_credentials(proxy_uri) if proxy_uri else 'None'}"
    )

    # Compute proxy authentication headers properly (the bug fix!)
    proxy_headers = None
    proxy_env_found = None
    for env_var in ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"]:
        proxy_url = os.environ.get(env_var)
        if proxy_url:
            logger.debug(
                f"Found proxy URL in {env_var}: {mask_proxy_credentials(proxy_url)}"
            )
            auth_info = _basic_proxy_auth_header(proxy_url)
            if auth_info:
                proxy_headers = auth_info["proxy_headers"]
                proxy_env_found = env_var
                logger.debug(f"Successfully created proxy headers from {env_var}")
                break
            else:
                logger.debug(
                    f"No authentication info found in proxy URL from {env_var}"
                )

    if proxy_headers:
        logger.info(f"Using proxy authentication headers from {proxy_env_found}")
    else:
        logger.warning(
            "No proxy authentication headers could be created from environment variables"
        )

    proxy_manager = ProxyManager(
        self.proxy_uri,
        num_pools=1,
        proxy_headers=proxy_headers,
    )

    # Validate proxy manager attributes
    if not hasattr(self, "realhost") or not hasattr(self, "realport"):
        logger.warning(
            "THttpClient missing realhost/realport attributes, falling back to original"
        )
        return original_open(self)

    # Set up the connection pool
    self._THttpClient__pool = proxy_manager.connection_from_host(
        host=self.realhost,
        port=self.realport,
        scheme=self.scheme,
        pool_kwargs=pool_kwargs,  # type: ignore
    )
    logger.debug(f"Created proxy connection pool for {self.realhost}:{self.realport}")


def _create_patched_open_method(original_open):
    """Create the patched THttpClient.open method with proxy authentication fix."""

    def patched_open(self):
        """Patched version of THttpClient.open following databricks-sql >= 3.0 structure.

        This is largely copied from the >= 3.0 implementation:
        https://github.com/databricks/databricks-sql-python/pull/354/files
        """
        logger.debug(
            f"Patched THttpClient.open called for host={getattr(self, 'host', 'unknown')}, scheme={getattr(self, 'scheme', 'unknown')}"
        )

        try:
            # Validate required attributes
            required_attrs = ["scheme", "host", "port", "max_connections"]
            missing_attrs = [attr for attr in required_attrs if not hasattr(self, attr)]
            if missing_attrs:
                logger.warning(
                    f"THttpClient missing required attributes: {missing_attrs}, falling back to original"
                )
                return original_open(self)

            # Code structure reused from https://github.com/databricks/databricks-sql-python/pull/354
            # Determine pool class based on scheme
            if self.scheme == "http":
                from urllib3 import HTTPConnectionPool

                pool_class = HTTPConnectionPool
            elif self.scheme == "https":
                from urllib3 import HTTPSConnectionPool

                pool_class = HTTPSConnectionPool
            else:
                logger.warning(
                    f"Unknown scheme '{self.scheme}', falling back to original"
                )
                return original_open(self)

            _pool_kwargs = {"maxsize": self.max_connections}
            logger.debug(f"Pool kwargs: {_pool_kwargs}")

            if self.using_proxy():
                return _handle_proxy_connection(self, original_open, _pool_kwargs)
            else:
                logger.debug(f"Direct connection (no proxy) to {self.host}:{self.port}")
                self._THttpClient__pool = pool_class(
                    self.host, self.port, **_pool_kwargs
                )

            logger.debug("Patched THttpClient.open completed successfully")

        except Exception as e:
            logger.warning(
                f"Error in proxy auth patch: {e}, falling back to original",
                exc_info=True,
            )
            # Fallback to original implementation
            try:
                return original_open(self)
            except Exception as fallback_error:
                logger.error(
                    f"Fallback to original THttpClient.open also failed: {fallback_error}",
                    exc_info=True,
                )
                raise

    return patched_open


def apply_databricks_proxy_fix():
    """Apply the databricks-sql < 3.0 proxy authentication fix at module import time.

    This implements the same fix as Databricks PR #354 to resolve
    "407 Proxy Authentication Required" errors that occur even when
    all proxy environment variables are correctly set.

    Note: This fix may not work with all thrift versions due to compatibility issues
    between databricks-sql-connector 2.9.6 and newer thrift versions. The fix will
    gracefully fail with a warning if thrift compatibility issues are detected.
    The main SQL functionality will continue to work normally without this fix.
    """
    _log_proxy_environment()
    logger.info("Applying databricks-sql proxy authentication fix...")

    try:
        _ensure_thrift_imports()
        import databricks.sql.auth.thrift_http_client as thrift_http

        # Store original method for fallback
        original_open = getattr(thrift_http.THttpClient, "open", None)
        if not original_open:
            logger.warning("Could not find THttpClient.open method to patch")
            return False

        logger.debug(f"Found THttpClient.open method at {original_open}")

        # Apply the patch
        patched_open = _create_patched_open_method(original_open)
        thrift_http.THttpClient.open = patched_open
        logger.info("Successfully applied databricks-sql proxy authentication fix")

        # Verify the patch was applied
        current_method = getattr(thrift_http.THttpClient, "open", None)
        if current_method == patched_open:
            logger.debug(
                "Patch verification successful: THttpClient.open is now the patched version"
            )
            return True
        else:
            logger.warning(
                "Patch verification failed: THttpClient.open was not replaced correctly"
            )
            return False

    except ImportError as e:
        logger.debug(f"Could not import databricks-sql internals for proxy patch: {e}")
        return False
    except AttributeError as e:
        if "thrift" in str(e).lower() and "transport" in str(e).lower():
            warning_msg = (
                f"Databricks-sql proxy authentication patch could not be applied due to thrift version incompatibility: {e}. "
                "In most environments, the SQL connection will still work without this patch."
            )
            logger.warning(warning_msg)
            # Import here to avoid circular imports
            from datahub.utilities.global_warning_util import add_global_warning

            add_global_warning(warning_msg)
        else:
            logger.error(
                f"Failed to apply databricks-sql proxy patch: {e}", exc_info=True
            )
        return False
    except Exception as e:
        logger.error(f"Failed to apply databricks-sql proxy patch: {e}", exc_info=True)
        return False
