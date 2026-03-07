"""
Helper utilities for Kafka SSL configuration.

This module provides utilities to handle SSL configuration for Kafka Schema Registry,
particularly for converting ssl.ca.location from string paths to SSL context objects
as required by confluent-kafka-python >= 2.8.0.

Related Issues:
- https://github.com/datahub-project/datahub/issues/14576
- https://github.com/confluentinc/confluent-kafka-python/issues/1909
"""

import logging
import ssl
from typing import Any, Dict

logger = logging.getLogger(__name__)


def prepare_schema_registry_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare schema registry configuration by converting ssl.ca.location from
    string path to SSL context object.

    Starting with confluent-kafka-python 2.8.0, the library migrated from using
    'requests' to 'httpx' for HTTP communication. This change requires that
    ssl.ca.location be provided as an SSL context object rather than a string path.

    This function handles the conversion automatically while maintaining backward
    compatibility with configurations that already provide SSL context objects.

    Args:
        config: Schema registry configuration dictionary that may contain:
            - url: Schema registry URL
            - ssl.ca.location: CA certificate path (string) or SSL context
            - ssl.certificate.location: Client certificate path (string)
            - ssl.key.location: Client key path (string)
            - Other schema registry configuration options

    Returns:
        Updated configuration dictionary with ssl.ca.location converted to
        SSL context object if it was provided as a string path.

    Example:
        >>> config = {
        ...     "url": "https://registry.example.com",
        ...     "ssl.ca.location": "/path/to/ca-bundle.pem",
        ...     "ssl.certificate.location": "/path/to/client.cert.pem",
        ...     "ssl.key.location": "/path/to/client.key.pem",
        ... }
        >>> prepared = prepare_schema_registry_config(config)
        >>> isinstance(prepared["ssl.ca.location"], ssl.SSLContext)
        True

    Note:
        - If ssl.ca.location is already an SSL context, it is left unchanged
        - If ssl.ca.location is not provided, the config is returned as-is
        - If creating the SSL context fails, a warning is logged and the
          original string path is retained (allowing SchemaRegistryClient
          to provide a clear error message)
    """
    # Make a copy to avoid modifying the original config
    prepared_config = config.copy()

    # Check if ssl.ca.location is provided
    ca_cert_path = prepared_config.get("ssl.ca.location")

    if ca_cert_path is None:
        # No SSL CA certificate configured
        return prepared_config

    if isinstance(ca_cert_path, ssl.SSLContext):
        # Already an SSL context, no conversion needed
        logger.debug("ssl.ca.location is already an SSL context")
        return prepared_config

    if not isinstance(ca_cert_path, str):
        # Unexpected type, log warning and return as-is
        logger.warning(
            f"ssl.ca.location has unexpected type {type(ca_cert_path)}, "
            f"expected str or ssl.SSLContext"
        )
        return prepared_config

    if not ca_cert_path:
        # Empty string, return as-is
        logger.debug("ssl.ca.location is an empty string, skipping conversion")
        return prepared_config

    # Convert string path to SSL context
    try:
        logger.debug(
            f"Converting ssl.ca.location from path to SSL context: {ca_cert_path}"
        )
        ca_context = ssl.create_default_context(cafile=ca_cert_path)
        prepared_config["ssl.ca.location"] = ca_context
        logger.debug("Successfully created SSL context from CA certificate")
    except FileNotFoundError:
        logger.warning(
            f"CA certificate file not found: {ca_cert_path}. "
            f"Keeping original path - SchemaRegistryClient will handle the error."
        )
    except ssl.SSLError as e:
        logger.warning(
            f"Failed to create SSL context from CA certificate {ca_cert_path}: {e}. "
            f"Keeping original path - SchemaRegistryClient will handle the error."
        )
    except Exception as e:
        logger.warning(
            f"Unexpected error creating SSL context from {ca_cert_path}: {e}. "
            f"Keeping original path - SchemaRegistryClient will handle the error."
        )

    return prepared_config
