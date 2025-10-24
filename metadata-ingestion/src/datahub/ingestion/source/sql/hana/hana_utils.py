"""
SAP HANA utilities for DataHub ingestion.

This module provides utility classes and functions for SAP HANA connector,
including identifier builders, filters, and other helper functionality.
"""

from typing import Optional

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig


class HanaIdentifierBuilder:
    """Builder for generating DataHub identifiers and URNs for SAP HANA objects."""

    platform = "hana"

    def __init__(self, config: SQLCommonConfig):
        """Initialize the identifier builder.

        Args:
            config: SQL configuration containing platform instance and environment settings
        """
        self.config = config

    def get_dataset_identifier(
        self, table_name: str, schema_name: str, db_name: str
    ) -> str:
        """Generate dataset identifier for a table or view.

        Args:
            table_name: Name of the table or view
            schema_name: Name of the schema
            db_name: Name of the database

        Returns:
            Dataset identifier string
        """
        if db_name:
            return f"{db_name.lower()}.{schema_name.lower()}.{table_name.lower()}"
        else:
            return f"{schema_name.lower()}.{table_name.lower()}"

    def gen_dataset_urn(self, dataset_identifier: str) -> str:
        """Generate dataset URN for a dataset identifier.

        Args:
            dataset_identifier: Dataset identifier string

        Returns:
            Dataset URN
        """
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_identifier,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def get_user_identifier(
        self, user_name: str, user_email: Optional[str] = None
    ) -> str:
        """Generate user identifier.

        Args:
            user_name: Username
            user_email: Optional email address

        Returns:
            User identifier string
        """
        if user_email:
            return user_email
        else:
            return user_name.lower()

    def gen_user_urn(self, user_identifier: str) -> str:
        """Generate user URN.

        Args:
            user_identifier: User identifier string

        Returns:
            User URN
        """
        return make_user_urn(user_identifier)

    def hana_identifier(self, identifier: str) -> str:
        """Convert identifier to HANA-compatible format.

        Args:
            identifier: Original identifier

        Returns:
            HANA-compatible identifier (lowercase)
        """
        return identifier.lower()
