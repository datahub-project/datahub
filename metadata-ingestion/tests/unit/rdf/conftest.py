"""
Pytest configuration for rdf tests.

This file configures warning filters to suppress deprecation warnings from
third-party dependencies (DataHub SDK, Pydantic internals) while keeping
our own deprecation warnings visible.
"""

import warnings

# Suppress Pydantic V2 deprecation warnings from third-party dependencies
# These are from DataHub SDK and will be fixed when DataHub updates to Pydantic V2
try:
    from pydantic import PydanticDeprecatedSince20

    warnings.filterwarnings("ignore", category=PydanticDeprecatedSince20)
except ImportError:
    pass

# Suppress general deprecation warnings from third-party packages
warnings.filterwarnings("ignore", category=DeprecationWarning, module="datahub")
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, module="pydantic._internal"
)

# Suppress UserWarnings from Pydantic about config key changes (V2 migration)
warnings.filterwarnings(
    "ignore", category=UserWarning, module="pydantic._internal._config"
)

# Keep our own deprecation warnings visible
warnings.filterwarnings(
    "error", category=DeprecationWarning, module="datahub.ingestion.source.rdf"
)


def pytest_configure(config):
    """Configure pytest to suppress third-party deprecation warnings."""
    # Register custom markers or configure warnings here
    config.addinivalue_line(
        "filterwarnings", "ignore::pydantic.PydanticDeprecatedSince20"
    )
    config.addinivalue_line(
        "filterwarnings", "ignore::UserWarning:pydantic._internal._config"
    )
