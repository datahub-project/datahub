import types

import datahub.metadata.schema_classes as models
from datahub.errors import SdkUsageError
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.graph.filters import FilterOperator
from datahub.metadata.urns import (
    ChartUrn,
    ContainerUrn,
    CorpGroupUrn,
    CorpUserUrn,
    DashboardUrn,
    DataPlatformInstanceUrn,
    DataPlatformUrn,
    DatasetUrn,
    DomainUrn,
    GlossaryTermUrn,
    SchemaFieldUrn,
    TagUrn,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import Filter, FilterDsl

# We want to print out the warning if people do `from datahub.sdk import X`.
# But we don't want to print out warnings if they're doing a more direct
# import like `from datahub.sdk.container import Container`, since that's
# what our internal code does.
_vars = {}
for _name, _value in list(locals().items()):
    if not _name.startswith("_") and (
        _name == "models" or not isinstance(_value, types.ModuleType)
    ):
        _vars[_name] = _value
        del locals()[_name]


def __getattr__(name):
    import warnings

    from datahub.errors import ExperimentalWarning

    warnings.warn(
        "The new datahub SDK (e.g. datahub.sdk.*) is experimental. "
        "Our typical backwards-compatibility and stability guarantees do not apply to this code. "
        "When it's promoted to stable, the import path will change "
        "from `from datahub.sdk import ...` to `from datahub import ...`.",
        ExperimentalWarning,
        stacklevel=2,
    )
    return _vars[name]
