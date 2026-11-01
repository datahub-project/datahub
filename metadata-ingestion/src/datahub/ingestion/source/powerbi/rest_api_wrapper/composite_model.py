import re
from typing import Dict, Optional
from urllib.parse import unquote

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    RemoteModelReference,
)

# Power BI Desktop names the shared expression of a "DirectQuery to Power BI
# semantic model" connection `DirectQuery to AS - <Model Name>`.
REMOTE_MODEL_EXPRESSION_PREFIX = "DirectQuery to AS - "

# `AnalysisServices.Database("powerbi://api.powerbi.com/v1.0/myorg/<Workspace>",
# "<Model>"[, [Catalog="...", ...]])`. The URL's last path segment is the
# workspace name; Desktop URL-encodes it on some versions.
ANALYSIS_SERVICES_DATABASE_RE = re.compile(
    r"AnalysisServices\.Database\(\s*"
    r'"(?P<url>[^"]+)"\s*,\s*'
    r'"(?P<database>[^"]+)"'
    r"(?:\s*,\s*\[(?P<options>[^\]]*)\])?"
)
CATALOG_OPTION_RE = re.compile(r'Catalog\s*=\s*"([^"]+)"')
# A dataset id as the XMLA endpoint exposes it: the bare GUID or the
# `sobe_wowvirtualserver-<guid>` catalog name of a Power BI dataset.
DATASET_ID_RE = re.compile(
    r"^(?:sobe_wowvirtualserver-)?"
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$"
)


def _dataset_id(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    match = DATASET_ID_RE.match(value.strip())
    return match.group(1) if match else None


def _workspace_name(url: str) -> Optional[str]:
    segment = url.rstrip("/").rsplit("/", 1)[-1]
    name = unquote(segment).strip()
    return name or None


def parse_remote_model_reference(
    expression_name: str, expression: str
) -> Optional[RemoteModelReference]:
    """The semantic model a `DirectQuery to AS` shared expression connects
    to, or None when the expression is not an AnalysisServices.Database call
    over a Power BI workspace URL.

    The model name is the call's database argument; when that argument is
    itself a dataset id, the name is taken from the expression's own
    `DirectQuery to AS - <Model Name>` title. A dataset id is kept from the
    `Catalog` option or the database argument when either carries one."""
    match = ANALYSIS_SERVICES_DATABASE_RE.search(expression)
    if match is None:
        return None
    url = match.group("url").strip()
    if "://" not in url:
        return None
    workspace_name = _workspace_name(url)
    if workspace_name is None:
        return None

    database = match.group("database").strip()
    options = match.group("options") or ""
    catalog_match = CATALOG_OPTION_RE.search(options)
    catalog = catalog_match.group(1) if catalog_match else None
    dataset_id = _dataset_id(catalog) or _dataset_id(database)

    if _dataset_id(database) is None:
        model_name = database
    elif expression_name.startswith(REMOTE_MODEL_EXPRESSION_PREFIX):
        model_name = expression_name[len(REMOTE_MODEL_EXPRESSION_PREFIX) :].strip()
    else:
        model_name = database
    if not model_name:
        return None
    return RemoteModelReference(
        workspace_name=workspace_name,
        model_name=model_name,
        dataset_id=dataset_id,
    )


def remote_models_from_expressions(
    expressions: Dict[str, str],
) -> Dict[str, RemoteModelReference]:
    """Shared-expression name -> remote model, for the expressions of one
    model that connect to another Power BI semantic model."""
    remote_models: Dict[str, RemoteModelReference] = {}
    for name, expression in expressions.items():
        reference = parse_remote_model_reference(name, expression)
        if reference is not None:
            remote_models[name] = reference
    return remote_models


def remote_model_key(workspace_name: str, model_name: str) -> str:
    """Case-folded (workspace, model) key of the run-wide name index; Power
    BI compares both names case-insensitively."""
    return f"{workspace_name.strip().casefold()}/{model_name.strip().casefold()}"
