import json
import logging
from typing import Dict, List, Optional

from datahub.ingestion.autogen_ui.form_model import ConnectorForm
from datahub.ingestion.autogen_ui.inference import build_form
from datahub.ingestion.source.source_registry import source_registry

logger = logging.getLogger(__name__)

BUNDLE_VERSION = 1


def generate_form(connector: str) -> Optional[ConnectorForm]:
    try:
        source_cls = source_registry.get(connector)
    except Exception:
        logger.warning(f"Could not resolve source class for connector '{connector}'")
        return None

    get_config_class = getattr(source_cls, "get_config_class", None)
    if get_config_class is None:
        logger.warning(f"Connector '{connector}' has no get_config_class()")
        return None

    try:
        config_class = get_config_class()

        display_name = connector
        get_platform_name = getattr(source_cls, "get_platform_name", None)
        if get_platform_name is not None:
            try:
                platform_name = get_platform_name()
            except Exception:
                platform_name = None
            if platform_name:
                display_name = platform_name

        return build_form(connector, display_name, config_class)
    except Exception:
        logger.warning(
            f"Could not build UI form for connector '{connector}'", exc_info=True
        )
        return None


def generate_bundle(connectors: List[str]) -> Dict[str, object]:
    forms = []
    for connector in connectors:
        form = generate_form(connector)
        if form is not None:
            forms.append(form.model_dump(exclude_none=True))
    return {"version": BUNDLE_VERSION, "forms": forms}


def write_ui_form_bundle(output_path: str, connectors: List[str]) -> None:
    bundle = generate_bundle(connectors)
    with open(output_path, "w") as f:
        json.dump(bundle, f, indent=2)
        f.write("\n")
