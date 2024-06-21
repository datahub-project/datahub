import logging
from typing import Any, Dict

from liquid import Undefined
from liquid.exceptions import LiquidSyntaxError

from datahub.ingestion.source.looker.looker_liquid_tag import (
    CustomTagException,
    create_template,
)
from datahub.ingestion.source.looker.str_functions import (
    remove_extra_spaces_and_newlines,
)

logger = logging.getLogger(__name__)


def resolve_liquid_variable(text: str, liquid_variable: Dict[Any, Any]) -> str:
    # Set variable value to NULL if not present in liquid_variable dictionary
    Undefined.__str__ = lambda instance: "NULL"  # type: ignore
    try:
        # Resolve liquid template
        return create_template(text).render(liquid_variable)
    except LiquidSyntaxError as e:
        logger.warning(f"Unsupported liquid template encountered. error [{e.message}]")
        # TODO: There are some tag specific to looker and python-liquid library does not understand them. currently
        #  we are not parsing such liquid template.
        #
        # See doc: https://cloud.google.com/looker/docs/templated-filters and look for { % condition region %}
        # order.region { % endcondition %}
    except CustomTagException as e:
        logger.warning(e)
        logger.debug(e, exc_info=e)

    return text


def resolve_liquid_variable_in_view_dict(
    raw_view: dict, liquid_variable: Dict[Any, Any]
) -> None:
    if "views" not in raw_view:
        return

    for view in raw_view["views"]:
        if "sql_table_name" in view:
            view["sql_table_name"] = resolve_liquid_variable(
                text=remove_extra_spaces_and_newlines(view["sql_table_name"]),
                liquid_variable=liquid_variable,
            )

        if "derived_table" in view and "sql" in view["derived_table"]:
            # In sql we don't need to remove the extra spaces as sql parser takes care of extra spaces and \n
            # while generating URN from sql
            view["derived_table"]["sql"] = resolve_liquid_variable(
                text=view["derived_table"]["sql"], liquid_variable=liquid_variable
            )
