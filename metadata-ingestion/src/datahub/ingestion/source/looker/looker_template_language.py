import logging
import re
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, List, Set

from deepmerge import always_merger
from liquid import Undefined
from liquid.exceptions import LiquidSyntaxError

from datahub.ingestion.source.looker.looker_constant import (
    DATAHUB_TRANSFORMED_SQL,
    DATAHUB_TRANSFORMED_SQL_TABLE_NAME,
    DERIVED_TABLE,
    NAME,
    SQL,
    SQL_TABLE_NAME,
    dev,
    prod,
)
from datahub.ingestion.source.looker.looker_liquid_tag import (
    CustomTagException,
    create_template,
)
from datahub.ingestion.source.looker.lookml_config import (
    DERIVED_VIEW_PATTERN,
    LookMLSourceConfig,
)

logger = logging.getLogger(__name__)


class SpecialVariable:
    SPECIAL_VARIABLE_PATTERN: ClassVar[
        str
    ] = r"\b\w+(\.\w+)*\._(is_selected|in_query|is_filtered)\b"
    liquid_variable: dict

    def __init__(self, liquid_variable):
        self.liquid_variable = liquid_variable

    def _create_new_liquid_variables_with_default(
        self,
        variables: Set[str],
    ) -> dict:
        new_dict = {**self.liquid_variable}

        for variable in variables:
            keys = variable.split(
                "."
            )  # variable is defined as view._is_selected or view.field_name._is_selected

            current_dict: dict = new_dict

            for key in keys[:-1]:

                if key not in current_dict:
                    current_dict[key] = {}

                current_dict = current_dict[key]

            if keys[-1] not in current_dict:
                current_dict[keys[-1]] = True

        logger.debug("added special variables in liquid_variable dictionary")

        return new_dict

    def liquid_variable_with_default(self, text: str) -> dict:
        variables: Set[str] = set(
            [
                text[m.start() : m.end()]
                for m in re.finditer(SpecialVariable.SPECIAL_VARIABLE_PATTERN, text)
            ]
        )

        # if set is empty then no special variables are found.
        if not variables:
            return self.liquid_variable

        return self._create_new_liquid_variables_with_default(variables=variables)


def resolve_liquid_variable(text: str, liquid_variable: Dict[Any, Any]) -> str:
    # Set variable value to NULL if not present in liquid_variable dictionary
    Undefined.__str__ = lambda instance: "NULL"  # type: ignore
    try:
        # See is there any special boolean variables are there in the text like _in_query, _is_selected, and
        # _is_filtered. Refer doc for more information
        # https://cloud.google.com/looker/docs/liquid-variable-reference#usage_of_in_query_is_selected_and_is_filtered
        # update in liquid_variable with there default values
        liquid_variable = SpecialVariable(liquid_variable).liquid_variable_with_default(
            text
        )
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


class LookMLViewTransformer(ABC):
    source_config: LookMLSourceConfig

    def __init__(self, source_config: LookMLSourceConfig):
        self.source_config = source_config

    @abstractmethod
    def transform(self, view: dict) -> dict:
        pass


class AddTransformedKeys(LookMLViewTransformer):
    """
    All transformers work either on sql_table_name or derived_table.sql attribute of views and
    hence copy them to another key to perform transformation action.
    """

    def transform(self, view: dict) -> dict:
        if SQL_TABLE_NAME in view:
            return {DATAHUB_TRANSFORMED_SQL_TABLE_NAME: view[SQL_TABLE_NAME]}

        if DERIVED_TABLE in view and SQL in view[DERIVED_TABLE]:
            return {DERIVED_TABLE: {DATAHUB_TRANSFORMED_SQL: view[DERIVED_TABLE][SQL]}}

        return {}


class LiquidVariableTransformer(LookMLViewTransformer):
    """
    Replace the liquid variables with their values.
    """

    def transform(self, view: dict) -> dict:
        if DATAHUB_TRANSFORMED_SQL_TABLE_NAME in view:
            return {
                DATAHUB_TRANSFORMED_SQL_TABLE_NAME: resolve_liquid_variable(
                    text=view[DATAHUB_TRANSFORMED_SQL_TABLE_NAME],
                    liquid_variable=self.source_config.liquid_variable,
                )
            }  # keeping original sql_table_name as is to avoid any visualization issue later

        if DERIVED_TABLE in view and DATAHUB_TRANSFORMED_SQL in view[DERIVED_TABLE]:
            # In sql we don't need to remove the extra spaces as sql parser takes care of extra spaces and \n
            # while generating URN from sql
            return {
                DERIVED_TABLE: {
                    DATAHUB_TRANSFORMED_SQL: resolve_liquid_variable(
                        text=view[DERIVED_TABLE][DATAHUB_TRANSFORMED_SQL],
                        liquid_variable=self.source_config.liquid_variable,
                    )  # keeping original sql as is, so that on UI sql will be shown same is it is visible on looker
                    # portal
                }
            }

        return {}


class IncompleteSqlTransformer(LookMLViewTransformer):
    """
    lookml view may contain the fragment of sql, however for lineage generation we need a complete sql.
    IncompleteSqlTransformer will complete the view's sql.
    """

    def transform(self, view: dict) -> dict:
        if (
            DERIVED_TABLE not in view
            or DATAHUB_TRANSFORMED_SQL not in view[DERIVED_TABLE]
        ):
            return {}

        # Looker supports sql fragments that omit the SELECT and FROM parts of the query
        # Add those in if we detect that it is missing
        sql_query: str = view[DERIVED_TABLE][DATAHUB_TRANSFORMED_SQL]

        if not re.search(r"SELECT\s", sql_query, flags=re.I):
            # add a SELECT clause at the beginning
            sql_query = f"SELECT {sql_query}"

        if not re.search(r"FROM\s", sql_query, flags=re.I):
            # add a FROM clause at the end
            sql_query = f"{sql_query} FROM {view['name']}"

        return {"derived_table": {"datahub_transformed_sql": sql_query}}


class DropDerivedViewPatternTransformer(LookMLViewTransformer):
    """
    drop ${} from datahub_transformed_sql_table_name and  view["derived_table"]["datahub_transformed_sql_table_name"] values.

    Example: transform ${view_name}.sql_table_name to view_name.sql_table_name
    """

    def transform(self, view: dict) -> dict:
        if DATAHUB_TRANSFORMED_SQL_TABLE_NAME in view:
            return {
                DATAHUB_TRANSFORMED_SQL_TABLE_NAME: re.sub(
                    DERIVED_VIEW_PATTERN,
                    r"\1",
                    view[DATAHUB_TRANSFORMED_SQL_TABLE_NAME],
                )
            }

        if DERIVED_TABLE in view and DATAHUB_TRANSFORMED_SQL in view[DERIVED_TABLE]:
            return {
                DERIVED_TABLE: {
                    DATAHUB_TRANSFORMED_SQL: re.sub(
                        DERIVED_VIEW_PATTERN,
                        r"\1",
                        view[DERIVED_TABLE][DATAHUB_TRANSFORMED_SQL],
                    )
                }
            }

        return {}


class LookMlIfCommentTransformer(LookMLViewTransformer):
    """
    Evaluate the looker -- if -- comments.
    """

    evaluate_to_true_regx: str
    remove_if_comment_line_regx: str

    def __init__(self, source_config: LookMLSourceConfig):
        super().__init__(source_config=source_config)

        # This regx will keep whatever after -- if looker_environment --
        self.evaluate_to_true_regx = r"-- if {} --".format(
            self.source_config.looker_environment
        )

        # It will remove all other lines starts with -- if ... --
        self.remove_if_comment_line_regx = r"-- if {} --.*?(?=\n|-- if|$)".format(
            dev if self.source_config.looker_environment.lower() == prod else prod
        )

    def _apply_regx(self, value: str) -> str:
        result: str = re.sub(
            self.remove_if_comment_line_regx, "", value, flags=re.IGNORECASE | re.DOTALL
        )

        # Remove '-- if prod --' but keep the rest of the line
        result = re.sub(self.evaluate_to_true_regx, "", result, flags=re.IGNORECASE)

        return result

    def transform(self, view: dict) -> dict:

        if DATAHUB_TRANSFORMED_SQL_TABLE_NAME in view:
            return {
                DATAHUB_TRANSFORMED_SQL_TABLE_NAME: self._apply_regx(
                    value=view[DATAHUB_TRANSFORMED_SQL_TABLE_NAME]
                )
            }

        if DERIVED_TABLE in view and DATAHUB_TRANSFORMED_SQL in view[DERIVED_TABLE]:
            return {
                DERIVED_TABLE: {
                    DATAHUB_TRANSFORMED_SQL: self._apply_regx(
                        value=view[DERIVED_TABLE][DATAHUB_TRANSFORMED_SQL]
                    )
                }
            }

        return {}


class TransformedLookMlView:
    transformers: List[LookMLViewTransformer]
    view_dict: dict
    transformed_dict: dict

    def __init__(
        self,
        transformers: List[LookMLViewTransformer],
        view_dict: dict,
    ):
        self.transformers = transformers
        self.view_dict = view_dict
        self.transformed_dict = {}

    def view(self) -> dict:
        if self.transformed_dict:
            return self.transformed_dict

        self.transformed_dict = {**self.view_dict}

        logger.debug(f"Processing view {self.view_dict[NAME]}")

        for transformer in self.transformers:
            logger.debug(f"Applying transformer {transformer.__class__.__name__}")

            self.transformed_dict = always_merger.merge(
                self.transformed_dict, transformer.transform(self.transformed_dict)
            )

        return self.transformed_dict


def process_lookml_template_language(
    source_config: LookMLSourceConfig,
    view_lkml_file_dict: dict,
) -> None:
    if "views" not in view_lkml_file_dict:
        return

    transformers: List[LookMLViewTransformer] = [
        AddTransformedKeys(
            source_config=source_config
        ),  # Add new key to perform transformation action
        LookMlIfCommentTransformer(
            source_config=source_config
        ),  # First evaluate the -- if -- comments
        LiquidVariableTransformer(
            source_config=source_config
        ),  # Now resolve liquid variables
        DropDerivedViewPatternTransformer(
            source_config=source_config
        ),  # Remove any ${} symbol
        IncompleteSqlTransformer(
            source_config=source_config
        ),  # complete any incomplete sql
    ]

    transformed_views: List[dict] = []

    for view in view_lkml_file_dict["views"]:
        transformed_views.append(
            TransformedLookMlView(transformers=transformers, view_dict=view).view()
        )

    view_lkml_file_dict["views"] = transformed_views
