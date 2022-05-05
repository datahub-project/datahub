import logging
import re
from typing import Any, Dict, Optional

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import OwnerType


class Constants:
    ADD_TAG_OPERATION = "add_tag"
    ADD_TERM_OPERATION = "add_term"
    ADD_OWNER_OPERATION = "add_owner"
    OPERATION = "operation"
    OPERATION_CONFIG = "config"
    TAG = "tag"
    TERM = "term"
    OWNER_TYPE = "owner_type"
    MATCH = "match"
    USER_OWNER = "user"
    GROUP_OWNER = "group"
    OPERAND_DATATYPE_SUPPORTED = [int, bool, str, float]


class OperationProcessor:
    """
    A general class that processes a dictionary of properties and operations defined on it.
    An action is defined over a property with a specific match condition. If the condition is satisfied then the
    operation is carried out.
    Supported operations in this context include creating tags, terms or owners.
    For e.g below is a sample definition of two operations in yaml format
    operation_def:
      business_owner:
        match: ".*"
        operation: "add_owner"
        config:
          owner_type: "user"
      has_pii:
        match: true
        operation: "add_tag"
        config:
          tag: "has_pii"

    The raw input properties can be -
    "meta": {
        "business_owner": "jdoe.lastnew223@gmail.com",
        "has_pii": true,
      }
    If the match clause of both operations are satisfied on the raw properties a tag and a term aspect
    will be returned for further processing.
    """

    operation_defs: Dict[str, Dict] = {}
    logger = logging.getLogger(__name__)
    tag_prefix: str = ""

    def __init__(
        self,
        operation_defs: Dict[str, Dict],
        tag_prefix: str = "",
        owner_source_type: str = None,
        strip_owner_email_id: bool = False,
    ):
        self.operation_defs = operation_defs
        self.tag_prefix = tag_prefix
        self.strip_owner_email_id = strip_owner_email_id
        self.owner_source_type = owner_source_type

    def process(self, raw_props: Dict[str, Any]) -> Dict[str, Any]:
        # Defining the following local variables -
        # operations_map - the final resulting map when operations are processed.
        # Against each operation the values to be applied are stored.
        # for e.g "tag_operation" -> set("has_pii", "external")
        # operation_key : the property on which an operation is defined in the defs
        # operation config: map which contains the parameters to carry out that operation.
        # For e.g for add_tag operation config will have the tag value.
        # operation_type: the type of operation (add_tag, add_term, etc.)
        aspect_map: Dict[str, Any] = {}  # map of aspect name to aspect object
        try:
            operations_map: Dict[str, set] = {}
            for operation_key in self.operation_defs:
                operation_type = self.operation_defs.get(operation_key, {}).get(
                    Constants.OPERATION
                )
                operation_config = self.operation_defs.get(operation_key, {}).get(
                    Constants.OPERATION_CONFIG
                )
                if not operation_type or not operation_config:
                    continue
                if self.is_match(
                    self.operation_defs[operation_key][Constants.MATCH],
                    raw_props.get(operation_key),
                ):
                    operation = self.get_operation_value(
                        operation_key, operation_type, operation_config, raw_props
                    )
                    if operation:
                        operations_value_set: set = operations_map.get(
                            operation_type, set()
                        )
                        operations_value_set.add(operation)
                        operations_map[operation_type] = operations_value_set
            aspect_map = self.convert_to_aspects(operations_map)
        except Exception as e:
            self.logger.error("Error while processing operation defs over raw_props", e)
        return aspect_map

    def convert_to_aspects(self, operation_map: Dict[str, set]) -> Dict[str, Any]:
        aspect_map: Dict[str, Any] = {}
        if Constants.ADD_TAG_OPERATION in operation_map:
            tag_aspect = mce_builder.make_global_tag_aspect_with_tag_list(
                sorted(operation_map[Constants.ADD_TAG_OPERATION])
            )
            aspect_map[Constants.ADD_TAG_OPERATION] = tag_aspect
        if Constants.ADD_OWNER_OPERATION in operation_map:
            owner_aspect = mce_builder.make_ownership_aspect_from_urn_list(
                sorted(operation_map[Constants.ADD_OWNER_OPERATION]),
                self.owner_source_type,
            )
            aspect_map[Constants.ADD_OWNER_OPERATION] = owner_aspect
        if Constants.ADD_TERM_OPERATION in operation_map:
            term_aspect = mce_builder.make_glossary_terms_aspect_from_urn_list(
                sorted(operation_map[Constants.ADD_TERM_OPERATION])
            )
            aspect_map[Constants.ADD_TERM_OPERATION] = term_aspect
        return aspect_map

    def get_operation_value(
        self,
        operation_key: str,
        operation_type: str,
        operation_config: Dict,
        raw_props: Dict,
    ) -> Optional[str]:
        match_regexp = r"{{\s*\$match\s*}}"

        if (
            operation_type == Constants.ADD_TAG_OPERATION
            and operation_config[Constants.TAG]
        ):
            tag = operation_config[Constants.TAG]
            if isinstance(raw_props[operation_key], str):
                tag = re.sub(
                    match_regexp, raw_props[operation_key], tag, 0, re.MULTILINE
                )

            if self.tag_prefix:
                tag = self.tag_prefix + tag
            return tag
        elif (
            operation_type == Constants.ADD_OWNER_OPERATION
            and operation_config[Constants.OWNER_TYPE]
        ):
            owner_id = raw_props[operation_key]
            if self.strip_owner_email_id:
                owner_id = self.sanitize_owner_ids(owner_id)
            if operation_config[Constants.OWNER_TYPE] == Constants.USER_OWNER:
                return mce_builder.make_owner_urn(owner_id, OwnerType.USER)
            elif operation_config[Constants.OWNER_TYPE] == Constants.GROUP_OWNER:
                return mce_builder.make_owner_urn(owner_id, OwnerType.GROUP)
        elif (
            operation_type == Constants.ADD_TERM_OPERATION
            and operation_config[Constants.TERM]
        ):
            term = operation_config[Constants.TERM]
            if isinstance(raw_props[operation_key], str):
                term = re.sub(
                    match_regexp, raw_props[operation_key], term, 0, re.MULTILINE
                )
            return mce_builder.make_term_urn(term)
        return None

    def sanitize_owner_ids(self, owner_id: str) -> str:
        if owner_id.__contains__("@"):
            owner_id = owner_id[0 : owner_id.index("@")]
        return owner_id

    def is_match(self, match_clause: Any, raw_props_value: Any) -> bool:
        # function to check if a match clause is satisfied to a value.
        is_matching: bool
        if type(raw_props_value) not in Constants.OPERAND_DATATYPE_SUPPORTED or type(
            raw_props_value
        ) != type(match_clause):
            is_matching = False
        elif type(raw_props_value) == str:
            is_matching = True if re.match(match_clause, raw_props_value) else False
        else:
            is_matching = match_clause == raw_props_value
        return is_matching
