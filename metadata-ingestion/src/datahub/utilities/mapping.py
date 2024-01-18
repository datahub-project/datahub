import contextlib
import logging
import operator
import re
import time
from functools import reduce
from typing import Any, Dict, List, Mapping, Match, Optional, Union, cast

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import OwnerType
from datahub.metadata.schema_classes import (
    AuditStampClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipTypeClass,
)

logger = logging.getLogger(__name__)


def _get_best_match(the_match: Match, group_name: str) -> str:
    with contextlib.suppress(IndexError):
        return the_match.group(group_name)

    with contextlib.suppress(IndexError):
        return the_match.group(1)

    return the_match.group(0)


_match_regexp = re.compile(r"{{\s*\$match\s*}}", flags=re.MULTILINE)


def _insert_match_value(original_value: str, match_value: str) -> str:
    """
    If the original value is something like "foo{{ $match }}bar", then we insert the match value
    e.g. "foo<match_value>bar". Otherwise, it will leave the original value unchanged.
    """
    return _match_regexp.sub(match_value, original_value)


class Constants:
    ADD_DOC_LINK_OPERATION = "add_doc_link"
    ADD_TAG_OPERATION = "add_tag"
    ADD_TERM_OPERATION = "add_term"
    ADD_TERMS_OPERATION = "add_terms"
    ADD_OWNER_OPERATION = "add_owner"
    OPERATION = "operation"
    OPERATION_CONFIG = "config"
    TAG = "tag"
    TERM = "term"
    DOC_LINK = "link"
    DOC_DESCRIPTION = "description"
    OWNER_TYPE = "owner_type"
    OWNER_CATEGORY = "owner_category"
    MATCH = "match"
    USER_OWNER = "user"
    GROUP_OWNER = "group"
    OPERAND_DATATYPE_SUPPORTED = [int, bool, str, float]
    TAG_PARTITION_KEY = "PARTITION_KEY"
    TAG_DIST_KEY = "DIST_KEY"
    TAG_SORT_KEY = "SORT_KEY"
    SEPARATOR = "separator"


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
    tag_prefix: str = ""

    def __init__(
        self,
        operation_defs: Dict[str, Dict],
        tag_prefix: str = "",
        owner_source_type: Optional[str] = None,
        strip_owner_email_id: bool = False,
        match_nested_props: bool = False,
    ):
        self.operation_defs = operation_defs
        self.tag_prefix = tag_prefix
        self.strip_owner_email_id = strip_owner_email_id
        self.owner_source_type = owner_source_type
        self.match_nested_props = match_nested_props

    def process(self, raw_props: Mapping[str, Any]) -> Dict[str, Any]:
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
            operations_map: Dict[str, Union[set, list]] = {}
            for operation_key in self.operation_defs:
                operation_type = self.operation_defs.get(operation_key, {}).get(
                    Constants.OPERATION
                )
                operation_config = self.operation_defs.get(operation_key, {}).get(
                    Constants.OPERATION_CONFIG
                )
                if not operation_type or not operation_config:
                    continue
                raw_props_value = raw_props.get(operation_key)
                if not raw_props_value and self.match_nested_props:
                    try:
                        raw_props_value = reduce(
                            operator.getitem, operation_key.split("."), raw_props
                        )
                    except KeyError:
                        pass

                maybe_match = self.get_match(
                    self.operation_defs[operation_key][Constants.MATCH],
                    raw_props_value,
                )
                if maybe_match is not None:
                    operation = self.get_operation_value(
                        operation_key, operation_type, operation_config, maybe_match
                    )
                    if operation_type == Constants.ADD_TERMS_OPERATION:
                        # add_terms operation is a special case where the operation value is a list of terms.
                        # We want to aggregate these values with the add_term operation.
                        operation_type = Constants.ADD_TERM_OPERATION

                    if operation:
                        if isinstance(operation, (str, list)):
                            operations_value_set = operations_map.get(
                                operation_type, set()
                            )
                            if isinstance(operation, list):
                                operations_value_set.update(operation)  # type: ignore
                            else:
                                operations_value_set.add(operation)  # type: ignore
                            operations_map[operation_type] = operations_value_set
                        else:
                            operations_value_list = operations_map.get(
                                operation_type, list()
                            )
                            operations_value_list.append(operation)  # type: ignore
                            operations_map[operation_type] = operations_value_list
            aspect_map = self.convert_to_aspects(operations_map)
        except Exception as e:
            logger.error(f"Error while processing operation defs over raw_props: {e}")
        return aspect_map

    def convert_to_aspects(
        self, operation_map: Dict[str, Union[set, list]]
    ) -> Dict[str, Any]:
        aspect_map: Dict[str, Any] = {}

        if Constants.ADD_TAG_OPERATION in operation_map:
            tag_aspect = mce_builder.make_global_tag_aspect_with_tag_list(
                sorted(operation_map[Constants.ADD_TAG_OPERATION])
            )
            aspect_map[Constants.ADD_TAG_OPERATION] = tag_aspect
        if Constants.ADD_OWNER_OPERATION in operation_map:
            owner_aspect = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=x.get("urn"),
                        type=x.get("category"),
                        typeUrn=x.get("categoryUrn"),
                        source=OwnershipSourceClass(type=self.owner_source_type)
                        if self.owner_source_type
                        else None,
                    )
                    for x in sorted(
                        operation_map[Constants.ADD_OWNER_OPERATION],
                        key=lambda x: x["urn"],
                    )
                ]
            )
            aspect_map[Constants.ADD_OWNER_OPERATION] = owner_aspect

        if Constants.ADD_TERM_OPERATION in operation_map:
            term_aspect = mce_builder.make_glossary_terms_aspect_from_urn_list(
                sorted(operation_map[Constants.ADD_TERM_OPERATION])
            )
            aspect_map[Constants.ADD_TERM_OPERATION] = term_aspect

        if Constants.ADD_DOC_LINK_OPERATION in operation_map:
            try:
                if len(
                    operation_map[Constants.ADD_DOC_LINK_OPERATION]
                ) == 1 and isinstance(
                    operation_map[Constants.ADD_DOC_LINK_OPERATION], list
                ):
                    docs_dict = cast(
                        List[Dict], operation_map[Constants.ADD_DOC_LINK_OPERATION]
                    )[0]
                    if "description" not in docs_dict or "link" not in docs_dict:
                        raise Exception(
                            "Documentation_link meta_mapping config needs a description key and a link key"
                        )

                    now = int(time.time() * 1000)  # milliseconds since epoch
                    institutional_memory_element = InstitutionalMemoryMetadataClass(
                        url=docs_dict["link"],
                        description=docs_dict["description"],
                        createStamp=AuditStampClass(
                            time=now, actor="urn:li:corpuser:ingestion"
                        ),
                    )

                    # create a new institutional memory aspect
                    institutional_memory_aspect = InstitutionalMemoryClass(
                        elements=[institutional_memory_element]
                    )

                    aspect_map[
                        Constants.ADD_DOC_LINK_OPERATION
                    ] = institutional_memory_aspect
                else:
                    raise Exception(
                        f"Expected 1 item of type list for the documentation_link meta_mapping config,"
                        f" received type of {type(operation_map[Constants.ADD_DOC_LINK_OPERATION])}"
                        f", and size of {len(operation_map[Constants.ADD_DOC_LINK_OPERATION])}."
                    )

            except Exception as e:
                logger.error(
                    f"Error while constructing aspect for documentation link and description : {e}"
                )

        return aspect_map

    def get_operation_value(
        self,
        operation_key: str,
        operation_type: str,
        operation_config: Dict,
        match: Match,
    ) -> Optional[Union[str, Dict, List[str]]]:
        if (
            operation_type == Constants.ADD_TAG_OPERATION
            and operation_config[Constants.TAG]
        ):
            tag = operation_config[Constants.TAG]
            tag = _insert_match_value(tag, _get_best_match(match, "tag"))

            if self.tag_prefix:
                tag = self.tag_prefix + tag
            return tag
        elif (
            operation_type == Constants.ADD_OWNER_OPERATION
            and operation_config[Constants.OWNER_TYPE]
        ):
            owner_id = _get_best_match(match, "owner")
            owner_category = (
                operation_config.get(Constants.OWNER_CATEGORY)
                or OwnershipTypeClass.DATAOWNER
            )
            owner_category_urn = None
            if owner_category.startswith("urn:li:"):
                owner_category_urn = owner_category
                owner_category = OwnershipTypeClass.DATAOWNER
            else:
                owner_category = owner_category.upper()
            if self.strip_owner_email_id:
                owner_id = self.sanitize_owner_ids(owner_id)
            if operation_config[Constants.OWNER_TYPE] == Constants.USER_OWNER:
                return {
                    "urn": mce_builder.make_owner_urn(owner_id, OwnerType.USER),
                    "category": owner_category,
                    "categoryUrn": owner_category_urn,
                }
            elif operation_config[Constants.OWNER_TYPE] == Constants.GROUP_OWNER:
                return {
                    "urn": mce_builder.make_owner_urn(owner_id, OwnerType.GROUP),
                    "category": owner_category,
                    "categoryUrn": owner_category_urn,
                }
        elif (
            operation_type == Constants.ADD_TERM_OPERATION
            and operation_config[Constants.TERM]
        ):
            term = operation_config[Constants.TERM]
            term = _insert_match_value(term, _get_best_match(match, "term"))
            return mce_builder.make_term_urn(term)
        elif (
            operation_type == Constants.ADD_DOC_LINK_OPERATION
            and operation_config[Constants.DOC_LINK]
            and operation_config[Constants.DOC_DESCRIPTION]
        ):
            link = operation_config[Constants.DOC_LINK]
            link = _insert_match_value(link, _get_best_match(match, "link"))
            description = operation_config[Constants.DOC_DESCRIPTION]
            return {"link": link, "description": description}

        elif operation_type == Constants.ADD_TERMS_OPERATION:
            separator = operation_config.get(Constants.SEPARATOR, ",")
            captured_terms = match.group(0)
            return [
                mce_builder.make_term_urn(term.strip())
                for term in captured_terms.split(separator)
                if term.strip()
            ]
        return None

    def sanitize_owner_ids(self, owner_id: str) -> str:
        if owner_id.__contains__("@"):
            owner_id = owner_id[0 : owner_id.index("@")]
        return owner_id

    def get_match(self, match_clause: Any, raw_props_value: Any) -> Optional[Match]:
        # function to check if a match clause is satisfied to a value.
        if not any(
            isinstance(raw_props_value, t) for t in Constants.OPERAND_DATATYPE_SUPPORTED
        ) or not isinstance(raw_props_value, type(match_clause)):
            return None
        elif isinstance(raw_props_value, str):
            return re.match(match_clause, raw_props_value)
        else:
            return re.match(str(match_clause), str(raw_props_value))
