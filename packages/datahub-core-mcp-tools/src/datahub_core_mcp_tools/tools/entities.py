"""Entity retrieval: get_entities."""

import inspect
from dataclasses import dataclass
from typing import Annotated, List, Literal, Optional, Sequence, cast

from datahub.cli.env_utils import get_boolean_env_variable
from fastmcp.exceptions import ToolError
from loguru import logger
from pydantic import AfterValidator, BeforeValidator, Field
from pydantic_core import from_json

from .. import arg_validation, entity_cleaning, graphql_helpers
from ..read_backend import get_read_backend
from ..result_policy import (
    apply_decision,
    attach_header,
    configuration_response,
    current_decisions,
    is_configuration,
    trust_labeled,
)
from ..schema_matching import SchemaFieldMatcher, SubstringMatcher
from ..sibling_merge import merge_siblings
from ..version_requirements import read_only

# Opt-in: structured properties on every schema field cost tokens on every column.
SCHEMA_FIELD_EXTRA_ATTRS_IN_GET_ENTITIES = get_boolean_env_variable(
    "SCHEMA_FIELD_EXTRA_ATTRS_IN_GET_ENTITIES", default=False
)

# `fields` is a strict allowlist: naming `ownership` returns the core fields plus
# ownership and nothing else. Measured on real payloads, schemaMetadata alone is ~40%
# of a response, and an additive `fields` could never leave it out.
#
# The names are published as a schema enum: without one, callers invented plausible
# names (groupMembership, statistics, usage) on about a quarter of requests. Only the
# levers are advertised: fields that are fetched but withheld unless named.
_ADVERTISED_FIELDS = (
    "description",
    "documentation",
    "properties",
    "schemaMetadata",
    "ownership",
    "tags",
    "glossaryTerms",
    "domain",
    "structuredProperties",
    "statsSummary",
    "relatedDocuments",
    # A dataset's data contract: its URN and state.
    "contract",
    "health",
    "info",
    # An assertion's last 10 completed runs with their observed values.
    "runEvents",
    # Metric edges: lists with no size limit, so a caller has to name them.
    "semanticModel",
    "metricRelationships",
    "metricUpstreams",
    "editableProperties",
    "viewProperties",
    "parentDomains",
    "dataProduct",
    # The datasets a data job declares as its inputs and outputs.
    "inputOutput",
)

# Always returned, so naming one changes nothing. Accepted rather than rejected: a call
# must not fail for asking after data it was about to be handed.
_ALWAYS_PRESENT_FIELDS = frozenset(
    {
        "deprecation",
        "relatedAssets",
        "parentDocument",
        "subTypes",
        "definition",
        "siblings",
    }
)
_ACCEPTED_FIELDS = frozenset(_ADVERTISED_FIELDS) | _ALWAYS_PRESENT_FIELDS


def _validate_field(value: str) -> str:
    """Accept any real field name while the schema advertises only the levers."""
    if value not in _ACCEPTED_FIELDS:
        raise ValueError(
            f"unknown field {value!r}; valid fields: {sorted(_ADVERTISED_FIELDS)}"
        )
    return value


EntityField = Annotated[
    str,
    AfterValidator(_validate_field),
    Field(json_schema_extra={"enum": list(_ADVERTISED_FIELDS)}),
]

# Always returned, so a caller can name and reference what came back. Each entity type
# carries its own identity keys: a query's `subjects`, a structured property's
# `definition`, a chart's `chartId`.
_CORE_FIELDS = frozenset(
    {
        "urn",
        "type",
        "name",
        "platform",
        "url",
        "subType",
        "subTypes",
        "hierarchicalName",
        "chartId",
        "dashboardId",
        "tool",
        "definition",
        "subjects",
        # An agent that cannot see a deprecation recommends the deprecated table.
        "deprecation",
        "error",
        # Sibling URNs (the dbt model behind a warehouse table). Their metadata is
        # merged in, but a caller must still learn the other entity exists.
        "siblings",
        # A Semantic Model Dataset's only link back to its model.
        "semanticModelProperties",
    }
)

# Returned when the caller names nothing. Dropping `properties` from this default
# measured worse in evals: agents made more calls to recover it than it saved.
_DEFAULT_FIELDS = frozenset({"properties"})

_DESCRIPTION_KEY = "description"
# Asked for together with the description: the same question of a different aspect.
_DOCUMENTATION_KEY = "documentation"
_DOCUMENT_URN_PREFIX = "urn:li:document:"
_DATA_CONTRACT_KEY = "contract"
_DATA_CONTRACT_URN_PREFIX = "urn:li:dataContract:"
_DATA_CONTRACT_FIELDS = frozenset({"status", "properties", "result"})

# Name and description live under `info` for these URNs.
_INFO_IDENTITY_PREFIXES = ("urn:li:metric:", "urn:li:semanticModel:")
_LIFTED_INFO_KEYS = frozenset({"name", "description"})

_DescriptionSite = Literal["top", "properties", "info", "editableProperties"]

# The parts of a document's `info` small enough to lift to the top level. `contents`
# stays behind, which is what `contentHint` is for. `relatedDocuments` names the
# doc-to-doc links here and the reverse lookup on assets: one name for "documents
# related to this".
_LIFTED_DOCUMENT_INFO_KEYS = frozenset(
    {
        "title",
        "relatedAssets",
        "relatedDocuments",
        "parentDocument",
        "source",
        "lastModified",
    }
)
# Neither document link list is bounded or paged by the query.
_MAX_DOCUMENT_LINKS = 25


def _describe_omitted(name: str, value: object) -> str:
    """One entry of the omitted-fields manifest; sized only where the size changes
    what the caller does next."""
    if name == "schemaMetadata" and isinstance(value, dict):
        count = len(value.get("fields") or [])
        return f"{name} ({count} columns)" if count else name
    if name == _DOCUMENTATION_KEY and isinstance(value, list):
        # Named as well as sized: nothing else tells an agent that the rest of a
        # description lives under `documentation`.
        chars = sum(map(len, entity_cleaning.documentation_text_of(value)))
        return f"{name} ({chars:,} chars, propagated / AI-generated / doc-extracted description)"
    return name


def _select_schema_columns(schema: dict, columns: Sequence[str]) -> dict:
    """Note when `schema_columns` matched nothing.

    clean_get_entities_response already chose the columns from the whole schema and
    recorded `totalColumns` and `matchedColumns`.
    """
    if not isinstance(schema.get("fields"), list) or not any(
        c and c.strip() for c in columns
    ):
        return schema
    out = {**schema}
    total = out.setdefault("totalColumns", len(schema["fields"]))
    if not out.setdefault("matchedColumns", 0):
        # Otherwise indistinguishable from a dataset with no schema.
        out["note"] = f"no column matched {list(columns)}; {total} available"
    return out


def _has_info_identity(entity: dict) -> bool:
    urn = entity.get("urn")
    return isinstance(urn, str) and urn.startswith(_INFO_IDENTITY_PREFIXES)


def _find_description(entity: dict) -> tuple[Optional[str], _DescriptionSite]:
    """The entity's description and which block holds it.

    A description written in the UI or SDK (`editableProperties`) is what the entity
    page shows, so it wins over the ingested one in `properties`.
    """
    top = entity.get(_DESCRIPTION_KEY)
    if isinstance(top, str) and top:
        return top, "top"
    sites: list[_DescriptionSite] = ["editableProperties", "properties"]
    if _has_info_identity(entity):
        sites.append("info")
    for site in sites:
        block = entity.get(site)
        if isinstance(block, dict):
            nested = block.get(_DESCRIPTION_KEY)
            if isinstance(nested, str) and nested:
                return nested, site
    return None, "top"


def _drop_nested_key(projected: dict, block_key: str, field_key: str) -> None:
    """Remove one key from a nested block, and the block when that empties it."""
    block = projected.get(block_key)
    if not isinstance(block, dict) or field_key not in block:
        return
    trimmed = {key: value for key, value in block.items() if key != field_key}
    if trimmed:
        projected[block_key] = trimmed
    else:
        del projected[block_key]


def _apply_description(projected: dict, entity: dict, wanted: bool) -> Optional[int]:
    """Put the description at the top level, or take it out; its length if removed.

    Never edits a nested block in place: `projected` shares them with `entity`.
    """
    text, _site = _find_description(entity)
    if wanted and text is not None:
        # One place whatever block stored it, so the shape doesn't depend on that.
        projected[_DESCRIPTION_KEY] = text
    elif not wanted:
        projected.pop(_DESCRIPTION_KEY, None)
    _drop_nested_key(projected, "properties", _DESCRIPTION_KEY)
    _drop_nested_key(projected, "editableProperties", _DESCRIPTION_KEY)
    if _has_info_identity(entity):
        _drop_nested_key(projected, "info", _DESCRIPTION_KEY)
    return len(text) if not wanted and text is not None else None


def _requested_fields(
    fields: Optional[Sequence[str]], schema_columns: Optional[Sequence[str]]
) -> set[str]:
    requested = {f.strip() for f in fields} if fields else set()
    if schema_columns:
        # Naming columns implies asking for the schema.
        requested.add("schemaMetadata")
    if fields is None and not schema_columns:
        requested |= _DEFAULT_FIELDS
    return requested


def _restore_own_description(
    entity: dict, text: str, site: _DescriptionSite, wanted: bool
) -> None:
    """Put the entity's own description back after truncate_descriptions capped it.

    Requested, it gets the larger REQUESTED_DESCRIPTION_LENGTH_LIMIT; withheld, it goes
    back whole so project_entity reports its real length in `omittedFields`.
    """
    limit = graphql_helpers.REQUESTED_DESCRIPTION_LENGTH_LIMIT if wanted else len(text)
    holder = entity if site == "top" else entity.get(site)
    if isinstance(holder, dict):
        holder[_DESCRIPTION_KEY] = entity_cleaning.sanitize_and_truncate_description(
            text, limit, dropped_count=True
        )


def _urns_under(entries: object, key: str) -> List[str]:
    """URNs out of a [{<key>: {urn}}] list, the shape both document link fields use."""
    urns: List[str] = []
    for entry in entries if isinstance(entries, list) else []:
        target = entry.get(key) if isinstance(entry, dict) else None
        if isinstance(target, dict) and target.get("urn"):
            urns.append(target["urn"])
    return urns


def _lift_document_info(projected: dict, entity: dict) -> bool:
    """Surface a document's identity and links out of `info`.

    True when `info` held nothing else, which keeps it out of `omittedFields`.
    """
    info = entity.get("info")
    if not isinstance(info, dict):
        return False
    if info.get("title") and "name" not in projected:
        projected["name"] = info["title"]
    for key, item_key in (("relatedAssets", "asset"), ("relatedDocuments", "document")):
        urns = _urns_under(info.get(key), item_key)
        if urns and key not in projected:
            projected[key] = urns[:_MAX_DOCUMENT_LINKS]
            if len(urns) > _MAX_DOCUMENT_LINKS:
                projected[f"{key}Total"] = len(urns)
    parent = info.get("parentDocument")
    parent_document = parent.get("document") if isinstance(parent, dict) else None
    if isinstance(parent_document, dict) and parent_document.get("urn"):
        projected["parentDocument"] = parent_document["urn"]
    if isinstance(info.get("source"), dict) and info["source"]:
        # sourceType + externalUrl: the way back to an externally ingested original.
        projected["source"] = info["source"]
    if isinstance(info.get("lastModified"), str):
        projected["lastModified"] = info["lastModified"]
    # `info` is nameable; return what remains rather than the same links twice.
    if isinstance(projected.get("info"), dict):
        remainder = {
            k: v
            for k, v in projected["info"].items()
            if k not in _LIFTED_DOCUMENT_INFO_KEYS
        }
        if remainder:
            projected["info"] = remainder
        else:
            del projected["info"]
    return not any(
        key not in _LIFTED_DOCUMENT_INFO_KEYS and value not in (None, [], {}, "")
        for key, value in info.items()
    )


def _not_returned(
    fields: Optional[Sequence[str]], projected: dict, entity: dict
) -> List[str]:
    """The advertised fields the caller named that this response holds nothing for.

    Without this, a named field that came back empty read as "the entity has none",
    though the query may simply not select it for this entity type.
    """
    missing: List[str] = []
    for name in dict.fromkeys(f.strip() for f in fields or []):
        if name not in _ADVERTISED_FIELDS:
            continue
        if name == _DESCRIPTION_KEY:
            present = _find_description(entity)[0] is not None or entity.get(
                _DOCUMENTATION_KEY
            ) not in (None, [], {}, "")
        else:
            # Checked on the input too: lifts move values out of their blocks.
            present = any(
                source.get(name) not in (None, [], {}, "")
                for source in (projected, entity)
            )
        if not present:
            missing.append(name)
    return missing


def _lift_info_identity(projected: dict, entity: dict) -> bool:
    """Lift `info.name` to the top level; True when `info` held only name and description."""
    if not _has_info_identity(entity):
        return False
    info = entity.get("info")
    if isinstance(info, dict):
        name = info.get("name")
        if isinstance(name, str) and name and "name" not in projected:
            projected["name"] = name
    _drop_nested_key(projected, "info", "name")
    if not isinstance(info, dict):
        return False
    return not any(
        key not in _LIFTED_INFO_KEYS and value not in (None, [], {}, "")
        for key, value in info.items()
    )


def project_entity(
    entity: dict,
    fields: Optional[Sequence[str]],
    schema_columns: Optional[Sequence[str]] = None,
) -> dict:
    """The core fields plus what the caller named.

    Anything withheld is listed in `omittedFields`, sized where that matters. That
    manifest is what makes a strict default safe: a caller can tell "not requested"
    from "does not exist".
    """
    requested = _requested_fields(fields, schema_columns)
    keep = _CORE_FIELDS | requested
    if _DESCRIPTION_KEY in requested:
        # A propagated-only description lives in `documentation`.
        keep = keep | {_DOCUMENTATION_KEY}
    urn = str(entity.get("urn", ""))
    if urn.startswith("urn:li:query:"):
        # A query's SQL is in `properties.statement`: that block is the entity.
        keep = keep | {"properties"}
    if urn.startswith("urn:li:schemaField:"):
        # A column read by its own URN is read for these.
        keep = keep | {
            "structuredProperties",
            _DOCUMENTATION_KEY,
            "semanticFieldAnnotation",
        }
    if urn.startswith(_DATA_CONTRACT_URN_PREFIX):
        # A contract has no content beyond its state, members and result.
        keep = keep | _DATA_CONTRACT_FIELDS
    projected = {k: v for k, v in entity.items() if k in keep}

    # Some entities carry their name only inside `properties`.
    if "name" not in projected:
        props = entity.get("properties")
        if isinstance(props, dict) and props.get("name"):
            projected["name"] = props["name"]

    info_fully_lifted = _lift_info_identity(projected, entity)
    is_document = urn.startswith(_DOCUMENT_URN_PREFIX)
    if is_document:
        info_fully_lifted = _lift_document_info(projected, entity) or info_fully_lifted

    if schema_columns and isinstance(projected.get("schemaMetadata"), dict):
        projected["schemaMetadata"] = _select_schema_columns(
            projected["schemaMetadata"], schema_columns
        )

    omitted = [
        _describe_omitted(k, v)
        for k, v in entity.items()
        if k not in keep
        and v not in (None, [], {}, "")
        and not (k == "info" and info_fully_lifted)
    ]
    dropped = _apply_description(
        projected, entity, wanted=_DESCRIPTION_KEY in requested
    )
    if dropped is not None:
        # Characters, so the caller can judge whether fetching it is worth a call.
        omitted.insert(0, f"description ({dropped:,} chars)")

    # A withheld body would otherwise read as "this document has no body".
    if is_document and "contents" not in (projected.get("info") or {}):
        info = entity.get("info")
        contents = info.get("contents") if isinstance(info, dict) else None
        if isinstance(contents, dict) and contents.get("text"):
            projected["contentHint"] = (
                "Document body not included here; use grep_documents to read it."
            )

    if omitted:
        projected["omittedFields"] = omitted
    if not_returned := _not_returned(fields, projected, entity):
        projected["notReturnedFields"] = not_returned
    return projected


def _parse_urns(urns: List[str] | str) -> tuple[List[str], bool]:
    """The requested URNs, and whether the caller passed a single URN.

    Some clients send a list as a JSON string. Malformed JSON is not repaired: it is
    taken as one value, which URN validation then rejects with its own message.
    """
    if not isinstance(urns, str):
        return list(urns), False
    stripped = urns.strip()
    if stripped.startswith("["):
        try:
            parsed = from_json(stripped)
        except ValueError:
            return [stripped], True
        if isinstance(parsed, list) and all(isinstance(u, str) for u in parsed):
            return cast(List[str], parsed), False
    return [stripped], True


@dataclass(frozen=True)
class SchemaFieldsTool:
    """The schema-reading tools, bound to a host's column matcher.

    Each server binds its own, so one host's matcher never changes another's.
    """

    matcher: SchemaFieldMatcher

    def _read_entity(
        self,
        urn: str,
        *,
        fields: Optional[Sequence[str]],
        schema_columns: Optional[List[str]],
        named_budget: Optional[int],
    ) -> dict:
        named = {f.strip() for f in fields or []}
        backend = get_read_backend()
        if not backend.exists(urn):
            raise ToolError(f"Entity {urn} not found")
        result = backend.entity(urn, include_contract=_DATA_CONTRACT_KEY in named)
        if result is None:
            raise ToolError(
                f"Entity {urn} exists but no data could be retrieved. This can happen "
                "if the entity has no aspects ingested yet, or if there's a permissions issue."
            )
        result = cast(dict, result)
        merge_siblings(result)

        # One extra round trip per entity, so only when named. Documents are skipped:
        # their doc-to-doc links come from `info` (see _lift_document_info).
        if "relatedDocuments" in named and not urn.startswith(_DOCUMENT_URN_PREFIX):
            try:
                related = backend.related_documents(urn, 0, 10)
                related_entity = cast(dict, related.get("entity") or {})
                if related_entity.get("relatedDocuments"):
                    result["relatedDocuments"] = graphql_helpers.clean_gql_response(
                        related_entity["relatedDocuments"]
                    )
            except Exception as e:
                logger.debug(f"Could not fetch related documents for {urn}: {e}")

        # Read before truncate_descriptions caps every description; the entity's own
        # one is put back under its own limit.
        own_description, description_site = _find_description(result)
        entity_cleaning.truncate_descriptions(result)
        if own_description is not None:
            _restore_own_description(
                result,
                own_description,
                description_site,
                wanted=_DESCRIPTION_KEY in _requested_fields(fields, schema_columns),
            )
        return project_entity(
            entity_cleaning.clean_get_entities_response(
                result,
                include_schema_field_entity_attrs=SCHEMA_FIELD_EXTRA_ATTRS_IN_GET_ENTITIES,
                # Named columns are picked from the whole schema, before the token
                # budget, and get their full definitions.
                selected_columns=schema_columns,
                column_matcher=self.matcher,
                schema_token_budget=named_budget,
            ),
            fields,
            schema_columns,
        )

    @read_only
    @trust_labeled
    def get_entities(
        self,
        urns: Annotated[
            arg_validation.StrListOrStr,
            Field(validation_alias=arg_validation.URNS_ALIASES),
        ],
        fields: Annotated[
            Optional[List[EntityField]],
            BeforeValidator(arg_validation.coerce_str_list),
            Field(validation_alias=arg_validation.FIELDS_ALIASES),
        ] = None,
        schema_columns: Annotated[
            Optional[List[str]], BeforeValidator(arg_validation.coerce_keyword_list)
        ] = None,
        explain_trust: bool = False,
    ) -> List[dict] | dict:
        """Get detailed information about one or more entities by their DataHub URNs.

        IMPORTANT: Pass an array of URNs to retrieve multiple entities in a single call - this is much
        more efficient than calling this tool multiple times. When examining search results, always pass
        an array with the top 3-10 result URNs to compare and find the best match.

        Accepts an array of URNs or a single URN. Supports all entity types including datasets,
        assertions, incidents, dashboards, charts, users, groups, and more. The response fields vary
        based on the entity type. In an array, a URN that cannot be read comes back as
        {"urn", "error"} in its place.

        `fields` is an allowlist: identity always, anything else only when named — including
        the description, which can be long. What was withheld is listed in `omittedFields`
        with its size, so ask again naming it rather than assuming it is missing. A field
        you named that came back with nothing is listed in `notReturnedFields`; that means
        this tool returned no value, not that the entity has none. A named
        description comes back in full unless it is very long; a cut one ends in a
        `[+N chars]` marker. For a wide table name the columns instead of the whole schema:
        schema_columns=["user_id"], matched by column name, best match first. A
        whole-schema dump returns each column's description as a short preview; one
        ending in a `[+N chars]` marker is truncated — name that column in
        schema_columns to get its full definition.

        A description written in the UI or by the SDK is stored in `editableProperties`
        and is returned under `description`. A Semantic Model Dataset includes
        `semanticModelProperties` (its alias and the owning model) with its identity.
        Each column's role and expression are `semanticFieldAnnotation`, returned with
        `schemaMetadata` or `schema_columns`.

        Descriptions written by propagation, by AI, or extracted from a linked document
        live in `documentation`, not `description`. Asking for `description` returns
        both. Each `documentation` entry carries an `origin` — `propagated` and
        `ui_authored` are copies of text a human wrote, `document_extracted` is an
        excerpt of a human-written document about this asset, `ai_generated` is model
        output that may be wrong.

        A dataset's descriptions, tags and glossary terms include those of its siblings
        (e.g. the dbt model behind a warehouse table), merged as the DataHub UI shows them;
        `siblings` lists their URNs. A data job's declared input and output datasets are
        `inputOutput`.

        For an assertion, fields=["info"] returns its definition and fields=["runEvents"]
        its last 10 completed runs with observed values (actualAggValue, rowCount,
        nativeResults, error, severity) and failed/errored counts. Long key/value lists
        are capped; `<name>Total` gives the full count.

        A data contract URN returns its `status` and its member assertions, each with its
        latest completed run (and, on DataHub Cloud, its `assertionStatus` and a rolled-up
        `result`). Judge a contract the way the DataHub contract tab does, in this order:
        `status.state` PENDING means pending (normal for contracts created from YAML);
        otherwise classify each member — by `assertionStatus` when present, else by its
        latest completed run: SUCCESS as PASSING, FAILURE as FAILING, ERROR as ERROR, INIT
        or no run as not run, which does not count. No member counted means no assertions
        have run; all PASSING means passing; any FAILING means failing; any ERROR means
        erroring. Cloud's `result.type` PASSING only means no counted member's latest
        result is a FAILURE. Past a cap, failing and erroring members come first and
        `properties.dataQualityTotal` gives the full count, so the verdict still holds.
        On a dataset, `fields=["contract"]` returns its contract URN and state;
        `contract` in `notReturnedFields` means no contract is linked to the dataset.

        With trust labels enabled, `explain_trust=true` includes the matched category's
        instruction, matched rule and classification reason on each successful item.
        """
        # Direct callers skip the argument validator, which already splits "a, b".
        schema_columns = arg_validation.coerce_keyword_list(schema_columns)
        requested, return_single = _parse_urns(urns)

        usable, skipped = arg_validation.partition_urns(
            requested, tool="get_entities", param="urns"
        )
        # An empty list asks for nothing and gets nothing; raise only when values were
        # supplied and none was usable.
        if skipped and not usable:
            raise ToolError(
                arg_validation.no_usable_urns_error("get_entities", skipped)
            )
        skip_errors = {entry["urn"]: entry["error"] for entry in skipped}

        decisions = current_decisions(usable)
        # With several URNs, a column-named read splits one entity's schema budget
        # between them, so the call stays bounded however many tables it names.
        named_budget = (
            graphql_helpers.ENTITY_SCHEMA_TOKEN_BUDGET // max(1, len(usable))
            if any(name and name.strip() for name in schema_columns or [])
            else None
        )

        # In the caller's order, so results line up with the input.
        results: list[dict] = []
        for urn in requested:
            if urn in skip_errors:
                results.append({"error": skip_errors[urn], "urn": urn})
                continue
            urn = urn.strip()
            if is_configuration(urn, decisions):
                results.append(configuration_response(urn))
                continue
            try:
                item = self._read_entity(
                    urn,
                    fields=fields,
                    schema_columns=schema_columns,
                    named_budget=named_budget,
                )
            except Exception as e:
                logger.warning(f"Error fetching entity {urn}: {e}")
                if return_single:
                    raise e if isinstance(e, ToolError) else ToolError(str(e)) from e
                results.append({"error": str(e), "urn": urn})
                continue
            results.append(
                configuration_response(urn)
                if is_configuration(urn, decisions, item)
                else item
            )

        if decisions is not None:
            results = [
                apply_decision(item, item["urn"], decisions, explain=explain_trust)
                if "error" not in item and item.get("urn")
                else item
                for item in results
            ]
        if return_single:
            attach_header(results[0], decisions, usable, reader_only=True)
            return results[0]
        return results


# The substring default, for direct callers and servers that bind no matcher.
_SUBSTRING_TOOLS = SchemaFieldsTool(matcher=SubstringMatcher())
get_entities = _SUBSTRING_TOOLS.get_entities

# `documentation` (propagated, AI and document-extracted descriptions) is read only on
# DataHub Cloud: entity_details.gql gates it, so DataHub Core never returns it.
_CLOUD_ONLY_PARAGRAPHS = ("Descriptions written by propagation, by AI,",)


def get_entities_description(*, is_oss: bool) -> str:
    """get_entities' description, without the paragraphs about Cloud-only fields on Core."""
    doc = inspect.cleandoc(SchemaFieldsTool.get_entities.__doc__ or "")
    if not is_oss:
        return doc
    paragraphs = doc.split("\n\n")
    kept = [p for p in paragraphs if not p.startswith(_CLOUD_ONLY_PARAGRAPHS)]
    assert len(kept) == len(paragraphs) - len(_CLOUD_ONLY_PARAGRAPHS)
    return "\n\n".join(kept)
