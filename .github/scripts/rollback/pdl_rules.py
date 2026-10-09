"""Classify PDL changes (aspects, nested/shared records, typerefs, version gaps) for N-1."""

from __future__ import annotations

from typing import Optional

import bump_schema_versions as bsv
import report_aspect_changes as rac

from rollback import model, pdl_parser, repo


# (N-1 type, N type) pairs where N-1 holds every value N can write.
_LOSSLESS_NUMERIC = {("long", "int"), ("double", "int"), ("double", "float")}
_NUMERIC = {"int", "long", "float", "double"}
_INTEGRAL = {"int", "long"}


def _finding(
    origin: model.Origin,
    risk: str,
    impact: dict[str, str],
    change: str,
    note: Optional[str] = None,
    *,
    subject: Optional[str] = None,
    record: Optional[str] = None,
    detail: Optional[str] = None,
    dimension: str = model.DIM_PDL_SCHEMA,
    reindex_required: bool = False,
) -> model.RollbackFinding:
    """A finding summarised as "[In `record`: ]<change>[ — <note>]"."""
    head = (f"In `{record}`: " if record else "") + change
    return model.RollbackFinding(
        dimension=dimension,
        risk=risk,
        path=origin.path,
        aspect_name=origin.aspect_name,
        **impact,
        summary=f"{head} — {note}" if note else head,
        detail=detail,
        pr_number=origin.pr,
        author=origin.author,
        reindex_required=reindex_required,
        subject=subject,
        record=record,
    )


def type_change_finding(
    origin: model.Origin,
    name: str,
    tgt_t: str,
    cur_t: str,
    record: Optional[str] = None,
) -> model.RollbackFinding:
    change = f"Type change on `{name}`: `{tgt_t}`→`{cur_t}`"
    if tgt_t in _NUMERIC and cur_t in _NUMERIC:
        # Pegasus converts between number types with Number.intValue() etc.
        if (tgt_t, cur_t) in _LOSSLESS_NUMERIC:
            return _finding(
                origin,
                model.SAFE,
                model.impact(model.OK, model.OK, model.LOSS_NO),
                change,
                "N-1's type holds every value",
                subject=name,
                record=record,
            )
        if tgt_t in _INTEGRAL and cur_t in _INTEGRAL:
            loss, what = (
                model.LOSS_IF_OUT_OF_RANGE,
                f"values outside `{tgt_t}`'s range are silently truncated.",
            )
        elif tgt_t in _INTEGRAL:
            loss, what = (
                model.LOSS_FRACTIONS,
                "fractional parts are dropped and values outside "
                f"`{tgt_t}`'s range are truncated.",
            )
        elif cur_t in _INTEGRAL:
            loss, what = (
                model.LOSS_PRECISION,
                f"`{tgt_t}` can't hold every large integer exactly "
                f"(above 2^{24 if tgt_t == 'float' else 53}), so those are rounded.",
            )
        else:  # double -> float
            loss, what = (
                model.LOSS_PRECISION,
                "values lose precision, and values beyond `float`'s range "
                "become infinite.",
            )
        return _finding(
            origin,
            model.REQUIRES_ATTENTION,
            model.impact(model.MAY_TRUNCATE, model.OK, loss),
            change,
            subject=name,
            record=record,
            detail=f"N-1 converts N's `{cur_t}` values to `{tgt_t}` without error; {what}",
        )
    return _finding(
        origin,
        model.REQUIRES_ATTENTION,
        model.impact(model.API_FAILS, model.FAILS, model.LOSS_NO),
        change,
        subject=name,
        record=record,
        detail=(
            "N-1's typed getters throw on N's values and writes fail schema "
            "validation. Raw storage reads only log a warning."
        ),
    )


RESTORE_REBUILDS = (
    "The rollback's restore-indices rebuilds the N-1 edges from the stored records."
)
# @Searchable settings that only shape queries. Each version builds queries
# from its own registry, so a change here needs nothing after rollback.
_QUERY_ONLY_SEARCH_KEYS = {
    "queryByDefault",
    "boostScore",
    "addToFilters",
    "addHasValuesToFilters",
    "filterNameOverride",
    "hasValuesFilterNameOverride",
    "weightsPerFieldValue",
    "includeQueryEmptyAggregation",
}


def _relationship_findings(
    origin: model.Origin,
    name: str,
    cur_rel: Optional[str],
    tgt_rel: Optional[str],
    record: Optional[str],
) -> list[model.RollbackFinding]:
    """@Relationship changes on one field, compared per path. Graph edges are
    derived from the stored record, so these never fail reads or writes of the
    record itself (except target types N-1 rejects); what differs is which
    edges exist after rollback. Edges store only source, destination and
    relationship name, so flags such as isLineage don't change them."""
    if cur_rel == tgt_rel:
        return []
    cur, tgt = (
        pdl_parser.annotation_specs(cur_rel),
        pdl_parser.annotation_specs(tgt_rel),
    )
    if cur is None or tgt is None:
        return [
            _finding(
                origin,
                model.REQUIRES_ATTENTION,
                model.impact(model.STALE, model.OK, model.LOSS_GRAPH_ONLY),
                f"Graph relationship changed on `{name}`",
                subject=name,
                record=record,
                detail=(
                    "The relationship annotation couldn't be compared. Check the PR "
                    "for which graph edges N builds differently from N-1."
                ),
            )
        ]

    def types(spec: dict) -> set[str]:
        return set(spec.get("entityTypes") or [])

    findings: list[model.RollbackFinding] = []
    gained: set[str] = set()
    changed_keys: set[str] = set()
    for path in sorted(set(cur) | set(tgt)):
        c, t = cur.get(path), tgt.get(path)
        if t and not c:
            findings.append(
                _relationship_removed(origin, name, t.get("name", "?"), record)
            )
        elif c and not t:
            findings.append(
                _relationship_added(origin, name, c.get("name", "?"), record)
            )
        else:
            # N-1 validates writes against its own target types, renamed or not.
            gained |= types(c) - types(t)
            if c.get("name") != t.get("name"):
                findings.append(
                    _relationship_renamed(
                        origin, name, t.get("name", "?"), c.get("name", "?"), record
                    )
                )
            else:
                changed_keys |= {k for k in set(c) | set(t) if c.get(k) != t.get(k)}
    if gained:
        names = ", ".join(f"`{x}`" for x in sorted(gained))
        findings.append(
            _finding(
                origin,
                model.EXPECTED_LOSS,
                model.impact(model.OK, model.FAILS, model.LOSS_NO),
                f"Graph relationship on `{name}` gained target types {names}",
                subject=name,
                record=record,
                detail=(
                    f"N-1's write validation rejects {names} URNs in this field, so "
                    "saving a record that holds one fails on N-1 until the URN is "
                    "removed from the aspect. N's graph edges to them stay until "
                    "then; restore-indices doesn't remove them. If N-1 has no such "
                    "entity type, the UI shows these links as empty."
                ),
            )
        )
    elif changed_keys and not findings:
        keys = ", ".join(f"`{k}`" for k in sorted(changed_keys))
        findings.append(
            _finding(
                origin,
                model.SAFE,
                model.impact(model.OK, model.OK, model.LOSS_NO),
                f"Graph relationship on `{name}`: only {keys} changed",
                "same edges in N and N-1",
                subject=name,
                record=record,
                detail=(
                    "Edges store only their source, destination and relationship "
                    "name, which didn't change. N-1 applies its own settings, such "
                    "as which relationships count as lineage, when it reads them, "
                    "and every target type N writes is one N-1 accepts."
                ),
            )
        )
    return findings


def _relationship_removed(
    origin: model.Origin, name: str, rel: str, record: Optional[str]
) -> model.RollbackFinding:
    f = _finding(
        origin,
        model.SAFE,
        model.impact(model.OK, model.OK, model.LOSS_NO),
        f"Graph relationship `{rel}` removed from `{name}`",
        subject=name,
        record=record,
        detail=(
            f"N doesn't build `{rel}` edges from this field. {RESTORE_REBUILDS} "
            "Records N wrote without values in this field get no edges."
        ),
    )
    f.relationship, f.rel_change = rel, "removed"
    return f


def _relationship_renamed(
    origin: model.Origin, name: str, old: str, new: str, record: Optional[str]
) -> model.RollbackFinding:
    f = _finding(
        origin,
        model.SAFE,
        model.impact(model.OK, model.OK, model.LOSS_NO),
        f"Graph relationship on `{name}` renamed `{old}` → `{new}`",
        subject=name,
        record=record,
        detail=(
            f"N builds these edges as `{new}` instead of `{old}`. {RESTORE_REBUILDS} "
            f"N's `{new}` edges stay in the graph, but N-1 doesn't build or query "
            f"`{new}` for this entity."
        ),
    )
    f.relationship, f.rel_change, f.rel_new = old, "renamed", new
    return f


def _relationship_added(
    origin: model.Origin, name: str, rel: str, record: Optional[str]
) -> model.RollbackFinding:
    f = _finding(
        origin,
        model.REQUIRES_ATTENTION,
        model.impact(model.OK, model.OK, model.LOSS_NO),
        f"Graph relationship `{rel}` added on `{name}`",
        subject=name,
        record=record,
        detail=(
            f"N built `{rel}` edges from this field. N-1 has no rule for this field, "
            "so it never updates or removes them."
        ),
    )
    f.relationship, f.rel_change = rel, "added"
    return f


def _default_search_type(type_text: str, path: str) -> str:
    """The fieldType DataHub uses when @Searchable doesn't set one
    (SearchableAnnotation.getDefaultFieldType), for the value at `path`."""
    t = type_text.strip()
    if path.endswith("/*") and t.startswith("array["):
        t = t[len("array[") : -1].strip()
    if t == "int":
        return "COUNT"
    if t in ("float", "double"):
        return "DOUBLE"
    return "KEYWORD" if t.startswith("map[") else "TEXT"


def _effective_search(
    specs: dict[str, dict], name: str, type_text: str
) -> dict[str, dict]:
    """@Searchable settings per path with DataHub's defaults filled in, so a
    default spelled out explicitly isn't taken for a change."""
    return {
        path: {
            "fieldName": name,
            "fieldType": _default_search_type(type_text, path),
            **spec,
        }
        for path, spec in specs.items()
    }


def _search_findings(
    origin: model.Origin,
    name: str,
    cur_map: dict[str, Optional[str]],
    tgt_map: dict[str, Optional[str]],
    record: Optional[str],
    cur_type: str = "string",
    tgt_type: str = "string",
) -> list[model.RollbackFinding]:
    """@Searchable/@SearchableRef changes on one field, by kind. DataHub changes
    an index mapping only for fields that differ or are new; a field N stopped
    indexing keeps its mapping, so no reindex happens in either direction."""
    findings: list[model.RollbackFinding] = []
    for key in ("Searchable", "SearchableRef"):
        c, t = cur_map.get(key), tgt_map.get(key)
        if c == t:
            continue
        if not t:
            findings.append(
                _finding(
                    origin,
                    model.SAFE,
                    model.impact(model.OK, model.OK, model.LOSS_NO),
                    f"Search indexing added on `{name}`",
                    "N's field stays in the index",
                    subject=name,
                    record=record,
                    detail=(
                        "N indexed this field. It stays in the search index after "
                        "rollback, so N-1 searches still match N's values, but N-1 "
                        "never updates it. Nothing else changes."
                    ),
                )
            )
            continue
        cs, ts = pdl_parser.annotation_specs(c), pdl_parser.annotation_specs(t)
        if key == "Searchable" and cs is not None and ts is not None:
            cs, ts = (
                _effective_search(cs, name, cur_type),
                _effective_search(ts, name, tgt_type),
            )
        if cs is not None and ts is not None and c and cs == ts:
            continue  # only defaults spelled out
        changed = (
            {
                k
                for path in set(cs) | set(ts)
                for k in set(cs.get(path, {})) | set(ts.get(path, {}))
                if cs.get(path, {}).get(k) != ts.get(path, {}).get(k)
            }
            if cs is not None and ts is not None and c
            else None
        )
        if not c or (changed is not None and "fieldName" in changed):
            if c:
                old = sorted({s.get("fieldName") for s in ts.values()})
                new = sorted({s.get("fieldName") for s in cs.values()})
                summary = f"Search field renamed on `{name}`: `{', '.join(old)}` → `{', '.join(new)}`"
                what = f"N indexed this field as `{', '.join(new)}` instead of `{', '.join(old)}`"
            else:
                summary = f"Search indexing removed from `{name}`"
                what = "N indexed records without this field"
            findings.append(
                _finding(
                    origin,
                    model.SAFE,
                    model.impact(model.OK, model.OK, model.LOSS_NO),
                    summary,
                    subject=name,
                    record=record,
                    detail=(
                        f"{what}. N-1's mapping for its field is still in the index, "
                        "since a field N stops indexing keeps its mapping, and the "
                        "rollback's restore-indices rebuilds it in every document from "
                        "the stored records. Records N wrote without a value have "
                        "nothing to index."
                    ),
                )
            )
        elif changed is not None and changed <= _QUERY_ONLY_SEARCH_KEYS:
            keys = ", ".join(f"`{k}`" for k in sorted(changed))
            findings.append(
                _finding(
                    origin,
                    model.SAFE,
                    model.impact(model.OK, model.OK, model.LOSS_NO),
                    f"Search settings changed on `{name}`: {keys}",
                    "query-time only",
                    subject=name,
                    record=record,
                    detail=(
                        "These settings only shape queries, and each version builds "
                        "queries from its own settings, so N-1 searches as before."
                    ),
                )
            )
        else:
            findings.append(
                _finding(
                    origin,
                    model.SAFE,
                    model.impact(model.OK, model.OK, model.LOSS_NO),
                    f"Search mapping changed on `{name}`",
                    "the rollback reindexes it",
                    subject=name,
                    record=record,
                    detail=(
                        "N built the search index with a different mapping for this "
                        "field. With a new DATAHUB_REVISION, the blocking step of N-1's "
                        "system-update reindexes it to N-1's mapping, which adds time "
                        "to the rollback for this index, and restore-indices then "
                        "fills the documents. With the same revision it skips the "
                        "index, because it built it before the upgrade, and search "
                        "keeps N's mapping."
                    ),
                    reindex_required=True,
                )
            )
    return findings


def diff_fields(
    cur_fields: dict,
    tgt_fields: dict,
    cur_enums: dict[str, list[str]],
    tgt_enums: dict[str, list[str]],
    target_content: Optional[str],
    path: str,
    aspect_name: Optional[str],
    pr: Optional[str],
    author: Optional[str],
    record: Optional[str] = None,
) -> list[model.RollbackFinding]:
    """Field and enum changes of one record, classified for N-1. `record` names
    a nested record (summaries then start "In `Foo`: "). Field maps come from
    `pdl_parser.effective_fields`, which sets `has_default` on every field."""
    origin = model.Origin(path, aspect_name, pr, author)
    findings: list[model.RollbackFinding] = []

    def has_default(name: str) -> bool:
        return tgt_fields[name]["has_default"]

    for name in sorted(set(cur_fields) - set(tgt_fields)):
        findings.append(
            _finding(
                origin,
                model.EXPECTED_LOSS,
                model.impact(model.OK, model.DROPS_NEW_FIELD, model.LOSS_NO),
                f"Added field `{name}`{pdl_parser.via_note(cur_fields[name])}",
                "N-1 ignores unknown fields",
                subject=name,
                record=record,
                detail=(
                    "N-1 doesn't show this field and deletes N's value the next "
                    "time it saves the record."
                ),
            )
        )

    for name in sorted(set(tgt_fields) - set(cur_fields)):
        tgt = tgt_fields[name]
        removed = f"Removed field `{name}`{pdl_parser.via_note(tgt)}"
        # N-1 fills an absent field from its default, so only a required
        # field without one breaks N-1 on records N wrote without it.
        required_no_default = not tgt["optional"] and not has_default(name)
        if required_no_default:
            findings.append(
                _finding(
                    origin,
                    model.BLOCKS_ROLLBACK,
                    model.impact(model.API_FAILS, model.FAILS, model.LOSS_YES),
                    removed,
                    "required in N-1, no default",
                    subject=name,
                    record=record,
                    detail=(
                        "N wrote every record without this field, and N-1 can't read "
                        "or save any of them. Before relying on N-1, delete this aspect "
                        "for those entities (the API returns 400, but the row is "
                        "removed), then re-emit the data in N-1's schema."
                    ),
                )
            )
        else:
            findings.append(
                _finding(
                    origin,
                    model.REQUIRES_ATTENTION,
                    model.impact(model.OK, model.OK, model.LOSS_YES),
                    removed,
                    "N-1 expects it",
                    subject=name,
                    record=record,
                    detail=(
                        "Records N wrote lose this field's value; N-1 reads them "
                        "as empty or with its default."
                    ),
                )
            )

    for name in sorted(set(cur_fields) & set(tgt_fields)):
        cur, tgt = cur_fields[name], tgt_fields[name]

        cur_t, tgt_t = (
            pdl_parser.comparable_type(cur["type"]),
            pdl_parser.comparable_type(tgt["type"]),
        )
        if cur_t != tgt_t:
            findings.append(type_change_finding(origin, name, tgt_t, cur_t, record))

        findings.extend(
            _search_findings(
                origin,
                name,
                pdl_parser.mapping_annotations(cur["annotations"]),
                pdl_parser.mapping_annotations(tgt["annotations"]),
                record,
                cur["type"],
                tgt["type"],
            )
        )

        findings.extend(
            _relationship_findings(
                origin,
                name,
                pdl_parser.normalized_annotation(
                    cur["annotations"].get("Relationship")
                ),
                pdl_parser.normalized_annotation(
                    tgt["annotations"].get("Relationship")
                ),
                record,
            )
        )

        # N always writes a field it requires, so N-1 can read it whether or
        # not N-1 requires it.
        if tgt["optional"] and not cur["optional"]:
            findings.append(
                _finding(
                    origin,
                    model.SAFE,
                    model.impact(model.OK, model.OK, model.LOSS_NO),
                    f"Optional→required flip on `{name}`",
                    "safe for rollback",
                    subject=name,
                    record=record,
                )
            )

        # N may write records without a field it made optional. N-1 fills
        # it from its default if it has one; otherwise reading them fails.
        if not tgt["optional"] and cur["optional"] and has_default(name):
            findings.append(
                _finding(
                    origin,
                    model.SAFE,
                    model.impact(model.OK, model.OK, model.LOSS_NO),
                    f"Required→optional flip on `{name}`",
                    "N-1 uses its default",
                    subject=name,
                    record=record,
                )
            )
        elif not tgt["optional"] and cur["optional"]:
            findings.append(
                _finding(
                    origin,
                    model.REQUIRES_ATTENTION,
                    model.impact(model.API_FAILS, model.FAILS, model.LOSS_NO),
                    f"Required→optional flip on `{name}`",
                    "N-1 requires it",
                    subject=name,
                    record=record,
                    detail=(
                        "N-1 can't read or save records N wrote without this field: "
                        "the UI fails for those entities and writes fail. For each one, "
                        "delete this aspect on N-1 (the API returns 400, but the row is "
                        "removed), then re-emit the data from its source in N-1's schema."
                    ),
                )
            )

    for ename in sorted(set(cur_enums) & set(tgt_enums)):
        # Unlike an unknown field, an unknown enum symbol can't be trimmed:
        # Pegasus validation rejects it and N-1's getter returns $UNKNOWN,
        # which GraphQL mappers using `valueOf(x.toString())` throw on.
        for v in sorted(set(cur_enums[ename]) - set(tgt_enums[ename])):
            findings.append(
                _finding(
                    origin,
                    model.EXPECTED_LOSS,
                    model.impact(model.UI_API_FAILS, model.FAILS, model.LOSS_NO),
                    f"Enum `{ename}`: added value `{v}`",
                    "N-1 doesn't know it",
                    subject=ename,
                    record=record,
                    detail=(
                        "On N-1, entities whose records hold this value fail to load "
                        "in the UI and GraphQL, writes to those records fail, and so "
                        "do their search and graph updates. Rewrite them without the "
                        "value on N before rolling back if they must keep working."
                    ),
                )
            )
        # A value removed in N is never in N's data, so it can't affect N-1;
        # it only matters when rolling forward, which is out of scope.
    return findings


def classify_pdl_for_rollback(
    path: str,
    current: str,
    target: str,
    read: Optional[repo.Reader] = None,
) -> list[model.RollbackFinding]:
    """Classify field/enum/rename changes for rollback risk.

    Direction is reversed compared to forward-compatibility: a field *added*
    in N means N-1 doesn't know about it.
    """
    findings: list[model.RollbackFinding] = []
    read = read or rac.file_at
    current_content = read(current, path)
    target_content = read(target, path)

    cur_meta = pdl_parser.parse(current_content).aspect if current_content else None
    tgt_meta = pdl_parser.parse(target_content).aspect if target_content else None
    aspect_name = (cur_meta or {}).get("name") or (tgt_meta or {}).get("name")

    # Skip non-aspect PDL files (enums, shared records) — they're not stored
    # in metadata_aspect_v2 and don't directly affect rollback. Changes
    # propagate via the aspects that include them (captured by schema version
    # gaps on those aspects).
    if not aspect_name:
        return findings

    pr = repo.first_pr(current, path, target)
    author = repo.file_author(current, path, target)
    origin = model.Origin(path, aspect_name, pr, author)

    if current_content and not target_content:
        new_entities = _new_entity_types(aspect_name, current, target, read)
        if new_entities:
            names = ", ".join(f"`{e}`" for e in new_entities)
            findings.append(
                _finding(
                    origin,
                    model.EXPECTED_LOSS,
                    model.impact(model.API_FAILS, model.FAILS, model.LOSS_NO),
                    "New file in N",
                    f"part of {names}, an entity type new in N",
                    subject=new_entities[0],
                    detail=(
                        f"{names} is new in N, so N-1 can't read, write or index these "
                        "entities at all: its APIs return not found or unknown "
                        "entity, and GraphQL returns null. The rollback's restore-indices skips these rows one by one (counted as ignored) and restores everything else; N's rows stay in the database untouched. Restoring specific URNs that include these entities fails for that group of URNs."
                    ),
                )
            )
            return findings
        findings.append(
            _finding(
                origin,
                model.EXPECTED_LOSS,
                model.impact(model.API_FAILS, model.FAILS, model.LOSS_NO),
                "New file in N",
                "absent in N-1 (N-1 rejects writes to it)",
                detail=(
                    "N-1 can't read or write this aspect; reading its entities "
                    "returns their other aspects as usual. The rollback's restore-indices skips these rows one by one (counted as ignored) and restores everything else; N's rows stay in the database untouched. Restoring specific URNs that include these entities fails for that group of URNs."
                ),
            )
        )
        return findings

    if not current_content and target_content:
        findings.append(
            _finding(
                origin,
                model.REQUIRES_ATTENTION,
                model.impact(model.STALE, model.OK, model.LOSS_NO),
                "File deleted in N",
                "N-1 expects it",
            )
        )
        return findings

    if not current_content and not target_content:
        return findings

    findings.extend(
        diff_fields(
            pdl_parser.effective_fields(current_content, current, read=read),
            pdl_parser.effective_fields(target_content, target, read=read),
            pdl_parser.enum_symbols(current_content),
            pdl_parser.enum_symbols(target_content),
            target_content,
            path,
            aspect_name,
            pr,
            author,
        )
    )

    cur_name = pdl_parser.parse(current_content).record_name
    tgt_name = pdl_parser.parse(target_content).record_name
    if cur_name and tgt_name and cur_name != tgt_name:
        findings.append(
            _finding(
                origin,
                model.SAFE,
                model.impact(model.OK, model.OK, model.LOSS_NO),
                f"Record renamed `{tgt_name}`→`{cur_name}`",
                "stored by aspect name, N-1 unaffected",
                subject=cur_name,
            )
        )

    return findings


def _new_entity_types(
    aspect_name: str, current: str, target: str, read: repo.Reader
) -> list[str]:
    """Entity types holding `aspect_name` in N, when none of them exist in N-1.
    Empty if the aspect belongs to an existing entity or a registry is missing."""
    reg_n = pdl_parser.entity_registry(read(current, pdl_parser.ENTITY_REGISTRY) or "")
    reg_t = pdl_parser.entity_registry(read(target, pdl_parser.ENTITY_REGISTRY) or "")
    owners = sorted(e for e, aspects in reg_n.items() if aspect_name in aspects)
    if not reg_t or not owners or any(e in reg_t for e in owners):
        return []
    return owners


def aspects_using(
    changed_fqns: set[str], contents: dict[str, str], events: bool = False
) -> dict[str, set[str]]:
    """For each changed record, the aspect names that reach it through field
    types or `includes`, directly or through other records. With `events`,
    the Kafka event schemas that reach it instead (the record itself counts
    when it is one)."""
    reverse: dict[str, set[str]] = {}
    aspect_of: dict[str, str] = {}
    for path, content in contents.items():
        own = pdl_parser.fqn_of_path(path)
        if events:
            if pdl_parser.is_event_root(own) and pdl_parser.parse(content).main_record:
                aspect_of[own] = own.rsplit(".", 1)[-1]
        else:
            meta = pdl_parser.parse(content).aspect
            if meta and meta.get("name"):
                aspect_of[own] = meta["name"]
        for dep in bsv.resolve_dependencies(content):
            if dep != own:
                reverse.setdefault(dep, set()).add(own)
    result: dict[str, set[str]] = {}
    for fqn in changed_fqns:
        seen, queue = {fqn}, [fqn]
        aspects = {aspect_of[fqn]} if events and fqn in aspect_of else set()
        while queue:
            node = queue.pop()
            for parent in reverse.get(node, ()):
                if parent not in seen:
                    seen.add(parent)
                    queue.append(parent)
                    if parent in aspect_of:
                        aspects.add(aspect_of[parent])
        result[fqn] = aspects
    return result


def _all_pdls_at(ref: str) -> dict[str, str]:
    all_paths = [
        p
        for p in repo.git(
            "ls-tree", "-r", "--name-only", ref, "--", rac.PDL_PREFIX
        ).split()
        if p.endswith(".pdl")
    ]
    return repo.read_files_at(ref, all_paths)


def _aspects_using_at(ref: str, fqns: set[str]) -> dict[str, set[str]]:
    """`aspects_using` over every PDL file at `ref`."""
    return aspects_using(fqns, _all_pdls_at(ref))


def typeref_findings(
    cur: str,
    tgt: str,
    path: str,
    pr: Optional[str],
    author: Optional[str],
) -> list[model.RollbackFinding]:
    """Changes to typerefs and fixed types defined in a file."""
    origin = model.Origin(path, None, pr, author)
    cur_t, cur_f, _ = pdl_parser.parse(cur).split
    tgt_t, tgt_f, _ = pdl_parser.parse(tgt).split
    findings: list[model.RollbackFinding] = []
    for name in sorted(set(cur_t) & set(tgt_t)):
        old, new = tgt_t[name], cur_t[name]
        old_m, new_m = pdl_parser.union_members(old), pdl_parser.union_members(new)
        if old_m is not None and new_m is not None:
            # A member removed in N never appears in N's data (roll-forward only).
            for member in sorted(new_m - old_m):
                findings.append(
                    _finding(
                        origin,
                        model.EXPECTED_LOSS,
                        model.impact(model.API_FAILS, model.FAILS, model.LOSS_NO),
                        f"Union `{name}`: added member `{member}`",
                        "N-1 doesn't know it",
                        subject=name,
                        detail=(
                            "On N-1, records holding this member fail to load in the "
                            "UI and GraphQL, OpenAPI returns the union empty, and "
                            "writes fail schema validation."
                        ),
                    )
                )
        elif pdl_parser.comparable_type(old) != pdl_parser.comparable_type(new):
            findings.append(
                type_change_finding(
                    origin,
                    name,
                    pdl_parser.comparable_type(old),
                    pdl_parser.comparable_type(new),
                )
            )
    for name in sorted(set(cur_f) & set(tgt_f)):
        if cur_f[name] != tgt_f[name]:
            findings.append(
                _finding(
                    origin,
                    model.REQUIRES_ATTENTION,
                    model.impact(model.API_FAILS, model.FAILS, model.LOSS_NO),
                    f"Fixed `{name}`: size {tgt_f[name]}→{cur_f[name]}",
                    subject=name,
                    detail="N-1 rejects values of a different size.",
                )
            )
    return findings


def _include_closures_of_changed_aspects(
    current: str, pdl_paths: list[str], read: repo.Reader
) -> dict[str, set[str]]:
    """{aspect name: records it includes} for aspects whose own file changed."""
    out: dict[str, set[str]] = {}
    for path in pdl_paths:
        content = read(current, path)
        meta = pdl_parser.parse(content).aspect if content else None
        if meta and meta.get("name"):
            out[meta["name"]] = pdl_parser.include_closure(
                content, current, set(), read
            )
    return out


def analyze_nested_changes(
    current: str,
    target: str,
    pdl_paths: list[str],
    read: Optional[repo.Reader] = None,
) -> list[model.RollbackFinding]:
    """Field and enum changes in non-aspect records, reported once per change
    and attributed to every aspect that uses the record."""
    read = read or rac.file_at
    changed: dict[str, tuple[str, str]] = {}
    for path in pdl_paths:
        cur, tgt = read(current, path), read(target, path)
        if cur and tgt and not pdl_parser.parse(cur).aspect:
            changed[pdl_parser.fqn_of_path(path)] = (cur, tgt)
    if not changed:
        return []
    contents = _all_pdls_at(current)
    users = aspects_using(set(changed), contents)
    event_users = aspects_using(set(changed), contents, events=True)
    covered = _include_closures_of_changed_aspects(current, pdl_paths, read)

    findings: list[model.RollbackFinding] = []
    for fqn, (cur, tgt) in sorted(changed.items()):
        aspects = sorted(users.get(fqn, ()))
        if bsv.normalize_pdl_for_compare(cur) == bsv.normalize_pdl_for_compare(tgt):
            continue  # comments or formatting only
        # An aspect whose own file changed already compares this record's
        # *fields* through its `includes` (see pdl_parser.effective_fields). Enum values
        # and unparseable files aren't covered that way, so they still count.
        field_aspects = [a for a in aspects if fqn not in covered.get(a, ())]
        events = sorted(event_users.get(fqn, ()))
        if not aspects and not events:
            continue
        path = pdl_parser.path_of_fqn(fqn)
        pr = repo.first_pr(current, path, target)
        author = repo.file_author(current, path, target)
        # Typerefs and fixed types are compared here; bsv's record parser gives
        # up on files containing them, so it reads the rest of the file.
        record_findings: list[model.RollbackFinding] = typeref_findings(
            cur, tgt, path, pr, author
        )
        field_findings: list[model.RollbackFinding] = []
        cur_defs = pdl_parser.parse(cur).defs
        tgt_defs = pdl_parser.parse(tgt).defs
        if cur_defs is None or tgt_defs is None:
            # Never let a change the parser can't read pass silently.
            short = fqn.rsplit(".", 1)[-1]
            record_findings.append(
                _finding(
                    model.Origin(path, None, pr, author),
                    model.REQUIRES_ATTENTION,
                    model.impact(
                        model.NOT_ANALYSED, model.NOT_ANALYSED, model.NOT_ANALYSED
                    ),
                    f"`{short}` changed but couldn't be analysed",
                    subject=short,
                    detail=(
                        "The file uses a construct this tool doesn't parse. Check "
                        "the PR for changes N-1 can't read."
                    ),
                )
            )
            cur_defs, tgt_defs = {}, {}
        for name in sorted(set(cur_defs) & set(tgt_defs)):
            if cur_defs[name]["kind"] == tgt_defs[name]["kind"] == "record":
                field_findings.extend(
                    diff_fields(
                        pdl_parser.effective_fields(
                            cur, current, name, cur_defs[name], read=read
                        ),
                        pdl_parser.effective_fields(
                            tgt, target, name, tgt_defs[name], read=read
                        ),
                        {},
                        {},
                        tgt,
                        path,
                        None,
                        pr,
                        author,
                        name,
                    )
                )
        record_findings.extend(
            diff_fields(
                {},
                {},
                pdl_parser.enum_symbols(cur),
                pdl_parser.enum_symbols(tgt),
                tgt,
                path,
                None,
                pr,
                author,
            )
        )
        if not aspects:
            findings.extend(
                _as_event_findings(record_findings + field_findings, events)
            )
            continue
        for group, users_of in (
            (record_findings, aspects),
            (field_findings, field_aspects),
        ):
            if not users_of:
                continue
            shown = ", ".join(users_of[:3]) + (
                f" +{len(users_of) - 3} more" if len(users_of) > 3 else ""
            )
            for f in group:
                f.aspect_name = shown
                f.affected_aspects = users_of
                used_by = f"Used by: {', '.join(users_of)}."
                f.detail = f"{f.detail} {used_by}" if f.detail else used_by
            findings.extend(group)
    return findings


def _as_event_findings(
    group: list[model.RollbackFinding], events: list[str]
) -> list[model.RollbackFinding]:
    """Changes to a type only Kafka events use. Stored data isn't affected and
    this tool can't see the events' consumers, so they're reported for review
    rather than classified."""
    names = ", ".join(f"`{e}`" for e in events)
    for f in group:
        f.dimension = model.DIM_EVENT_SCHEMA
        f.risk = model.REQUIRES_ATTENTION
        f.aspect_name = ", ".join(f"{e} (event)" for e in events)
        f.read_impact = f.write_impact = f.data_loss = model.NOT_ANALYSED
        f.detail = (
            f"Used by the Kafka event {names}, not by a stored aspect, so this tool "
            "doesn't analyse it. N may have sent events with this change; check "
            f"that N-1's consumers of {names} handle it (for example, enum values "
            "they don't know)."
        )
    return group


def _relationships_by_aspect(contents: dict[str, str]) -> dict[str, set[str]]:
    """{aspect name: relationship names it builds}, including those declared in
    the records it reaches through field types and includes."""
    by_fqn = {pdl_parser.fqn_of_path(p): c for p, c in contents.items()}
    result: dict[str, set[str]] = {}
    for fqn, content in by_fqn.items():
        meta = pdl_parser.parse(content).aspect
        if not (meta and meta.get("name")):
            continue
        seen, queue, names = {fqn}, [fqn], set()
        while queue:
            text = by_fqn.get(queue.pop())
            if not text:
                continue
            names |= pdl_parser.relationship_names(text)
            for dep in bsv.resolve_dependencies(text):
                if dep not in seen:
                    seen.add(dep)
                    queue.append(dep)
        result[meta["name"]] = names
    return result


def refine_relationship_findings(
    findings: list[model.RollbackFinding], target: str
) -> None:
    """Classify relationships N removed, renamed or added by what the
    rollback's restore-indices leaves behind, which depends on N-1's other
    aspects of the same entity. Restore-indices re-indexes with
    FORCE_INDEXING: per aspect it deletes all of the entity's outgoing edges of
    the relationships that aspect builds, then adds that aspect's, aspect by
    aspect in name order. So when two aspects build the same relationship, the
    last one restored replaces the other's edges."""
    todo = [f for f in findings if f.rel_change]
    if not todo:
        return
    registry = pdl_parser.entity_registry(
        rac.file_at(target, pdl_parser.ENTITY_REGISTRY) or ""
    )
    rels = _relationships_by_aspect(_all_pdls_at(target)) if registry else {}
    if not rels:
        return
    for f in todo:
        aspects = set(f.affected_aspects or ([f.aspect_name] if f.aspect_name else []))
        siblings = {b for asps in registry.values() if asps & aspects for b in asps}

        def builders(rel: Optional[str], include_own: bool) -> list[str]:
            pool = siblings if include_own else siblings - aspects
            return sorted(b for b in pool if rel in rels.get(b, ()))

        if f.rel_change in ("removed", "renamed") and builders(f.relationship, False):
            others = ", ".join(f"`{o}`" for o in builders(f.relationship, False))
            rel = f"`{f.relationship}`"
            f.risk = model.REQUIRES_ATTENTION
            f.read_impact, f.data_loss = model.STALE, model.LOSS_GRAPH_ONLY
            f.detail = (
                f"N doesn't build {rel} edges from this field. N-1 also builds {rel} "
                f"edges for this entity from {others}, and the rollback's "
                f"restore-indices rebuilds an entity's {rel} edges from each aspect in "
                "turn, the last one replacing the others. So edges that only this "
                f"field gives are missing after the rollback wherever {others} also "
                f"has {rel} edges, not only for records N changed. The stored records "
                "are intact; N-1 restores the edges when it next saves each record."
            )
        new = f.rel_new if f.rel_change == "renamed" else f.relationship
        if f.rel_change in ("added", "renamed") and builders(new, True):
            sources = ", ".join(f"`{a}`" for a in builders(new, True))
            f.risk = model.REQUIRES_ATTENTION
            f.read_impact = model.STALE
            f.detail = (
                f"{f.detail} N-1 also builds `{new}` edges for this entity (from "
                f"{sources}), so N's show up in its relationship and lineage views as "
                "extra edges. The rollback's restore-indices replaces them on entities "
                f"where N-1 builds `{new}` edges itself; elsewhere they stay."
            )
        elif f.rel_change == "added":
            f.risk = model.SAFE
            f.detail = (
                f"{f.detail} N-1 has no `{new}` relationship for this entity, so its "
                "views don't show them."
            )


def attribute_embedded_aspect_changes(
    findings: list[model.RollbackFinding],
    current: str,
    target: str,
    pdl_paths: list[str],
) -> None:
    """An aspect record can also be embedded as a field of another aspect (e.g.
    `IncidentInfo` inside `IncidentActivityEvent`). `analyze_nested_changes`
    skips aspect records, so attribute the aspect's own field and annotation
    changes to every aspect that embeds it; otherwise the embedding aspect's
    version bump looks unexplained."""
    changed: dict[str, str] = {}
    for path in pdl_paths:
        cur, tgt = rac.file_at(current, path), rac.file_at(target, path)
        meta = pdl_parser.parse(cur).aspect if cur and tgt else None
        if meta and meta.get("name"):
            changed[path] = meta["name"]
    by_path: dict[str, list[model.RollbackFinding]] = {}
    for f in findings:
        if (
            f.dimension == model.DIM_PDL_SCHEMA
            and f.path in changed
            and f.aspect_name == changed[f.path]
        ):
            by_path.setdefault(f.path, []).append(f)
    if not by_path:
        return
    users = _aspects_using_at(current, {pdl_parser.fqn_of_path(p) for p in by_path})
    for path, own in by_path.items():
        embedders = sorted(
            users.get(pdl_parser.fqn_of_path(path), set()) - {changed[path]}
        )
        if not embedders:
            continue
        also = f"Also embedded in: {', '.join(embedders)}."
        for f in own:
            f.affected_aspects = sorted(set(f.affected_aspects) | set(embedders))
            f.detail = f"{f.detail} {also}" if f.detail else also


def analyze_schema_version_gaps(
    current: str, target: str, pdl_paths: list[str]
) -> list[model.RollbackFinding]:
    findings: list[model.RollbackFinding] = []
    for path in pdl_paths:
        cur_content = rac.file_at(current, path)
        tgt_content = rac.file_at(target, path)
        cur_meta = pdl_parser.parse(cur_content).aspect if cur_content else None
        tgt_meta = pdl_parser.parse(tgt_content).aspect if tgt_content else None
        if not cur_meta or not tgt_meta:
            continue
        cur_v = cur_meta.get("schemaVersion") or 1
        tgt_v = tgt_meta.get("schemaVersion") or 1
        if cur_v > tgt_v:
            gap = cur_v - tgt_v
            origin = model.Origin(
                path,
                cur_meta.get("name"),
                repo.first_pr(current, path, target),
                repo.file_author(current, path, target),
            )
            findings.append(
                _finding(
                    origin,
                    model.SAFE,
                    model.impact(model.OK, model.OK, model.LOSS_NO),
                    f"Schema version gap: v{tgt_v}→v{cur_v} "
                    f"({gap} hop{'s' if gap > 1 else ''})",
                    dimension=model.DIM_SCHEMA_VERSION,
                    detail=(
                        f"N-1 reads records at version {cur_v} and writes its own "
                        f"version {tgt_v}. Field-level changes for this aspect are "
                        f"listed separately."
                    ),
                )
            )
    return findings
