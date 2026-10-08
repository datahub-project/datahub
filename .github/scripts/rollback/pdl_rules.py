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


def _relationship_findings(
    origin: model.Origin,
    name: str,
    cur_rel: Optional[str],
    tgt_rel: Optional[str],
    record: Optional[str],
) -> list[model.RollbackFinding]:
    """@Relationship changes on one field. N-1 rebuilds edges from stored
    records by its own rule, but never deletes an edge its rule doesn't
    produce, so N's extra edges outlive restore-indices."""
    if cur_rel == tgt_rel:
        return []
    findings: list[model.RollbackFinding] = []
    gained = (
        pdl_parser.relationship_entity_types(cur_rel)
        - pdl_parser.relationship_entity_types(tgt_rel)
        if cur_rel and tgt_rel
        else set()
    )
    if gained:
        types = ", ".join(f"`{t}`" for t in sorted(gained))
        findings.append(
            _finding(
                origin,
                model.EXPECTED_LOSS,
                model.impact(model.OK, model.FAILS, model.LOSS_NO),
                f"Graph relationship on `{name}` gained target types {types}",
                subject=name,
                record=record,
                detail=(
                    f"N-1's write validation rejects {types} URNs in this field, so "
                    "saving a record that holds one fails on N-1 until the URN is "
                    "removed from the aspect. N's graph edges to them stay until "
                    "then; restore-indices doesn't remove them. If N-1 has no such "
                    "entity type, the UI shows these links as empty."
                ),
            )
        )
        if pdl_parser.without_entity_types(cur_rel) == pdl_parser.without_entity_types(
            tgt_rel
        ):
            return findings
    if not tgt_rel:
        why = (
            "N built graph edges for this field that N-1 doesn't expect. They stay "
            "after rollback: restore-indices doesn't remove them, so relationship "
            "and lineage views can show extra edges."
        )
    elif not cur_rel:
        why = (
            "N didn't build the graph edges N-1 expects for this field. Running "
            "restore-indices on N-1 rebuilds them from the stored records."
        )
    else:
        why = (
            "N built graph edges for this field by its own @Relationship rule. "
            "Restore-indices on N-1 adds the edges N-1's rule expects but doesn't "
            "remove N's, so relationship and lineage views can show extra edges."
        )
    findings.append(
        _finding(
            origin,
            model.REQUIRES_ATTENTION,
            model.impact(model.OK, model.OK, model.LOSS_NO),
            f"Graph relationship changed on `{name}`",
            subject=name,
            record=record,
            detail=why,
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

        cur_map = pdl_parser.mapping_annotations(cur["annotations"])
        tgt_map = pdl_parser.mapping_annotations(tgt["annotations"])
        if cur_map != tgt_map:
            findings.append(
                _finding(
                    origin,
                    model.REQUIRES_ATTENTION,
                    model.impact(model.OK, model.OK, model.LOSS_NO),
                    f"Search mapping changed on `{name}`",
                    subject=name,
                    record=record,
                    detail=(
                        "N built the search index with a different mapping for this "
                        "field. N-1's system-update sees the difference but skips the "
                        "index, because it already built it before the upgrade, even "
                        "with ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX=true; search "
                        "keeps N's mapping. To rebuild it, delete N-1's "
                        "BuildIndicesIncremental upgrade result and re-run system-update."
                    ),
                    reindex_required=True,
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
        findings.append(
            _finding(
                origin,
                model.EXPECTED_LOSS,
                model.impact(model.RESTORE_FAILS, model.FAILS, model.LOSS_NO),
                "New file in N",
                "absent in N-1 (N-1 rejects writes to it)",
                detail=(
                    "N-1 can't read or write this aspect; its entities' other "
                    "aspects read normally. N-1's restore-indices skips the whole "
                    "batch these rows are in, valid rows included, without reporting "
                    "an error, unless N-1 itself has a fix that ignores unknown "
                    "aspects."
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


def aspects_using(
    changed_fqns: set[str], contents: dict[str, str]
) -> dict[str, set[str]]:
    """For each changed record, the aspect names that reach it through field
    types or `includes`, directly or through other records."""
    reverse: dict[str, set[str]] = {}
    aspect_of: dict[str, str] = {}
    for path, content in contents.items():
        own = pdl_parser.fqn_of_path(path)
        meta = pdl_parser.parse(content).aspect
        if meta and meta.get("name"):
            aspect_of[own] = meta["name"]
        for dep in bsv.resolve_dependencies(content):
            if dep != own:
                reverse.setdefault(dep, set()).add(own)
    result: dict[str, set[str]] = {}
    for fqn in changed_fqns:
        seen, queue, aspects = {fqn}, [fqn], set()
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


def _aspects_using_at(ref: str, fqns: set[str]) -> dict[str, set[str]]:
    """`_aspects_using` over every PDL file at `ref`."""
    all_paths = [
        p
        for p in repo.git(
            "ls-tree", "-r", "--name-only", ref, "--", rac.PDL_PREFIX
        ).split()
        if p.endswith(".pdl")
    ]
    return aspects_using(fqns, repo.read_files_at(ref, all_paths))


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
    users = _aspects_using_at(current, set(changed))
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
        if not aspects:
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
