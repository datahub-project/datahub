#!/usr/bin/env python3
"""
Rollback compatibility report between two DataHub releases (N → N-1).

Analyzes PDL schema changes, aspect migration mutators, upgrade steps, and
schema version gaps to produce a per-change risk classification and a
top-level feasibility verdict for rolling back from N to N-1.

Usage:
    python3 .github/scripts/rollback_analysis.py --current v2.3.0rc7-cloud --target v2.2.3-cloud
    python3 .github/scripts/rollback_analysis.py --current HEAD --target v2.2.3-cloud --output report.md --json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import bump_schema_versions as bsv
import report_aspect_changes as rac

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SAFE = "safe"
REQUIRES_ATTENTION = "requires_attention"
BLOCKS_ROLLBACK = "blocks_rollback"

DIM_PDL_SCHEMA = "pdl_schema"
DIM_MUTATOR = "mutator"
DIM_UPGRADE_STEP = "upgrade_step"
DIM_REINDEX = "reindex"
DIM_SCHEMA_VERSION = "schema_version"

VERDICT_FEASIBLE = "feasible_as_is"
VERDICT_MANUAL = "feasible_with_manual_intervention"
VERDICT_NOT_RECOMMENDED = "not_recommended"

VERDICT_LABELS = {
    VERDICT_FEASIBLE: "✅ Feasible as-is",
    VERDICT_MANUAL: "⚠️ Feasible with manual intervention",
    VERDICT_NOT_RECOMMENDED: "\U0001f6d1 Not recommended",
}

VERDICT_DESCRIPTIONS = {
    VERDICT_FEASIBLE: (
        "All changes are safe; rollback N → N-1 requires no manual steps."
    ),
    VERDICT_MANUAL: (
        "Some changes need review or manual action, but none categorically "
        "block rollback."
    ),
    VERDICT_NOT_RECOMMENDED: (
        "One or more changes block rollback without prior remediation."
    ),
}

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class RollbackFinding:
    dimension: str
    risk: str
    path: str
    aspect_name: Optional[str]
    summary: str
    detail: Optional[str] = None
    pr_number: Optional[str] = None
    author: Optional[str] = None
    reindex_required: bool = False
    # Aspects a nested-record change reaches (empty for top-level findings).
    affected_aspects: list[str] = field(default_factory=list)
    # Impact on N-1 after rollback, assuming default write validation.
    read_impact: Optional[str] = None
    write_impact: Optional[str] = None
    data_loss: Optional[str] = None


DROPS_NEW_FIELD = "ok, drops N's new field"


def _impact(read: str, write: str, data_loss: str) -> dict[str, str]:
    return {"read_impact": read, "write_impact": write, "data_loss": data_loss}


# ---------------------------------------------------------------------------
# Dimension 1 + 4: PDL schema changes (includes reindex triggers)
# ---------------------------------------------------------------------------


_INLINE_ENUM_RE = re.compile(r"^enum\s+(\w+)\s*\{[^{}]*\}$", re.DOTALL)


# Annotations that shape the search index mapping.
_MAPPING_ANNOTATIONS = ("Searchable", "SearchableRef")


def _normalized(value: object) -> Optional[str]:
    """Annotation value with whitespace and trailing commas removed."""
    if value is None:
        return None
    return re.sub(r",(?=[}\]])", "", re.sub(r"\s+", "", str(value)))


def _mapping_annotations(annotations: dict) -> dict[str, str]:
    """Mapping-affecting annotations, with whitespace and trailing commas
    normalised so formatting-only edits don't count as changes."""
    out = {}
    for key in _MAPPING_ANNOTATIONS:
        if key in annotations:
            out[key] = re.sub(r",(?=[}\]])", "", re.sub(r"\s+", "", str(annotations[key])))
    return out


# (N-1 type, N type) pairs where N-1 holds every value N can write.
_LOSSLESS_NUMERIC = {("long", "int"), ("double", "int"), ("double", "float")}
_NUMERIC = {"int", "long", "float", "double"}


def _type_change_finding(
    name: str, tgt_t: str, cur_t: str, path: str,
    aspect_name: Optional[str], pr: Optional[str], author: Optional[str],
    where: str = "",
) -> "RollbackFinding":
    summary = where + f"Type change on `{name}`: `{tgt_t}`→`{cur_t}`"
    if tgt_t in _NUMERIC and cur_t in _NUMERIC:
        # Pegasus converts between number types with Number.intValue() etc.
        if (tgt_t, cur_t) in _LOSSLESS_NUMERIC:
            return RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
                aspect_name=aspect_name, **_impact("ok", "ok", "no"),
                summary=f"{summary} — N-1's type holds every value",
                pr_number=pr, author=author,
            )
        return RollbackFinding(
            dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
            aspect_name=aspect_name,
            **_impact("ok, may truncate", "ok", "if out of range"),
            summary=summary,
            detail=(
                f"N-1 converts N's `{cur_t}` values to `{tgt_t}` without error; "
                f"values outside `{tgt_t}`'s range are silently truncated."
            ),
            pr_number=pr, author=author,
        )
    return RollbackFinding(
        dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
        aspect_name=aspect_name, **_impact("API fails", "fails", "no"),
        summary=summary,
        detail=(
            "N-1's typed getters throw on N's values and writes fail schema "
            "validation. Raw storage reads only log a warning."
        ),
        pr_number=pr, author=author,
    )


_ENUM_BLOCK_RE = re.compile(r"\benum\s+(\w+)\s*\{((?:[^{}]|\{[^{}]*\})*)\}")
# An annotation with an optional value: "...", {...} or a bare token.
_ANNOTATION_RE = re.compile(r'@[\w.]+(?:\s*=\s*(?:"(?:[^"\\]|\\.)*"|\{[^{}]*\}|\S+))?')
_SYMBOL_RE = re.compile(r"[A-Za-z_]\w*")


def _enums(pdl: str) -> dict[str, list[str]]:
    """Enum symbols per enum, ignoring comments, commas and annotations such
    as `@deprecated = "..."` (which `rac.enums` splits into fake symbols)."""
    cleaned = rac._strip_comments(pdl)
    out: dict[str, list[str]] = {}
    for m in _ENUM_BLOCK_RE.finditer(cleaned):
        body = _ANNOTATION_RE.sub(" ", m.group(2))
        out[m.group(1)] = _SYMBOL_RE.findall(body)
    return out


def _record_top_level(pdl: str, record: str) -> Optional[str]:
    """`record`'s body with comments removed and nested blocks blanked out,
    so only its own field declarations remain, one per line."""
    text = rac._strip_comments(pdl)
    m = re.search(rf"\brecord\s+{re.escape(record)}\b[^{{]*\{{", text)
    if not m:
        return None
    end = bsv._skip_balanced(text, m.end() - 1)
    if end is None:
        return None
    body = text[m.end() : end - 1]
    # Nested text (including its newlines) becomes spaces, so a default
    # written after an inline record or map stays on the field's line.
    return "".join(
        ch if depth == 0 else " " for _, ch, depth in _top_level_chars(body)
    )


def _has_default(pdl: str, field_name: str, record: Optional[str]) -> bool:
    """True if `record`'s own declaration of the field assigns a default.

    Read from the source because the field parser keeps some defaults in the
    type text (enums) and drops others (numbers). Scoped to the record so a
    same-named field elsewhere in the file doesn't count.
    """
    if not record:
        return False
    body = _record_top_level(pdl, record)
    if body is None:
        return False
    pattern = rf"^\s*{re.escape(field_name)}\s*:[^\n]*="
    return re.search(pattern, body, re.MULTILINE) is not None


def _top_level_chars(text: str):
    """Yield (index, char, depth) for `text`, skipping string literals.
    Depth counts open (), [] and {} so nested types can be told apart."""
    depth, in_str, escaped = 0, False, False
    for i, ch in enumerate(text):
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch in "{[(":
            depth += 1
        elif ch in "}])":
            depth -= 1
        yield i, ch, depth


def _strip_top_level_default(type_text: str) -> str:
    """Type text without the field's own `= default`. An `=` inside an inline
    record, union or map belongs to that nested type and is kept."""
    for i, ch, depth in _top_level_chars(type_text):
        if ch == "=" and depth == 0:
            return type_text[:i].strip()
    return type_text.strip()


def _comparable_type(type_text: str) -> str:
    """Field type without its default, with an inline enum reduced to its name.

    A default change or moving an enum into its own file doesn't change the
    stored type; enum symbol changes are reported separately.
    """
    t = _strip_top_level_default(type_text)
    m = _INLINE_ENUM_RE.match(t)
    return m.group(1) if m else t


def _diff_fields(
    cur_fields: dict, tgt_fields: dict,
    cur_enums: dict[str, list[str]], tgt_enums: dict[str, list[str]],
    target_content: Optional[str], path: str, aspect_name: Optional[str],
    pr: Optional[str], author: Optional[str], where: str = "",
) -> list[RollbackFinding]:
    """Field and enum changes of one record, classified for N-1. `where`
    prefixes summaries for nested records (e.g. "In `Foo`: "). Field maps
    come from `_effective_fields`, which sets `has_default` on every field."""
    findings: list[RollbackFinding] = []

    def has_default(name: str) -> bool:
        return tgt_fields[name]["has_default"]

    for name in sorted(set(cur_fields) - set(tgt_fields)):
        findings.append(RollbackFinding(
            dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
            aspect_name=aspect_name,
            **_impact("ok", DROPS_NEW_FIELD, "no"),
            summary=where + f"Added field `{name}`{_via(cur_fields[name])} — N-1 ignores unknown fields",
            pr_number=pr, author=author,
        ))

    for name in sorted(set(tgt_fields) - set(cur_fields)):
        tgt = tgt_fields[name]
        # N-1 fills an absent field from its default, so only a required
        # field without one breaks N-1 on records N wrote without it.
        required_no_default = not tgt["optional"] and not has_default(name)
        if required_no_default:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=BLOCKS_ROLLBACK, path=path,
                aspect_name=aspect_name,
                **_impact("API fails", "fails", "yes"),
                summary=where + f"Removed field `{name}`{_via(tgt)} — required in N-1, no default",
                detail=(
                    "N writes records without this field and N-1 can't read "
                    "or write them. Backfill a value before rolling back."
                ),
                pr_number=pr, author=author,
            ))
        else:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=aspect_name,
                **_impact("ok", "ok", "yes"),
                summary=where + f"Removed field `{name}`{_via(tgt)} — N-1 expects it",
                detail=(
                    "Records N wrote lose this field's value; N-1 reads them "
                    "as empty or with its default."
                ),
                pr_number=pr, author=author,
            ))

    for name in sorted(set(cur_fields) & set(tgt_fields)):
        cur, tgt = cur_fields[name], tgt_fields[name]

        cur_t, tgt_t = _comparable_type(cur["type"]), _comparable_type(tgt["type"])
        if cur_t != tgt_t:
            findings.append(_type_change_finding(
                name, tgt_t, cur_t, path, aspect_name, pr, author, where
            ))

        cur_map = _mapping_annotations(cur["annotations"])
        tgt_map = _mapping_annotations(tgt["annotations"])
        if cur_map != tgt_map:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=aspect_name,
                **_impact("ok", "ok", "no"),
                summary=where + f"Search mapping changed on `{name}`",
                detail=(
                    "N built the search index with a different mapping for this "
                    "field. N-1 reindexes only if "
                    "ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX=true (default "
                    "false); otherwise search keeps N's mapping for this field."
                ),
                pr_number=pr, author=author, reindex_required=True,
            ))

        if _normalized(cur["annotations"].get("Relationship")) != _normalized(
            tgt["annotations"].get("Relationship")
        ):
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=aspect_name,
                **_impact("ok", "ok", "no"),
                summary=where + f"Graph relationship changed on `{name}`",
                detail=(
                    "N built graph edges for this field by its own @Relationship "
                    "rule. N-1 expects its rule, so relationship and lineage "
                    "views can show missing or extra edges until restore-indices "
                    "rebuilds this aspect."
                ),
                pr_number=pr, author=author,
            ))

        # N always writes a field it requires, so N-1 can read it whether or
        # not N-1 requires it.
        if tgt["optional"] and not cur["optional"]:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
                aspect_name=aspect_name,
                **_impact("ok", "ok", "no"),
                summary=where + f"Optional→required flip on `{name}` — safe for rollback",
                pr_number=pr, author=author,
            ))

        # N may write records without a field it made optional. N-1 fills
        # it from its default if it has one; otherwise reading them fails.
        if not tgt["optional"] and cur["optional"] and has_default(name):
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
                aspect_name=aspect_name,
                **_impact("ok", "ok", "no"),
                summary=where + f"Required→optional flip on `{name}` — N-1 uses its default",
                pr_number=pr, author=author,
            ))
        elif not tgt["optional"] and cur["optional"]:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=aspect_name,
                **_impact("API fails", "fails", "no"),
                summary=where + f"Required→optional flip on `{name}` — N-1 requires it",
                detail=(
                    "N may write records without this field; N-1 fails to read "
                    "them. Check for records missing it before rolling back."
                ),
                pr_number=pr, author=author,
            ))

    for ename in sorted(set(cur_enums) & set(tgt_enums)):
        # Unlike an unknown field, an unknown enum symbol can't be trimmed:
        # Pegasus validation rejects it and N-1's getter returns $UNKNOWN,
        # which GraphQL mappers using `valueOf(x.toString())` throw on.
        for v in sorted(set(cur_enums[ename]) - set(tgt_enums[ename])):
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=aspect_name,
                **_impact("UI/API fails", "fails", "no"),
                summary=where + f"Enum `{ename}`: added value `{v}` — N-1 doesn't know it",
                detail=(
                    "Records N writes with this value read as $UNKNOWN in N-1 "
                    "and fail schema validation. Check whether N wrote it and "
                    "whether N-1 reads this field before rolling back."
                ),
                pr_number=pr, author=author,
            ))
        # A value removed in N is never in N's data, so it can't affect N-1;
        # it only matters when rolling forward, which is out of scope.
    return findings


def classify_pdl_for_rollback(
    path: str, current: str, target: str, read: Optional[Reader] = None,
) -> list[RollbackFinding]:
    """Classify field/enum/rename changes for rollback risk.

    Direction is reversed compared to forward-compatibility: a field *added*
    in N means N-1 doesn't know about it.
    """
    findings: list[RollbackFinding] = []
    read = read or rac.file_at
    current_content = read(current, path)
    target_content = read(target, path)

    cur_meta = rac.aspect_meta(current_content) if current_content else None
    tgt_meta = rac.aspect_meta(target_content) if target_content else None
    aspect_name = (cur_meta or {}).get("name") or (tgt_meta or {}).get("name")

    # Skip non-aspect PDL files (enums, shared records) — they're not stored
    # in metadata_aspect_v2 and don't directly affect rollback. Changes
    # propagate via the aspects that include them (captured by schema version
    # gaps on those aspects).
    if not aspect_name:
        return findings

    pr = _first_pr(current, path, target)
    author = _file_author(current, path, target)

    if current_content and not target_content:
        findings.append(RollbackFinding(
            dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
            aspect_name=aspect_name,
            **_impact("restore-indices fails", "fails", "no"),
            summary="New file in N — absent in N-1 (N-1 rejects writes to it)",
            detail=(
                "N-1's restore-indices fails on these rows and skips the whole "
                "batch, including valid rows. Normal API reads are unaffected."
            ),
            pr_number=pr, author=author,
        ))
        return findings

    if not current_content and target_content:
        findings.append(RollbackFinding(
            dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
            aspect_name=aspect_name,
            **_impact("ok, stale", "ok", "no"),
            summary="File deleted in N — N-1 expects it",
            pr_number=pr, author=author,
        ))
        return findings

    if not current_content and not target_content:
        return findings

    findings.extend(_diff_fields(
        _effective_fields(current_content, current, read=read),
        _effective_fields(target_content, target, read=read),
        _enums(current_content), _enums(target_content),
        target_content, path, aspect_name, pr, author,
    ))

    # --- Record rename ---
    # Aspects are stored as plain field maps and read back by aspect name, so
    # renaming the record (with or without @renamedFrom) doesn't affect N-1.
    cur_name = rac.record_name(current_content)
    tgt_name = rac.record_name(target_content)
    if cur_name and tgt_name and cur_name != tgt_name:
        findings.append(RollbackFinding(
            dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
            aspect_name=aspect_name,
            **_impact("ok", "ok", "no"),
            summary=(
                f"Record renamed `{tgt_name}`→`{cur_name}` — stored by "
                f"aspect name, N-1 unaffected"
            ),
            pr_number=pr, author=author,
        ))

    return findings


# ---------------------------------------------------------------------------
# Nested and shared records (reached through field types or `includes`)
# ---------------------------------------------------------------------------


def _record_fields(rdef: dict) -> dict[str, dict]:
    """`bsv.parse_top_level_defs` fields in the shape `rac.fields` returns."""
    return {
        name: {
            "optional": opt,
            "type": re.sub(r"^optional\s+", "", typ, count=1),
            "annotations": ann,
        }
        for name, (typ, opt, ann) in rdef["fields"].items()
    }


def _pdl_path(fqn: str) -> str:
    return f"{rac.PDL_PREFIX}/{fqn.replace('.', '/')}.pdl"


def _fqn(path: str) -> str:
    return path[len(rac.PDL_PREFIX) + 1 : -len(".pdl")].replace("/", ".")


def _main_record(content: str) -> Optional[dict]:
    # bsv gives up on files with typeref/fixed; read the records without them.
    defs = bsv.parse_top_level_defs(_split_typerefs(content)[2]) if content else None
    name = rac.record_name(content) if content else None
    rdef = (defs or {}).get(name or "")
    return rdef if rdef and rdef["kind"] == "record" else None


Reader = Callable[[str, str], str]


def _cached_reader() -> Reader:
    """`rac.file_at` with a per-run cache: include walks read the same shared
    records (e.g. `CustomProperties`) for many aspects."""
    cache: dict[tuple[str, str], str] = {}

    def read(ref: str, path: str) -> str:
        if (ref, path) not in cache:
            cache[(ref, path)] = rac.file_at(ref, path)
        return cache[(ref, path)]

    return read


def _via(field: dict) -> str:
    return f" (via includes `{field['via']}`)" if field.get("via") else ""


def _effective_fields(
    content: str, ref: str, record: Optional[str] = None,
    rdef: Optional[dict] = None, _seen: Optional[set[str]] = None,
    read: Optional[Reader] = None,
) -> dict[str, dict]:
    """All fields a record stores: its own plus those of included records
    (recursively). Where a field is declared doesn't change the stored data,
    so moving it into or out of an include isn't reported as a change.

    Each field carries `has_default` (looked up in the record that declares
    it) and `via` (the include it came from, if any).
    """
    if not content:
        return {}
    read = read or rac.file_at
    record = record or rac.record_name(content)
    if rdef is None:
        rdef = _main_record(content)
    own = _record_fields(rdef) if rdef else rac.fields(content)
    seen = _seen if _seen is not None else set()
    namespace, imports = bsv.parse_pdl_header(content)
    out: dict[str, dict] = {}
    for short in sorted((rdef or {}).get("includes", set())):
        fqn = imports.get(short) or f"{namespace}.{short}"
        if fqn in seen:
            continue
        seen.add(fqn)
        inc_content = read(ref, _pdl_path(fqn))
        for name, f in _effective_fields(inc_content, ref, _seen=seen, read=read).items():
            out.setdefault(name, {**f, "via": f.get("via") or short})
    for name, f in own.items():
        out[name] = {
            **f, "has_default": _has_default(content, name, record), "via": None,
        }
    return out


def _read_files_at(ref: str, paths: list[str]) -> dict[str, str]:
    """Contents of many files at `ref` with one `git cat-file --batch` call."""
    if not paths:
        return {}
    proc = subprocess.run(
        ["git", "cat-file", "--batch"],
        input="".join(f"{ref}:{p}\n" for p in paths).encode(),
        capture_output=True, cwd=rac.REPO_ROOT, check=True,
    )
    out, pos, result = proc.stdout, 0, {}
    for p in paths:
        nl = out.index(b"\n", pos)
        header = out[pos:nl].decode().split()
        pos = nl + 1
        if len(header) == 3 and header[1] == "blob":
            size = int(header[2])
            result[p] = out[pos : pos + size].decode("utf-8", "replace")
            pos += size + 1
    return result


def _aspects_using(
    changed_fqns: set[str], contents: dict[str, str]
) -> dict[str, set[str]]:
    """For each changed record, the aspect names that reach it through field
    types or `includes`, directly or through other records."""
    reverse: dict[str, set[str]] = {}
    aspect_of: dict[str, str] = {}
    for path, content in contents.items():
        own = _fqn(path)
        meta = rac.aspect_meta(content)
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
        p for p in rac._git("ls-tree", "-r", "--name-only", ref, "--", rac.PDL_PREFIX).split()
        if p.endswith(".pdl")
    ]
    return _aspects_using(fqns, _read_files_at(ref, all_paths))


# Declarations start a line; anchoring keeps text inside string literals
# (e.g. an annotation value "typeref X = long") from matching.
_TYPEREF_RE = re.compile(r"(?m)^[ \t]*typeref\s+(\w+)\s*=\s*")
_FIXED_RE = re.compile(r"(?m)^[ \t]*fixed\s+(\w+)\s+(\d+)")
_TYPE_NAME_RE = re.compile(r"[\w.]+")


def _type_expr_end(text: str, i: int) -> int:
    """End of the type expression starting at `i`: a name, optionally followed
    by a bracketed part (`union[...]`, `array[...]`, `map[...]`)."""
    m = _TYPE_NAME_RE.match(text, i)
    if not m:
        return i
    j = m.end()
    k = j
    while k < len(text) and text[k] in " \t\r\n":
        k += 1
    if k < len(text) and text[k] == "[":
        end = bsv._skip_balanced(text, k)
        return end if end is not None else len(text)
    return j


def _split_typerefs(pdl: str) -> tuple[dict[str, str], dict[str, int], str]:
    """Typerefs {name: type}, fixed types {name: size}, and the rest of the
    file (comments removed) without them, so `bsv.parse_top_level_defs`,
    which gives up on typeref/fixed, can still read the file's records."""
    text = bsv.strip_pdl_comments(pdl)
    typerefs: dict[str, str] = {}
    spans: list[tuple[int, int]] = []
    for m in _TYPEREF_RE.finditer(text):
        end = _type_expr_end(text, m.end())
        typerefs[m.group(1)] = " ".join(text[m.end() : end].split())
        spans.append((m.start(), end))
    fixed: dict[str, int] = {}
    for m in _FIXED_RE.finditer(text):
        fixed[m.group(1)] = int(m.group(2))
        spans.append((m.start(), m.end()))
    rest = text
    for start, end in sorted(spans, reverse=True):
        rest = rest[:start] + rest[end:]
    return typerefs, fixed, rest


_ALIAS_RE = re.compile(r"(\w+)\s*:")
# An inline definition up to its `{`, including an optional `includes` list.
_INLINE_DEF_RE = re.compile(
    r"(record|enum)\s+\w+\s*(?:includes\s+[\w.]+(?:\s*,\s*[\w.]+)*\s*)?(?=\{)"
)


def _union_members(type_text: str) -> Optional[set[str]]:
    """Members of `union[...]` as normalised "alias: type" strings; None if it
    isn't a union. PDL commas are optional, so members are read one by one:
    an optional `alias:`, then a type name (with `[...]`) or an inline
    record/enum definition."""
    t = type_text.strip()
    if not (t.startswith("union") and t.endswith("]") and "[" in t):
        return None
    inner = _ANNOTATION_RE.sub(" ", t[t.index("[") + 1 : -1])
    members: set[str] = set()
    i = 0
    while i < len(inner):
        if inner[i] in " \t\r\n,":
            i += 1
            continue
        start = i
        alias = _ALIAS_RE.match(inner, i)
        if alias and not inner[alias.end() - 1 :].startswith("::"):
            i = alias.end()
            while i < len(inner) and inner[i] in " \t\r\n":
                i += 1
        inline = _INLINE_DEF_RE.match(inner, i)
        if inline:
            end = bsv._skip_balanced(inner, inline.end())
            i = end if end is not None else len(inner)
        else:
            nxt = _type_expr_end(inner, i)
            i = nxt if nxt > i else i + 1
        members.add(" ".join(inner[start:i].split()))
    return members


def _typeref_findings(
    cur: str, tgt: str, path: str, pr: Optional[str], author: Optional[str],
) -> list[RollbackFinding]:
    """Changes to typerefs and fixed types defined in a file."""
    cur_t, cur_f, _ = _split_typerefs(cur)
    tgt_t, tgt_f, _ = _split_typerefs(tgt)
    findings: list[RollbackFinding] = []
    for name in sorted(set(cur_t) & set(tgt_t)):
        old, new = tgt_t[name], cur_t[name]
        old_m, new_m = _union_members(old), _union_members(new)
        if old_m is not None and new_m is not None:
            # A member removed in N never appears in N's data (roll-forward only).
            for member in sorted(new_m - old_m):
                findings.append(RollbackFinding(
                    dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                    aspect_name=None, **_impact("API fails", "fails", "no"),
                    summary=f"Union `{name}`: added member `{member}` — N-1 doesn't know it",
                    detail=(
                        "N-1's typed getters throw on a union member they don't "
                        "know and writes fail schema validation. Check whether N "
                        "wrote this member before rolling back."
                    ),
                    pr_number=pr, author=author,
                ))
        elif _comparable_type(old) != _comparable_type(new):
            findings.append(_type_change_finding(
                name, _comparable_type(old), _comparable_type(new), path, None, pr, author,
            ))
    for name in sorted(set(cur_f) & set(tgt_f)):
        if cur_f[name] != tgt_f[name]:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=None, **_impact("API fails", "fails", "no"),
                summary=f"Fixed `{name}`: size {tgt_f[name]}→{cur_f[name]}",
                detail="N-1 rejects values of a different size.",
                pr_number=pr, author=author,
            ))
    return findings


def _include_closure(content: str, ref: str, seen: set[str], read: Reader) -> set[str]:
    """FQNs of every record `content`'s main record includes, transitively."""
    rdef = _main_record(content)
    if not rdef:
        return seen
    namespace, imports = bsv.parse_pdl_header(content)
    for short in rdef["includes"]:
        fqn = imports.get(short) or f"{namespace}.{short}"
        if fqn not in seen:
            seen.add(fqn)
            _include_closure(read(ref, _pdl_path(fqn)), ref, seen, read)
    return seen


def _include_closures_of_changed_aspects(
    current: str, pdl_paths: list[str], read: Reader
) -> dict[str, set[str]]:
    """{aspect name: records it includes} for aspects whose own file changed."""
    out: dict[str, set[str]] = {}
    for path in pdl_paths:
        content = read(current, path)
        meta = rac.aspect_meta(content) if content else None
        if meta and meta.get("name"):
            out[meta["name"]] = _include_closure(content, current, set(), read)
    return out


def analyze_nested_changes(
    current: str, target: str, pdl_paths: list[str], read: Optional[Reader] = None,
) -> list[RollbackFinding]:
    """Field and enum changes in non-aspect records, reported once per change
    and attributed to every aspect that uses the record."""
    read = read or rac.file_at
    changed: dict[str, tuple[str, str]] = {}
    for path in pdl_paths:
        cur, tgt = read(current, path), read(target, path)
        if cur and tgt and not rac.aspect_meta(cur):
            changed[_fqn(path)] = (cur, tgt)
    if not changed:
        return []
    users = _aspects_using_at(current, set(changed))
    covered = _include_closures_of_changed_aspects(current, pdl_paths, read)

    findings: list[RollbackFinding] = []
    for fqn, (cur, tgt) in sorted(changed.items()):
        aspects = sorted(users.get(fqn, ()))
        if bsv.normalize_pdl_for_compare(cur) == bsv.normalize_pdl_for_compare(tgt):
            continue  # comments or formatting only
        # An aspect whose own file changed already compares this record's
        # *fields* through its `includes` (see _effective_fields). Enum values
        # and unparseable files aren't covered that way, so they still count.
        field_aspects = [a for a in aspects if fqn not in covered.get(a, ())]
        if not aspects:
            continue
        path = _pdl_path(fqn)
        pr = _first_pr(current, path, target)
        author = _file_author(current, path, target)
        # Typerefs and fixed types are compared here; bsv's record parser gives
        # up on files containing them, so it reads the rest of the file.
        record_findings: list[RollbackFinding] = _typeref_findings(cur, tgt, path, pr, author)
        field_findings: list[RollbackFinding] = []
        cur_defs = bsv.parse_top_level_defs(_split_typerefs(cur)[2])
        tgt_defs = bsv.parse_top_level_defs(_split_typerefs(tgt)[2])
        if cur_defs is None or tgt_defs is None:
            # Never let a change the parser can't read pass silently.
            record_findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=None,
                **_impact("not analysed", "not analysed", "not analysed"),
                summary=f"`{fqn.rsplit('.', 1)[-1]}` changed but couldn't be analysed",
                detail=(
                    "The file uses a construct this tool doesn't parse. Check "
                    "the PR for changes N-1 can't read."
                ),
                pr_number=pr, author=author,
            ))
            cur_defs, tgt_defs = {}, {}
        for name in sorted(set(cur_defs) & set(tgt_defs)):
            if cur_defs[name]["kind"] == tgt_defs[name]["kind"] == "record":
                field_findings.extend(_diff_fields(
                    _effective_fields(cur, current, name, cur_defs[name], read=read),
                    _effective_fields(tgt, target, name, tgt_defs[name], read=read),
                    {}, {}, tgt, path, None, pr, author, f"In `{name}`: ",
                ))
        record_findings.extend(_diff_fields(
            {}, {}, _enums(cur), _enums(tgt), tgt, path, None, pr, author,
        ))
        for group, users_of in ((record_findings, aspects), (field_findings, field_aspects)):
            if not users_of:
                continue
            shown = ", ".join(users_of[:3]) + (f" +{len(users_of) - 3} more" if len(users_of) > 3 else "")
            for f in group:
                f.aspect_name = shown
                f.affected_aspects = users_of
                used_by = f"Used by: {', '.join(users_of)}."
                f.detail = f"{f.detail} {used_by}" if f.detail else used_by
            findings.extend(group)
    return findings


def attribute_embedded_aspect_changes(
    findings: list[RollbackFinding], current: str, target: str, pdl_paths: list[str]
) -> None:
    """An aspect record can also be embedded as a field of another aspect (e.g.
    `IncidentInfo` inside `IncidentActivityEvent`). `analyze_nested_changes`
    skips aspect records, so attribute the aspect's own field and annotation
    changes to every aspect that embeds it; otherwise the embedding aspect's
    version bump looks unexplained."""
    changed: dict[str, str] = {}
    for path in pdl_paths:
        cur, tgt = rac.file_at(current, path), rac.file_at(target, path)
        meta = rac.aspect_meta(cur) if cur and tgt else None
        if meta and meta.get("name"):
            changed[path] = meta["name"]
    by_path: dict[str, list[RollbackFinding]] = {}
    for f in findings:
        if f.dimension == DIM_PDL_SCHEMA and f.path in changed and f.aspect_name == changed[f.path]:
            by_path.setdefault(f.path, []).append(f)
    if not by_path:
        return
    users = _aspects_using_at(current, {_fqn(p) for p in by_path})
    for path, own in by_path.items():
        embedders = sorted(users.get(_fqn(path), set()) - {changed[path]})
        if not embedders:
            continue
        also = f"Also embedded in: {', '.join(embedders)}."
        for f in own:
            f.affected_aspects = sorted(set(f.affected_aspects) | set(embedders))
            f.detail = f"{f.detail} {also}" if f.detail else also


# ---------------------------------------------------------------------------
# Dimension 2: Mutator migrations
# ---------------------------------------------------------------------------

_METHOD_RETURN_INT_RE_CACHE: dict[str, re.Pattern[str]] = {}


def _extract_method_return_int(
    java_content: str, method_name: str
) -> Optional[int]:
    if method_name not in _METHOD_RETURN_INT_RE_CACHE:
        _METHOD_RETURN_INT_RE_CACHE[method_name] = re.compile(
            rf"{method_name}\s*\(\s*\)\s*\{{\s*return\s+(\d+)[Ll]?\s*;",
            re.DOTALL,
        )
    m = _METHOD_RETURN_INT_RE_CACHE[method_name].search(java_content)
    return int(m.group(1)) if m else None


def java_files_added_between(base: str, head: str) -> set[str]:
    """Paths of .java files present at `head` but not at `base`.

    `git log --diff-filter=A base..head` also lists files re-added by root
    commits (history rewrites re-add the whole tree), which can mean tens of
    thousands of files that already existed at `base`. Filtering log entries
    through this set keeps only real additions.
    """
    try:
        out = rac._git(
            "diff", "--diff-filter=A", "--name-only", base, head, "--", "*.java"
        )
    except subprocess.CalledProcessError:
        return set()
    return {line.strip() for line in out.splitlines() if line.strip()}


def find_mutators_added_in_window(
    base: str, head: str, hierarchy: set[str]
) -> list[dict]:
    """Like `rac.find_mutators_added_in_window`, but only for files absent at `base`.

    Rollback cares about mutators N has and N-1 lacks, so mutators backported
    to N-1 are not new. Kept separate so the PDL change report is unchanged.
    """
    results: list[dict] = []
    constants_map = rac._load_aspect_name_constants()
    try:
        out = rac._git(
            "log", "--diff-filter=A", "--name-only",
            "--format=COMMIT %H %s", f"{base}..{head}", "--", "*.java",
        )
    except subprocess.CalledProcessError:
        return results

    added = java_files_added_between(base, head)
    current_sha: Optional[str] = None
    current_subject: str = ""
    for line in out.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("COMMIT "):
            parts = line.split(" ", 2)
            current_sha = parts[1] if len(parts) > 1 else None
            current_subject = parts[2] if len(parts) > 2 else ""
            continue
        if (
            not line.endswith(".java")
            or "/test/" in line
            or current_sha is None
            or line not in added
        ):
            continue
        try:
            content = rac._git("show", f"{current_sha}:{line}")
        except subprocess.CalledProcessError:
            continue
        cp = rac._extract_class_and_parent(content)
        if not cp:
            continue
        class_name, parent = cp
        if parent not in hierarchy:
            continue
        try:
            author = rac._git(
                "log", "-1", "--format=%an", current_sha
            ).strip() or None
        except subprocess.CalledProcessError:
            author = None
        results.append({
            "sha": current_sha[:10],
            "pr": rac._extract_pr_number(current_subject),
            "path": line,
            "class_name": class_name,
            "parent": parent,
            "author": author,
            "subject": current_subject,
            "target_aspect": rac._extract_mutator_target_aspect(
                content, constants_map
            ),
        })
    return results


def classify_mutators_for_rollback(
    current: str, target: str
) -> list[RollbackFinding]:
    """Classify new mutators for rollback risk.

    Rollback uses retention + Kafka replay (not reverse transforms). All new
    mutators are flagged as requires_attention — the operator must verify that
    the retention window and Kafka replay cover the mutated data.
    """
    hierarchy = rac.discover_mutator_hierarchy()
    mutators = find_mutators_added_in_window(target, current, hierarchy)

    # Deduplicate by (path, class_name) — same mutator touched by multiple PRs
    # should be a single finding with merged PR numbers.
    seen: dict[tuple[str, str], dict] = {}
    for m in mutators:
        key = (m["path"], m["class_name"])
        if key not in seen:
            seen[key] = {**m, "_prs": []}
        pr = m.get("pr")
        if pr and pr not in seen[key]["_prs"]:
            seen[key]["_prs"].append(pr)

    findings: list[RollbackFinding] = []
    for (path, _cls), m in seen.items():
        content = rac.file_at(current, path)
        if not content:
            continue

        src_v = _extract_method_return_int(content, "getSourceVersion")
        tgt_v = _extract_method_return_int(content, "getTargetVersion")
        hop = f"v{src_v}→v{tgt_v}" if src_v is not None and tgt_v is not None else ""

        hop_label = f" ({hop})" if hop else ""
        summary = (
            f"New mutator `{m['class_name']}`{hop_label} — "
            f"check what it changes"
        )
        pr_str = ", ".join(m["_prs"]) if m["_prs"] else None

        findings.append(RollbackFinding(
            dimension=DIM_MUTATOR, risk=REQUIRES_ATTENTION, path=path,
            aspect_name=m.get("target_aspect"),
            summary=summary,
            detail=None,  # set by _set_mutator_impact from the field changes
            pr_number=pr_str, author=m.get("author"),
        ))

    return findings


# ---------------------------------------------------------------------------
# Dimension 3: Upgrade steps
# ---------------------------------------------------------------------------

_BLOCKING_STEP = "BlockingSystemUpgrade"
_NON_BLOCKING_STEP = "NonBlockingSystemUpgrade"
_IMPLEMENTS_STEP_RE = re.compile(
    rf"\bimplements\s+[^{{]*?\b({_BLOCKING_STEP}|{_NON_BLOCKING_STEP})\b"
)
_CLASS_NAME_RE = re.compile(r"\bclass\s+(\w+)")


def _class_and_parent(content: str) -> Optional[tuple[str, Optional[str]]]:
    """(class name, superclass or None). Most upgrade steps only `implement`
    their interface, so a missing `extends` must not drop the class."""
    cp = rac._extract_class_and_parent(content)
    if cp:
        return cp
    m = _CLASS_NAME_RE.search(content)
    return (m.group(1), None) if m else None


def discover_upgrade_step_hierarchy() -> dict[str, str]:
    """Class names that implement BlockingSystemUpgrade or NonBlockingSystemUpgrade,
    directly or through a superclass. Returns {class_name: step_type}.

    Searches one inheritance level at a time with a single `git grep` per
    level, since most steps only `implement` the interface and a grep per
    class is slow on this repo.
    """
    hierarchy: dict[str, str] = {}
    frontier = {_BLOCKING_STEP: _BLOCKING_STEP, _NON_BLOCKING_STEP: _NON_BLOCKING_STEP}
    while frontier:
        names = "|".join(sorted(frontier))
        # Plain ERE (no \\b): a broad pre-filter, each file is checked below.
        pattern = rf"(implements|extends)[^{{]*({names})"
        try:
            out = rac._git("grep", "-l", "-E", pattern, "--", "*.java")
        except subprocess.CalledProcessError:
            break
        next_frontier: dict[str, str] = {}
        for path in out.strip().splitlines():
            if "/test/" in path:
                continue
            try:
                content = Path(rac.REPO_ROOT / path).read_text(encoding="utf-8")
            except OSError:
                continue
            cp = _class_and_parent(content)
            if not cp or cp[0] in hierarchy:
                continue
            m = _IMPLEMENTS_STEP_RE.search(content)
            root = m.group(1) if m else frontier.get(cp[1] or "")
            if root:
                hierarchy[cp[0]] = root
                next_frontier[cp[0]] = root
        frontier = next_frontier
    return hierarchy


def find_upgrade_steps_added_in_window(
    base: str, head: str
) -> list[dict]:
    hierarchy = discover_upgrade_step_hierarchy()
    results: list[dict] = []
    try:
        out = rac._git(
            "log", "--diff-filter=A", "--name-only",
            "--format=COMMIT %H %s", f"{base}..{head}", "--", "*.java",
        )
    except subprocess.CalledProcessError:
        return results

    added = java_files_added_between(base, head)
    current_sha: Optional[str] = None
    current_subject: str = ""
    for line in out.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("COMMIT "):
            parts = line.split(" ", 2)
            current_sha = parts[1] if len(parts) > 1 else None
            current_subject = parts[2] if len(parts) > 2 else ""
            continue
        if (
            not line.endswith(".java")
            or "/test/" in line
            or current_sha is None
            or line not in added
        ):
            continue
        try:
            content = rac._git("show", f"{current_sha}:{line}")
        except subprocess.CalledProcessError:
            continue

        cp = _class_and_parent(content)
        if not cp:
            continue
        class_name, parent = cp

        step_type: Optional[str] = None
        m = _IMPLEMENTS_STEP_RE.search(content)
        if m:
            step_type = m.group(1)
        elif parent in hierarchy:
            step_type = hierarchy[parent]

        if not step_type:
            continue

        try:
            author = rac._git(
                "log", "-1", "--format=%an", current_sha
            ).strip() or None
        except subprocess.CalledProcessError:
            author = None

        results.append({
            "sha": current_sha[:10],
            "pr": rac._extract_pr_number(current_subject),
            "path": line,
            "class_name": class_name,
            "step_type": step_type,
            "author": author,
            "subject": current_subject,
        })
    return results


def classify_upgrade_steps_for_rollback(
    current: str, target: str
) -> list[RollbackFinding]:
    # The same file can be added by several commits in the window (e.g. a
    # revert and re-land); report each step once with all its PRs.
    seen: dict[tuple[str, str], dict] = {}
    for s in find_upgrade_steps_added_in_window(target, current):
        key = (s["path"], s["class_name"])
        entry = seen.setdefault(key, {**s, "_prs": []})
        if s.get("pr") and s["pr"] not in entry["_prs"]:
            entry["_prs"].append(s["pr"])
    findings: list[RollbackFinding] = []
    for s in seen.values():
        findings.append(RollbackFinding(
            dimension=DIM_UPGRADE_STEP,
            risk=REQUIRES_ATTENTION,
            path=s["path"],
            aspect_name=None,
            **_impact("unknown", "unknown", "unknown"),
            summary=f"New {s['step_type']}: `{s['class_name']}`",
            detail="Verify idempotency and rollback safety",
            pr_number=", ".join(s["_prs"]) or None,
            author=s.get("author"),
        ))
    return findings


# ---------------------------------------------------------------------------
# Dimension 5: Schema version gap analysis
# ---------------------------------------------------------------------------


def analyze_schema_version_gaps(
    current: str, target: str, pdl_paths: list[str]
) -> list[RollbackFinding]:
    findings: list[RollbackFinding] = []
    for path in pdl_paths:
        cur_content = rac.file_at(current, path)
        tgt_content = rac.file_at(target, path)
        cur_meta = rac.aspect_meta(cur_content) if cur_content else None
        tgt_meta = rac.aspect_meta(tgt_content) if tgt_content else None
        if not cur_meta or not tgt_meta:
            continue
        cur_v = cur_meta.get("schemaVersion") or 1
        tgt_v = tgt_meta.get("schemaVersion") or 1
        if cur_v > tgt_v:
            gap = cur_v - tgt_v
            findings.append(RollbackFinding(
                dimension=DIM_SCHEMA_VERSION,
                risk=SAFE,
                path=path,
                aspect_name=cur_meta.get("name"),
                **_impact("ok", "ok", "no"),
                summary=(
                    f"Schema version gap: v{tgt_v}→v{cur_v} "
                    f"({gap} hop{'s' if gap > 1 else ''})"
                ),
                detail=(
                    f"N-1 reads records at version {cur_v} and writes its own "
                    f"version {tgt_v}. Field-level changes for this aspect are "
                    f"listed separately."
                ),
                pr_number=_first_pr(current, path, target),
                author=_file_author(current, path, target),
            ))
    return findings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _first_pr(head: str, path: str, base: str) -> Optional[str]:
    prs = rac.pr_numbers_for_file(head, path, base)
    return prs[0] if prs else None


def _file_author(head: str, path: str, base: str) -> Optional[str]:
    return rac.last_author_for_file(head, path, base)


def _resolve_ref_name(ref: str) -> str:
    """`ref`, or `origin/<ref>` when only the remote-tracking branch exists
    (CI checkouts usually have no local branches)."""
    return rac._resolve_ref(ref, f"origin/{ref}") or ref


def _resolve_sha(ref: str) -> str:
    try:
        return rac._git("rev-parse", ref).strip()
    except subprocess.CalledProcessError:
        print(f"Error: could not resolve ref '{ref}'", file=sys.stderr)
        raise SystemExit(2)


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------


def compute_verdict(findings: list[RollbackFinding]) -> str:
    risks = {f.risk for f in findings}
    if BLOCKS_ROLLBACK in risks:
        return VERDICT_NOT_RECOMMENDED
    if REQUIRES_ATTENTION in risks:
        return VERDICT_MANUAL
    return VERDICT_FEASIBLE


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------


def _format_pr(pr_number: Optional[str]) -> str:
    if not pr_number:
        return "—"
    return ", ".join(f"#{p.strip()}" for p in pr_number.split(","))


def render_rollback_report(
    findings: list[RollbackFinding],
    current: str,
    target: str,
    current_sha: str,
    target_sha: str,
    warning: Optional[str] = None,
) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    verdict = compute_verdict(findings)

    blockers = [f for f in findings if f.risk == BLOCKS_ROLLBACK]
    attention = [f for f in findings if f.risk == REQUIRES_ATTENTION]
    safe = [f for f in findings if f.risk == SAFE]

    lines = [
        f"# Rollback Compatibility Report: {current} → {target}",
        "",
        f"**Current (N):** `{current}` (sha: `{current_sha[:10]}`)  ",
        f"**Target (N-1):** `{target}` (sha: `{target_sha[:10]}`)  ",
        f"**Generated:** {generated}",
        "",
    ]
    if warning:
        lines += [f"> ⚠️ **Warning:** {warning}", ""]
    lines += [
        f"## Verdict: {VERDICT_LABELS[verdict]}",
        "",
        f"> {VERDICT_DESCRIPTIONS[verdict]}",
        "",
        (
            f"**Summary:** {len(findings)} changes analyzed · "
            f"{len(safe)} safe · {len(attention)} require attention · "
            f"{len(blockers)} block{'s' if len(blockers) == 1 else ''} rollback"
        ),
        "",
        "---",
        "",
    ]

    if not findings:
        lines.append(
            f"No schema, mutator, or upgrade-step changes between "
            f"`{target}` and `{current}`."
        )
        return "\n".join(lines) + "\n"

    lines.extend([
        "_N-1 read / N-1 write / N-1 data loss: what happens on N-1 to records "
        "N wrote, after rolling back._",
        "",
    ])

    if blockers:
        lines.extend(_render_finding_table("Blockers", blockers))

    if attention:
        lines.extend(_render_finding_table("Requires Attention", attention))

    if safe:
        lines.extend([
            "<details>",
            f"<summary>Safe Changes ({len(safe)})</summary>",
            "",
        ])
        lines.extend(
            _render_finding_table("Safe Changes", safe, heading=False)
        )
        lines.extend(["</details>", ""])

    mutator_findings = [f for f in findings if f.dimension == DIM_MUTATOR]
    if mutator_findings:
        lines.extend(_render_mutator_section(mutator_findings))

    step_findings = [f for f in findings if f.dimension == DIM_UPGRADE_STEP]
    if step_findings:
        lines.extend(_render_step_section(step_findings))

    reindex_findings = [f for f in findings if f.reindex_required]
    if reindex_findings:
        lines.extend(_render_reindex_section(reindex_findings))

    version_findings = [f for f in findings if f.dimension == DIM_SCHEMA_VERSION]
    if version_findings:
        lines.extend(_render_version_section(version_findings))

    return "\n".join(lines) + "\n"


def _table_cell(text: Optional[str]) -> str:
    """Make free text safe for a single markdown table cell."""
    if not text:
        return ""
    return " ".join(text.split()).replace("|", "\\|")


_TABLE_DESCRIPTIONS = {
    "Blockers": (
        "Don't roll back until these are handled: N-1 can't read or write "
        "data N wrote."
    ),
    "Requires Attention": (
        "Rollback can go ahead, but check each item first: N-1 may fail on, "
        "drop, or misread some data N wrote. Why / action says what to check."
    ),
    "Safe Changes": "N-1 handles these on its own; no action needed.",
}


def _render_finding_table(
    title: str,
    findings: list[RollbackFinding],
    heading: bool = True,
) -> list[str]:
    lines: list[str] = []
    if heading:
        lines.extend([f"## {title}", ""])
    if title in _TABLE_DESCRIPTIONS:
        lines.extend([f"_{_TABLE_DESCRIPTIONS[title]}_", ""])
    columns = [
        "#", "Dimension", "Aspect", "Risk", "Summary",
        "N-1 read", "N-1 write", "N-1 data loss", "Why / action", "PR",
    ]
    lines.extend([
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ])
    for i, f in enumerate(findings, 1):
        aspect = f"`{f.aspect_name}`" if f.aspect_name else "—"
        cells = [
            str(i), f.dimension, aspect, f.risk, f.summary,
            f.read_impact or "", f.write_impact or "", f.data_loss or "",
        ]
        cells += [_table_cell(f.detail), _format_pr(f.pr_number)]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    return lines


def _render_mutator_section(findings: list[RollbackFinding]) -> list[str]:
    lines = [
        "## Mutators in Window",
        "",
        "| Mutator | Aspect | Version Hop | Risk | PR |",
        "| --- | --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        aspect = f.aspect_name or "—"
        cls_m = re.search(r"`([^`]+)`", f.summary)
        cls = cls_m.group(1) if cls_m else "?"
        hop_m = re.search(r"\(v\d+→v\d+\)", f.summary)
        hop = hop_m.group(0).strip("()") if hop_m else "—"
        lines.append(f"| `{cls}` | {aspect} | {hop} | {f.risk} | {pr} |")
    lines.append("")
    return lines


def _render_step_section(findings: list[RollbackFinding]) -> list[str]:
    lines = [
        "## Upgrade Steps in Window",
        "",
        "| Step | Type | Risk | PR |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        cls_m = re.search(r"`([^`]+)`", f.summary)
        cls = cls_m.group(1) if cls_m else "?"
        step_type = "Non-blocking" if "NonBlocking" in f.summary else "Blocking"
        lines.append(f"| `{cls}` | {step_type} | {f.risk} | {pr} |")
    lines.append("")
    return lines


def _render_reindex_section(findings: list[RollbackFinding]) -> list[str]:
    lines = [
        "## Reindex Triggers",
        "",
        "| Aspect | Field | Reason | PR |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        aspect = _table_cell(f.aspect_name) or "—"
        # Summaries look like "[In `Rec`: ]<change> on `field`[ (via ...)]".
        m = re.match(r"(?:In `([^`]+)`: )?[^`]*`([^`]+)`", f.summary)
        field = (f"{m.group(1)}.{m.group(2)}" if m.group(1) else m.group(2)) if m else "—"
        reason = _table_cell(f.detail or f.summary)
        lines.append(f"| {aspect} | `{field}` | {reason} | {pr} |")
    lines.append("")
    return lines


def _render_version_section(findings: list[RollbackFinding]) -> list[str]:
    lines = [
        "## Schema Version Gaps",
        "",
        "| Aspect | Gap | Detail | PR |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        aspect = f.aspect_name or "—"
        lines.append(f"| {aspect} | {f.summary} | {f.detail or ''} | {pr} |")
    lines.append("")
    return lines


# ---------------------------------------------------------------------------
# JSON report
# ---------------------------------------------------------------------------


def render_json_report(
    findings: list[RollbackFinding],
    current: str,
    target: str,
    current_sha: str,
    target_sha: str,
    warning: Optional[str] = None,
) -> str:
    verdict = compute_verdict(findings)
    data = {
        "current": current,
        "current_sha": current_sha[:10],
        "target": target,
        "target_sha": target_sha[:10],
        "generated": datetime.now(timezone.utc).isoformat(),
        "warning": warning,
        "verdict": verdict,
        "verdict_label": VERDICT_LABELS[verdict],
        "summary": {
            "total": len(findings),
            "safe": sum(1 for f in findings if f.risk == SAFE),
            "requires_attention": sum(
                1 for f in findings if f.risk == REQUIRES_ATTENTION
            ),
            "blocks_rollback": sum(
                1 for f in findings if f.risk == BLOCKS_ROLLBACK
            ),
        },
        "findings": [asdict(f) for f in findings],
    }
    return json.dumps(data, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


_REF_VERSION_RE = re.compile(r"^v?(\d+(?:\.\d+){2,3})")


def _ref_version(ref: str) -> Optional[tuple[int, ...]]:
    """Version from a release tag or branch name (e.g. v1.7.0.1, releases/v1.8.0)."""
    name = ref.removeprefix("refs/tags/").removeprefix("origin/")
    name = name.removeprefix("releases/").removeprefix("hotfixes/")
    m = _REF_VERSION_RE.match(name)
    return tuple(int(p) for p in m.group(1).split(".")) if m else None


def order_warning(
    current: str, target: str, current_sha: str, target_sha: str
) -> Optional[str]:
    """Warn when N looks older than N-1, i.e. the arguments are probably swapped.

    Compares release versions when both refs carry one. Otherwise falls back to
    commit dates, since release tags live on parallel branches and ancestry
    can't order them.
    """
    if current_sha == target_sha:
        return None
    cur_v, tgt_v = _ref_version(current), _ref_version(target)
    if cur_v is not None and tgt_v is not None:
        reversed_order = cur_v < tgt_v
    else:
        try:
            cur_t = int(rac._git("log", "-1", "--format=%ct", current_sha).strip())
            tgt_t = int(rac._git("log", "-1", "--format=%ct", target_sha).strip())
        except (subprocess.CalledProcessError, ValueError):
            return None
        reversed_order = cur_t < tgt_t
    if not reversed_order:
        return None
    return (
        f"Current (N) `{current}` is older than target (N-1) `{target}`. "
        f"--current and --target may be swapped; findings describe the "
        f"wrong direction."
    )


# Worst first.
_READ_SEVERITY = [
    "API fails", "UI/API fails", "restore-indices fails", "ok, may truncate",
    "ok, stale", "ok",
]
_WRITE_SEVERITY = ["fails", DROPS_NEW_FIELD, "ok"]
_LOSS_SEVERITY = ["yes", "if out of range", "no"]


def _worst(values: list[Optional[str]], order: list[str]) -> str:
    """Most severe value. Values outside `order` ("not analysed", "unknown")
    mean the impact isn't known, so they win over any known value."""
    present = [v for v in values if v]
    unranked = [v for v in present if v not in order]
    if unranked:
        return unranked[0]
    return min(present, key=order.index) if present else order[-1]


def _set_mutator_impact(findings: list[RollbackFinding]) -> None:
    """A mutator only reshapes records into N's schema, so N-1 sees its output
    as that aspect's field changes. Use the worst of those."""
    for m in findings:
        if m.dimension != DIM_MUTATOR:
            continue
        fields = [
            f for f in findings
            if f.dimension == DIM_PDL_SCHEMA
            and (f.aspect_name == m.aspect_name or m.aspect_name in f.affected_aspects)
        ]
        if fields:
            m.read_impact = _worst([f.read_impact for f in fields], _READ_SEVERITY)
            m.write_impact = _worst([f.write_impact for f in fields], _WRITE_SEVERITY)
            m.data_loss = _worst([f.data_loss for f in fields], _LOSS_SEVERITY)
        else:
            # A mutator can rewrite values without changing the schema.
            m.read_impact = m.write_impact = m.data_loss = "unknown"
        m.detail = _mutator_detail(fields, m)


def _mutator_detail(fields: list[RollbackFinding], m: RollbackFinding) -> str:
    gate = "Only runs when ASPECT_MIGRATION_MUTATOR_ENABLED is on (off by default)."
    if not fields:
        return (
            "No schema change found for this aspect, so the impact depends on "
            "what the mutator rewrites; check its transform. If you roll back "
            "with the Option F restore, check the aspect's version history "
            f"covers records this mutator changed. {gate}"
        )
    changes = "; ".join(
        f.summary.split(" — ")[0][0].lower() + f.summary.split(" — ")[0][1:]
        for f in fields
    )
    if {m.read_impact, m.write_impact, m.data_loss} & {"not analysed", "unknown"}:
        effect = "Some of these changes couldn't be analysed; check the PR."
    elif "fails" in (m.read_impact or "") or m.write_impact == "fails":
        effect = "N-1 can't read or write the records it converts."
    elif m.write_impact == DROPS_NEW_FIELD:
        effect = "N-1 drops the new field when it saves a record."
    else:
        effect = "N-1 reads and writes them fine."
    # Why it needs attention: the ZDU rollback plan (Option F) restores
    # pre-mutation versions from aspect history, which must still hold them.
    restore = (
        "If you roll back with the Option F restore, check the aspect's "
        "version history covers records this mutator changed."
    )
    return f"Converts records to N's shape: {changes}. {effect} {restore} {gate}"


_ASPECT_CONST_RE = re.compile(r"\b([A-Z][A-Z0-9_]*_ASPECT_NAME)\b")
# Steps often write through side effects; their docs name the aspect, e.g.
# "a denormalized {@code dataProducts} aspect".
_DOC_ASPECT_RE = re.compile(r"\{@code\s+(\w+)\}\s+aspect")


def _step_aspects(step_path: str, ref: str, constants: dict[str, str]) -> set[str]:
    """Aspects an upgrade step's own files reference, by constant or in docs.
    Reads the step class plus its `<Class>*` and `Abstract*` siblings."""
    directory, filename = step_path.rsplit("/", 1)
    cls = filename[: -len(".java")]
    try:
        listing = rac._git("ls-tree", "--name-only", ref, f"{directory}/")
    except subprocess.CalledProcessError:
        return set()
    files = [
        p for p in listing.split()
        if p.endswith(".java")
        and (p.rsplit("/", 1)[-1].startswith(cls) or p.rsplit("/", 1)[-1].startswith("Abstract"))
    ]
    aspects: set[str] = set()
    for content in _read_files_at(ref, files).values():
        aspects |= {constants[c] for c in _ASPECT_CONST_RE.findall(content) if c in constants}
        aspects |= set(_DOC_ASPECT_RE.findall(content))
    return aspects


def _aspect_names_at(ref: str) -> set[str]:
    paths = [
        p for p in rac._git("ls-tree", "-r", "--name-only", ref, "--", rac.PDL_PREFIX).split()
        if p.endswith(".pdl")
    ]
    names = set()
    for content in _read_files_at(ref, paths).values():
        meta = rac.aspect_meta(content)
        if meta and meta.get("name"):
            names.add(meta["name"])
    return names


def _set_upgrade_step_impact(
    findings: list[RollbackFinding], current: str, target: str
) -> None:
    """Derive a step's impact from the aspects it touches: unknown to N-1
    breaks N-1's restore-indices; otherwise the worst of that aspect's field
    changes, or ok if it didn't change."""
    steps = [f for f in findings if f.dimension == DIM_UPGRADE_STEP]
    if not steps:
        return
    constants = rac._load_aspect_name_constants()
    n1_aspects = _aspect_names_at(target)
    for step in steps:
        # Steps record their own runs in these; not a data change.
        aspects = sorted(
            _step_aspects(step.path, current, constants)
            - {"dataHubUpgradeRequest", "dataHubUpgradeResult"}
        )
        if not aspects:
            step.detail = (
                "Couldn't tell which aspects this step writes. Verify it is "
                "safe to leave applied after rollback."
            )
            continue
        reads, writes, losses, notes = [], [], [], []
        for a in aspects:
            if a not in n1_aspects:
                reads.append("restore-indices fails")
                writes.append("fails")
                notes.append(f"`{a}` (not in N-1)")
                continue
            related = [
                f for f in findings
                if f.dimension == DIM_PDL_SCHEMA
                and (f.aspect_name == a or a in f.affected_aspects)
            ]
            reads += [f.read_impact for f in related] or ["ok"]
            writes += [f.write_impact for f in related]
            losses += [f.data_loss for f in related]
            notes.append(f"`{a}`" + (" (changed in N)" if related else ""))
        step.read_impact = _worst(reads, _READ_SEVERITY)
        step.write_impact = _worst(writes, _WRITE_SEVERITY)
        step.data_loss = _worst(losses, _LOSS_SEVERITY)
        step.detail = (
            f"Touches {', '.join(notes)}. Impact is for these aspects as a "
            f"whole (including changes the step itself doesn't make); still "
            f"verify the step's logic is safe to leave applied after rollback."
        )


def _flag_unexplained_version_gaps(findings: list[RollbackFinding]) -> None:
    """A version bump with no top-level field change means the change is in a
    nested or shared record, which this tool doesn't analyse."""
    explained: set[Optional[str]] = set()
    for f in findings:
        if f.dimension == DIM_PDL_SCHEMA:
            explained.add(f.aspect_name)
            explained.update(f.affected_aspects)
    for g in findings:
        if g.dimension != DIM_SCHEMA_VERSION or g.aspect_name in explained:
            continue
        g.risk = REQUIRES_ATTENTION
        g.read_impact = g.write_impact = g.data_loss = "not analysed"
        g.detail = (
            "Version bumped but no change found in this aspect or the records "
            "it uses (the tool doesn't parse typerefs or unions). Check the PR "
            "for what changed."
        )


def run(
    current: str, target: str
) -> tuple[list[RollbackFinding], str, str]:
    """Run all analysis dimensions. Returns (findings, current_sha, target_sha)."""
    current_sha = _resolve_sha(current)
    target_sha = _resolve_sha(target)

    findings: list[RollbackFinding] = []

    pdl_paths = rac.changed_pdls(target, current)
    read = _cached_reader()
    for path in pdl_paths:
        findings.extend(classify_pdl_for_rollback(path, current, target, read))
    findings.extend(analyze_nested_changes(current, target, pdl_paths, read))
    attribute_embedded_aspect_changes(findings, current, target, pdl_paths)

    findings.extend(classify_mutators_for_rollback(current, target))
    findings.extend(classify_upgrade_steps_for_rollback(current, target))
    findings.extend(
        analyze_schema_version_gaps(current, target, pdl_paths)
    )
    _set_mutator_impact(findings)
    _set_upgrade_step_impact(findings, current, target)
    _flag_unexplained_version_gaps(findings)

    return findings, current_sha, target_sha


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Rollback compatibility report between two DataHub releases."
        ),
    )
    parser.add_argument(
        "--current", required=True,
        help="Current build (N) — tag, branch, or SHA",
    )
    parser.add_argument(
        "--target", default=None,
        help=(
            "Target build (N-1) — tag, branch, or SHA (default: latest stable "
            "release tag — v*-cloud in acryl-fork repos, v* in OSS DataHub)"
        ),
    )
    parser.add_argument(
        "--output", default=None,
        help="Write markdown report to this file (default: stdout)",
    )
    parser.add_argument(
        "--json", dest="emit_json", action="store_true",
        help="Also emit a JSON report alongside markdown",
    )
    args = parser.parse_args(argv)

    # Same baseline rule as the PDL change report, so both tools agree on N-1.
    if args.target is None:
        args.target = rac.resolve_base()
        print(f"Resolved target (N-1): {args.target}", file=sys.stderr)
    args.current = _resolve_ref_name(args.current)
    args.target = _resolve_ref_name(args.target)

    findings, current_sha, target_sha = run(args.current, args.target)
    warning = order_warning(args.current, args.target, current_sha, target_sha)
    if warning:
        print(f"Warning: {warning}", file=sys.stderr)

    md = render_rollback_report(
        findings, args.current, args.target, current_sha, target_sha, warning
    )

    if args.output:
        Path(args.output).write_text(md, encoding="utf-8")
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(md)

    if args.emit_json:
        json_path = str(
            Path(args.output or "rollback-report.md").with_suffix(".json")
        )
        json_out = render_json_report(
            findings, args.current, args.target, current_sha, target_sha,
            warning,
        )
        Path(json_path).write_text(json_out, encoding="utf-8")
        print(f"JSON report written to {json_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
