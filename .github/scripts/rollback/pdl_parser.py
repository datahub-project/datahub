"""PDL parsing: types, defaults, enums, typerefs/unions, effective fields and includes."""

from __future__ import annotations

import re
from typing import Optional

import bump_schema_versions as bsv
import report_aspect_changes as rac

from rollback import repo


_INLINE_ENUM_RE = re.compile(r"^enum\s+(\w+)\s*\{[^{}]*\}$", re.DOTALL)


# Annotations that shape the search index mapping.
_MAPPING_ANNOTATIONS = ("Searchable", "SearchableRef")


def normalized_annotation(value: object) -> Optional[str]:
    """Annotation value with whitespace and trailing commas removed."""
    if value is None:
        return None
    return re.sub(r",(?=[}\]])", "", re.sub(r"\s+", "", str(value)))


def mapping_annotations(annotations: dict) -> dict[str, str]:
    """Mapping-affecting annotations, with whitespace and trailing commas
    normalised so formatting-only edits don't count as changes."""
    out = {}
    for key in _MAPPING_ANNOTATIONS:
        if key in annotations:
            out[key] = re.sub(
                r",(?=[}\]])", "", re.sub(r"\s+", "", str(annotations[key]))
            )
    return out


_ENUM_BLOCK_RE = re.compile(r"\benum\s+(\w+)\s*\{((?:[^{}]|\{[^{}]*\})*)\}")
# An annotation with an optional value: "...", {...} or a bare token.
ANNOTATION_RE = re.compile(r'@[\w.]+(?:\s*=\s*(?:"(?:[^"\\]|\\.)*"|\{[^{}]*\}|\S+))?')
_SYMBOL_RE = re.compile(r"[A-Za-z_]\w*")


def enum_symbols(pdl: str) -> dict[str, list[str]]:
    """Enum symbols per enum, ignoring comments, commas and annotations such
    as `@deprecated = "..."` (which `rac.enums` splits into fake symbols)."""
    cleaned = repo.strip_comments(pdl)
    out: dict[str, list[str]] = {}
    for m in _ENUM_BLOCK_RE.finditer(cleaned):
        body = ANNOTATION_RE.sub(" ", m.group(2))
        out[m.group(1)] = _SYMBOL_RE.findall(body)
    return out


def record_top_level(pdl: str, record: str) -> Optional[str]:
    """`record`'s body with comments removed and nested blocks blanked out,
    so only its own field declarations remain, one per line."""
    text = repo.strip_comments(pdl)
    m = re.search(rf"\brecord\s+{re.escape(record)}\b[^{{]*\{{", text)
    if not m:
        return None
    end = repo.skip_balanced(text, m.end() - 1)
    if end is None:
        return None
    body = text[m.end() : end - 1]
    # Nested text (including its newlines) becomes spaces, so a default
    # written after an inline record or map stays on the field's line.
    return "".join(ch if depth == 0 else " " for _, ch, depth in top_level_chars(body))


def field_has_default(pdl: str, field_name: str, record: Optional[str]) -> bool:
    """True if `record`'s own declaration of the field assigns a default.

    Read from the source because the field parser keeps some defaults in the
    type text (enums) and drops others (numbers). Scoped to the record so a
    same-named field elsewhere in the file doesn't count.
    """
    if not record:
        return False
    body = record_top_level(pdl, record)
    if body is None:
        return False
    pattern = rf"^\s*{re.escape(field_name)}\s*:[^\n]*="
    return re.search(pattern, body, re.MULTILINE) is not None


def top_level_chars(text: str):
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


def strip_top_level_default(type_text: str) -> str:
    """Type text without the field's own `= default`. An `=` inside an inline
    record, union or map belongs to that nested type and is kept."""
    for i, ch, depth in top_level_chars(type_text):
        if ch == "=" and depth == 0:
            return type_text[:i].strip()
    return type_text.strip()


def comparable_type(type_text: str) -> str:
    """Field type without its default, with an inline enum reduced to its name.

    A default change or moving an enum into its own file doesn't change the
    stored type; enum symbol changes are reported separately.
    """
    t = strip_top_level_default(type_text)
    m = _INLINE_ENUM_RE.match(t)
    return m.group(1) if m else t


def record_fields(rdef: dict) -> dict[str, dict]:
    """`bsv.parse_top_level_defs` fields in the shape `rac.fields` returns."""
    return {
        name: {
            "optional": opt,
            "type": re.sub(r"^optional\s+", "", typ, count=1),
            "annotations": ann,
        }
        for name, (typ, opt, ann) in rdef["fields"].items()
    }


def path_of_fqn(fqn: str) -> str:
    return f"{rac.PDL_PREFIX}/{fqn.replace('.', '/')}.pdl"


def fqn_of_path(path: str) -> str:
    return path[len(rac.PDL_PREFIX) + 1 : -len(".pdl")].replace("/", ".")


def main_record(content: str) -> Optional[dict]:
    # bsv gives up on files with typeref/fixed; read the records without them.
    defs = bsv.parse_top_level_defs(split_typerefs(content)[2]) if content else None
    name = rac.record_name(content) if content else None
    rdef = (defs or {}).get(name or "")
    return rdef if rdef and rdef["kind"] == "record" else None


def via_note(field: dict) -> str:
    return f" (via includes `{field['via']}`)" if field.get("via") else ""


def effective_fields(
    content: str,
    ref: str,
    record: Optional[str] = None,
    rdef: Optional[dict] = None,
    _seen: Optional[set[str]] = None,
    read: Optional[repo.Reader] = None,
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
        rdef = main_record(content)
    own = record_fields(rdef) if rdef else rac.fields(content)
    seen = _seen if _seen is not None else set()
    namespace, imports = bsv.parse_pdl_header(content)
    out: dict[str, dict] = {}
    for short in sorted((rdef or {}).get("includes", set())):
        fqn = imports.get(short) or f"{namespace}.{short}"
        if fqn in seen:
            continue
        seen.add(fqn)
        inc_content = read(ref, path_of_fqn(fqn))
        for name, f in effective_fields(
            inc_content, ref, _seen=seen, read=read
        ).items():
            out.setdefault(name, {**f, "via": f.get("via") or short})
    for name, f in own.items():
        out[name] = {
            **f,
            "has_default": field_has_default(content, name, record),
            "via": None,
        }
    return out


# Declarations start a line; anchoring keeps text inside string literals
# (e.g. an annotation value "typeref X = long") from matching.
_TYPEREF_RE = re.compile(r"(?m)^[ \t]*typeref\s+(\w+)\s*=\s*")
_FIXED_RE = re.compile(r"(?m)^[ \t]*fixed\s+(\w+)\s+(\d+)")
_TYPE_NAME_RE = re.compile(r"[\w.]+")


def type_expr_end(text: str, i: int) -> int:
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
        end = repo.skip_balanced(text, k)
        return end if end is not None else len(text)
    return j


def split_typerefs(pdl: str) -> tuple[dict[str, str], dict[str, int], str]:
    """Typerefs {name: type}, fixed types {name: size}, and the rest of the
    file (comments removed) without them, so `bsv.parse_top_level_defs`,
    which gives up on typeref/fixed, can still read the file's records."""
    text = bsv.strip_pdl_comments(pdl)
    typerefs: dict[str, str] = {}
    spans: list[tuple[int, int]] = []
    for m in _TYPEREF_RE.finditer(text):
        end = type_expr_end(text, m.end())
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


def union_members(type_text: str) -> Optional[set[str]]:
    """Members of `union[...]` as normalised "alias: type" strings; None if it
    isn't a union. PDL commas are optional, so members are read one by one:
    an optional `alias:`, then a type name (with `[...]`) or an inline
    record/enum definition."""
    t = type_text.strip()
    if not (t.startswith("union") and t.endswith("]") and "[" in t):
        return None
    inner = ANNOTATION_RE.sub(" ", t[t.index("[") + 1 : -1])
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
            end = repo.skip_balanced(inner, inline.end())
            i = end if end is not None else len(inner)
        else:
            nxt = type_expr_end(inner, i)
            i = nxt if nxt > i else i + 1
        members.add(" ".join(inner[start:i].split()))
    return members


def include_closure(
    content: str, ref: str, seen: set[str], read: repo.Reader
) -> set[str]:
    """FQNs of every record `content`'s main record includes, transitively."""
    rdef = main_record(content)
    if not rdef:
        return seen
    namespace, imports = bsv.parse_pdl_header(content)
    for short in rdef["includes"]:
        fqn = imports.get(short) or f"{namespace}.{short}"
        if fqn not in seen:
            seen.add(fqn)
            include_closure(read(ref, path_of_fqn(fqn)), ref, seen, read)
    return seen
