import json
from dataclasses import dataclass
from typing import Dict, Optional

import yaml

from datahub.ingestion.source.repository_documents.report import (
    RepositoryDocumentsSourceReport,
)

FRONTMATTER_DELIMITER = "---"
MAX_FRONTMATTER_BYTES = 64 * 1024


class _FrontmatterLoader(yaml.SafeLoader):
    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> Dict[object, object]:
        self.flatten_mapping(node)
        result: Dict[object, object] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in result:
                raise ValueError("Duplicate frontmatter key")
            result[key] = self.construct_object(value_node, deep=deep)
        return result


# Dates belong in the JSON as strings rather than Python datetime objects.
_FrontmatterLoader.yaml_implicit_resolvers = {
    key: [
        (tag, regex) for tag, regex in resolvers if tag != "tag:yaml.org,2002:timestamp"
    ]
    for key, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}


@dataclass(frozen=True)
class OkfFrontmatter:
    values: Dict[str, object]
    raw: str
    body: str


def _validate_json_keys(value: object) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("Frontmatter JSON requires string keys")
            _validate_json_keys(child)
    elif isinstance(value, list):
        for child in value:
            _validate_json_keys(child)


@dataclass(frozen=True)
class FrontmatterRead:
    """The outcome of reading a file's frontmatter, without reporting it.

    ``frontmatter`` is set when it was read; otherwise ``rejection`` says why
    not. ``error`` is set when the file has a frontmatter block that could not
    be parsed safely, as opposed to having none or not being a mapping.
    """

    frontmatter: Optional[OkfFrontmatter]
    rejection: Optional[str] = None
    error: Optional[Exception] = None


def is_okf_concept(frontmatter: Optional[OkfFrontmatter]) -> bool:
    """Whether the frontmatter declares an OKF concept: a non-empty string ``type``."""
    if frontmatter is None:
        return False
    concept_type = frontmatter.values.get("type")
    return isinstance(concept_type, str) and bool(concept_type.strip())


def read_frontmatter(content: str) -> FrontmatterRead:
    lines = content.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\r\n") != FRONTMATTER_DELIMITER:
        return FrontmatterRead(None, rejection="the file has no frontmatter")

    try:
        block_size = len(lines[0].encode("utf-8"))
        for end in range(1, len(lines)):
            block_size += len(lines[end].encode("utf-8"))
            if block_size > MAX_FRONTMATTER_BYTES:
                raise ValueError("Frontmatter exceeds 64 KiB")
            if lines[end].rstrip("\r\n") == FRONTMATTER_DELIMITER:
                break
        else:
            raise ValueError("Frontmatter has no closing delimiter")
        yaml_text = "".join(lines[1:end])
        # Reject references before construction can expand or traverse an alias graph.
        for event in yaml.parse(yaml_text, Loader=_FrontmatterLoader):
            if isinstance(event, yaml.AliasEvent) or getattr(event, "anchor", None):
                raise ValueError("Frontmatter anchors and aliases are not supported")
        values: object = yaml.load(yaml_text, Loader=_FrontmatterLoader)
        if not isinstance(values, dict):
            return FrontmatterRead(
                None, rejection="the frontmatter is not a YAML mapping"
            )
        _validate_json_keys(values)
        # Validate JSON compatibility without storing a second copy of the YAML.
        json.dumps(values, ensure_ascii=False, allow_nan=False)
        return FrontmatterRead(
            OkfFrontmatter(
                values=values,
                raw="".join(lines[: end + 1]),
                body="".join(lines[end + 1 :]),
            )
        )
    except (
        StopIteration,
        yaml.YAMLError,
        ValueError,
        TypeError,
        RecursionError,
    ) as exc:
        return FrontmatterRead(None, rejection=str(exc), error=exc)


def read_okf_frontmatter(content: str) -> FrontmatterRead:
    """Read frontmatter that declares an OKF concept, or say why it does not."""
    result = read_frontmatter(content)
    if result.frontmatter is None or is_okf_concept(result.frontmatter):
        return result
    return FrontmatterRead(
        None, rejection="the frontmatter has no non-empty string `type`"
    )


def parse_frontmatter(
    content: str, *, path: str, report: RepositoryDocumentsSourceReport
) -> Optional[OkfFrontmatter]:
    result = read_frontmatter(content)
    if result.error is not None:
        report.okf_parse_errors += 1
        report.warning(
            title="Invalid OKF frontmatter",
            message=(
                "Imported the original file because its frontmatter could not "
                "be parsed safely as JSON-compatible YAML (64 KiB maximum; "
                "anchors and aliases are not supported)."
            ),
            context=f"path={path}",
            exc=result.error,
        )
    return result.frontmatter


def parse_okf_frontmatter(
    content: str, *, path: str, report: RepositoryDocumentsSourceReport
) -> Optional[OkfFrontmatter]:
    frontmatter = parse_frontmatter(content, path=path, report=report)
    if is_okf_concept(frontmatter):
        return frontmatter
    if frontmatter is not None and "type" in frontmatter.values:
        report.okf_unresolved_values += 1
        report.warning(
            title="Unresolved OKF value",
            message=(
                "The file has no usable type, so it is imported as a plain "
                "document, not an OKF concept."
            ),
            context=f"path={path}, key=type",
        )
    return None
