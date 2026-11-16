import json
from dataclasses import dataclass
from typing import Dict, Optional

import yaml

from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
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


def parse_frontmatter(
    content: str, *, path: str, report: GitHubDocumentsSourceReport
) -> Optional[OkfFrontmatter]:
    lines = content.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\r\n") != FRONTMATTER_DELIMITER:
        return None

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
            return None
        _validate_json_keys(values)
        # Validate JSON compatibility without storing a second copy of the YAML.
        json.dumps(values, ensure_ascii=False, allow_nan=False)
        return OkfFrontmatter(
            values=values,
            raw="".join(lines[: end + 1]),
            body="".join(lines[end + 1 :]),
        )
    except (
        StopIteration,
        yaml.YAMLError,
        ValueError,
        TypeError,
        RecursionError,
    ) as exc:
        report.okf_parse_errors += 1
        report.warning(
            title="Invalid OKF frontmatter",
            message=(
                "Imported the original file because its frontmatter could not "
                "be parsed safely as JSON-compatible YAML (64 KiB maximum; "
                "anchors and aliases are not supported)."
            ),
            context=f"path={path}",
            exc=exc,
        )
        return None


def parse_okf_frontmatter(
    content: str, *, path: str, report: GitHubDocumentsSourceReport
) -> Optional[OkfFrontmatter]:
    frontmatter = parse_frontmatter(content, path=path, report=report)
    return frontmatter if frontmatter and "type" in frontmatter.values else None
