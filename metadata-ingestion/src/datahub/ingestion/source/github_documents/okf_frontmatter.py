import json
from dataclasses import dataclass
from typing import Dict, Optional

import yaml

from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)

FRONTMATTER_DELIMITER = "---"


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
    json: str
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
        end = next(
            index
            for index in range(1, len(lines))
            if lines[index].rstrip("\r\n") == FRONTMATTER_DELIMITER
        )
        values: object = yaml.load("".join(lines[1:end]), Loader=_FrontmatterLoader)
        if not isinstance(values, dict):
            return None
        _validate_json_keys(values)
        serialized = json.dumps(values, ensure_ascii=False, allow_nan=False)
        return OkfFrontmatter(
            values=values,
            json=serialized,
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
                "be parsed losslessly as JSON."
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
