from pathlib import PurePosixPath
from typing import Callable, Dict, Iterable, List, Optional, Set

from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.ingestion.source.github_documents.okf_frontmatter import parse_frontmatter
from datahub.metadata.schema_classes import OkfBundleClass

RESERVED_FILENAMES = {"index.md", "log.md"}


def enclosing_bundle(
    path: str, bundles: Dict[str, OkfBundleClass]
) -> Optional[OkfBundleClass]:
    roots = [root for root in bundles if not root or path.startswith(root + "/")]
    return bundles[max(roots, key=len)] if roots else None


def concept_id(path: str, bundle: OkfBundleClass) -> str:
    return (
        PurePosixPath(path)
        .relative_to(bundle.rootPath or ".")
        .with_suffix("")
        .as_posix()
    )


def discover_bundles(
    *,
    repo: str,
    file_paths: Iterable[str],
    index_paths: Set[str],
    explicit_roots: List[str],
    fetch: Callable[[str], Optional[str]],
    report: GitHubDocumentsSourceReport,
) -> Dict[str, OkfBundleClass]:
    bundles = {
        root: OkfBundleClass(repo=repo, rootPath=root) for root in explicit_roots
    }
    # Ancestor indexes can sit outside path_prefix or the max_files slice.
    # The recursive tree provides their existence without another tree request.
    candidates = {
        (parent / "index.md").as_posix()
        for path in file_paths
        for parent in PurePosixPath(path).parents
    } & index_paths
    for path in sorted(candidates):
        content = fetch(path)
        if content is None:
            report.warning(
                title="Could not read OKF bundle index",
                message="Bundle discovery could not inspect this index.md.",
                context=f"path={path}",
            )
            continue
        frontmatter = parse_frontmatter(content, path=path, report=report)
        if frontmatter is None or "okf_version" not in frontmatter.values:
            continue
        parent = PurePosixPath(path).parent
        root = "" if str(parent) == "." else str(parent)
        version = frontmatter.values["okf_version"]
        bundles[root] = OkfBundleClass(
            repo=repo,
            rootPath=root,
            okfVersion=version if isinstance(version, str) else None,
        )
        if not isinstance(version, str):
            report.okf_invalid_values += 1
            report.warning(
                title="Invalid OKF bundle version",
                message="The bundle is recognized, but okf_version is not a string.",
                context=f"path={path}",
            )
    return bundles
