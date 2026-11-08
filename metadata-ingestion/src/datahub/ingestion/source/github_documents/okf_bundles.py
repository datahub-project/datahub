from pathlib import PurePosixPath
from typing import Callable, Dict, Iterable, List, Optional, Set

from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.ingestion.source.github_documents.okf_frontmatter import parse_frontmatter
from datahub.metadata.schema_classes import OkfBundleClass

RESERVED_FILENAMES = {"index.md", "log.md"}
# Stored for a bundle at the location's root: an empty string cannot be filtered on in search.
ROOT_PATH = "."


def _key(root: str) -> str:
    root = root.strip("/")
    return "" if root in ("", ROOT_PATH) else root


def _bundle(location: str, key: str, version: Optional[str] = None) -> OkfBundleClass:
    return OkfBundleClass(
        location=location, rootPath=key or ROOT_PATH, okfVersion=version
    )


def bundle_index_path(bundle: OkfBundleClass) -> str:
    key = _key(bundle.rootPath)
    return f"{key}/index.md" if key else "index.md"


def enclosing_bundle(
    path: str, bundles: Dict[str, OkfBundleClass]
) -> Optional[OkfBundleClass]:
    roots = [root for root in bundles if not root or path.startswith(root + "/")]
    return bundles[max(roots, key=len)] if roots else None


def concept_id(path: str, bundle: OkfBundleClass) -> str:
    return (
        PurePosixPath(path)
        .relative_to(_key(bundle.rootPath) or ".")
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
    location = f"https://github.com/{repo}"
    bundles = {_key(root): _bundle(location, _key(root)) for root in explicit_roots}
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
                message=(
                    "Bundle discovery could not inspect this index.md; files "
                    "under its directory are skipped this run."
                ),
                context=f"path={path}",
            )
            continue
        frontmatter = parse_frontmatter(content, path=path, report=report)
        if frontmatter is None:
            continue
        if "okf_version" not in frontmatter.values:
            report.okf_unversioned_index_files += 1
            report.info(
                title="Unversioned OKF bundle candidate",
                message=(
                    "This index.md has frontmatter without okf_version. "
                    "Declare its directory in okf_bundle_roots to import it as a bundle."
                ),
                context=f"path={path}",
            )
            continue
        parent = PurePosixPath(path).parent
        root = _key(str(parent))
        version = frontmatter.values["okf_version"]
        bundles[root] = _bundle(
            location, root, version if isinstance(version, str) else None
        )
        if not isinstance(version, str):
            report.okf_invalid_values += 1
            report.warning(
                title="Invalid OKF bundle version",
                message="The bundle is recognized, but okf_version is not a string.",
                context=f"path={path}",
            )
    return bundles
