"""Verdicts for `probe filter` on data-lake sources, from the same PathSpec
calls ingestion makes.

path_specs are not AllowDenyPatterns, so none of the pattern machinery in
agent/filter_check applies; a source exposes these through
probe_verdict_override. Everything here takes ingestion-equivalent specs and URIs
in their scheme -- for GCS that is s3://, because GCSSource rewrites both
before matching (see equivalent_s3_path_specs).
"""

from dataclasses import replace
from typing import Callable, List, Optional, Sequence

import parse
from wcmatch import pathlib

from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    Verdict,
    VerdictContext,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.data_lake_common.path_listing import listing_prefix
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec

TABLE_MARKER = "{table}"
RECURSIVE_GLOB = "**"
_S3_SCHEME = "s3://"

TEMPLATED_FILE_RULES_WARNING = (
    "a {table} path_spec is judged at the table folder: exclude, file_types and "
    "default_extension apply to each file inside it during ingestion, and a table "
    "whose selected partitions hold no matching file is not emitted. List its "
    "files with `probe run objects` to check them."
)
UNPARSED_TABLE_WARNING = (
    "a {table} path_spec could not read the table name out of at least one of "
    "these folders (a wildcard or placeholder before {table} matched nothing, "
    "as a bucket wildcard like `my-bucket*` does for the bucket `my-bucket`): "
    "ingestion still emits that "
    "table, but names the dataset after a file inside it, not after the folder, "
    "and applies tables_filter_pattern to each file's name as well as to the "
    "folder's, so a pattern rejecting every file name drops the table"
)
CONTAINER_WARNING = (
    "buckets and folders above a dataset (or above a folders-only path_spec's "
    "leaf folder) are emitted only as the containers of something beneath them "
    "that is itself included"
)


def _matches(path: str, glob: str) -> bool:
    """The glob test PathSpec.allowed applies: `*` does not match a leading dot."""
    return pathlib.PurePath(path).globmatch(glob, flags=pathlib.GLOBSTAR)


def _reaches(path: str, glob: str) -> bool:
    """Whether a spec's listing walks to `path`. Unlike _matches it lets `*`
    match a leading dot, because list_folders_path returns hidden folders too
    and only the rules applied afterwards drop them."""
    return pathlib.PurePath(path).globmatch(
        glob, flags=pathlib.GLOBSTAR | pathlib.DOTGLOB
    )


def _table_depth(spec: PathSpec) -> int:
    # The same count extract_table_name_and_path uses to cut the table path.
    return spec.include.count("/", 0, spec.include.find(TABLE_MARKER))


def _container_globs(glob_parts: Sequence[str], depth: int) -> List[str]:
    """The glob prefixes a folder at `depth` (its count of "/") must reach to
    sit above a dataset of a non-table spec; empty when none can lie below it.

    A `**` matches any number of folders, so every folder at or below it can
    hold a matching file, however deep.
    """
    if RECURSIVE_GLOB in glob_parts:
        star = glob_parts.index(RECURSIVE_GLOB)
        if depth >= star:
            return ["/".join(glob_parts[: star + 1])]
    if depth >= len(glob_parts) - 1:
        return []
    return ["/".join(glob_parts[: depth + 1])]


def _table_name_parses(spec: PathSpec, folder: str) -> bool:
    """Whether the include, cut after {table}, parses `folder`. Ingestion names
    a table from parse() over a file in it, and `parse`'s fields match one
    character at least, so when this fails that parse fails too and
    extract_table_name_and_path falls back to the file's own path."""
    parsable = PathSpec.get_parsable_include(spec.include)
    head = parsable[: parsable.find(TABLE_MARKER) + len(TABLE_MARKER)]
    return parse.parse(head, folder) is not None


def _dataset_reason(
    spec: PathSpec, uri: str, warn: Callable[[str], None], ignore_ext: bool
) -> Optional[str]:
    """None: this spec includes uri. "": it does not reach uri. Else the field."""
    if spec.emit_folders_only:
        return ""
    if TABLE_MARKER in spec.include:
        folder = uri.rstrip("/")
        depth = _table_depth(spec)
        folder_glob = "/".join(spec.glob_include.split("/")[: depth + 1])
        if folder.count("/") != depth or not _reaches(folder, folder_glob):
            return ""
        warn(TEMPLATED_FILE_RULES_WARNING)
        # Every file under a hidden folder fails allowed()'s is_path_hidden
        # check, so the table can never be emitted.
        if spec.is_path_hidden(folder) and not spec.include_hidden_folders:
            return "include_hidden_folders"
        # With include_hidden_folders on, a dot-folder still fails allowed()'s
        # include glob for every file under it.
        if not _matches(folder, folder_glob):
            return "include"
        # _process_templated_path names the table from the folder path without a
        # trailing slash; same call, same input.
        table_name, _ = spec.extract_table_name_and_path(folder)
        if not spec.tables_filter_pattern.allowed(table_name):
            return "tables_filter_pattern"
        if not _table_name_parses(spec, folder):
            warn(UNPARSED_TABLE_WARNING)
        return None
    dirname, startswith = listing_prefix(spec.include)
    if not uri.startswith(dirname + startswith):
        return ""
    # S3Source.get_workunits_internal passes
    # ignore_ext=is_s3_platform() and use_s3_content_type; GCS never sets it.
    return spec.rejection_reason(uri, ignore_ext=ignore_ext)


def _first_claim(
    path_specs: Sequence[PathSpec],
    uri: str,
    reason_for: Callable[[PathSpec], Optional[str]],
) -> Verdict:
    claimed: Optional[str] = None
    for i, spec in enumerate(path_specs):
        reason = reason_for(spec)
        if reason is None:
            # include() carries no matched_target; the URI is still the
            # target the probe reports.
            return Verdict(included=True, matched_target=uri)
        if reason and claimed is None:
            claimed = f"path_specs[{i}].{reason}"
    return Verdict.exclude(claimed or "path_specs", matched_target=uri)


def judge_dataset(
    path_specs: Sequence[PathSpec],
    uri: str,
    warn: Callable[[str], None],
    *,
    ignore_ext: bool = False,
) -> Verdict:
    # The templated branch ignores ignore_ext: it decides at the folder, and the
    # per-file extension check it does not judge is what
    # TEMPLATED_FILE_RULES_WARNING says.
    return _first_claim(
        path_specs, uri, lambda s: _dataset_reason(s, uri, warn, ignore_ext)
    )


def judge_folder(
    path_specs: Sequence[PathSpec],
    uri: str,
    warn: Callable[[str], None],
    *,
    bucket_kind: Optional[str] = None,
    display_uri: Optional[str] = None,
) -> Verdict:
    """`bucket_kind` names the kind a bucket is judged as, for the error that
    tells a caller who passed a bare bucket URI where to ask instead.
    `display_uri` is the name that error quotes, when the caller wrote it in
    another scheme than the s3:// one judged here."""
    folder = uri.rstrip("/")
    # "s3://my-bucket" has two slashes: it is a bucket container, never a folder.
    if folder.count("/") <= 2:
        where = f'--kind "{bucket_kind}"' if bucket_kind else "the bucket kind"
        raise ProbeArgumentError(
            f"'{display_uri or uri}' is a bucket, not a folder; judge its bare "
            f"name with {where}"
        )

    def reason(spec: PathSpec) -> Optional[str]:
        glob_parts = spec.glob_include.rstrip("/").split("/")
        if spec.emit_folders_only:
            depth = folder.count("/")
            leaf = len(glob_parts) - 1
            if depth < leaf:
                # create_folder_containers emits every folder above an allowed
                # leaf. Under a hidden ancestor every leaf is hidden too, so
                # that one never appears; exclude globs match whole leaf paths
                # and say nothing about an ancestor.
                if not _reaches(folder, "/".join(glob_parts[: depth + 1])):
                    return ""
                if spec.is_path_hidden(folder) and not spec.include_hidden_folders:
                    return "include_hidden_folders"
                warn(CONTAINER_WARNING)
                return None
            # _process_folders applies folder_allowed only, no include glob.
            if not _reaches(folder, "/".join(glob_parts)):
                return ""
            return spec.folder_rejection_reason(folder)
        # A folder strictly above the dataset level is a container of datasets.
        depth = folder.count("/")
        if TABLE_MARKER in spec.include:
            globs = (
                ["/".join(glob_parts[: depth + 1])]
                if depth < _table_depth(spec)
                else []
            )
        else:
            globs = _container_globs(glob_parts, depth)
        reached = [glob for glob in globs if _reaches(folder, glob)]
        if not reached:
            return ""
        # Every dataset beneath it fails the same two checks _dataset_reason
        # applies, so ingestion never emits it as their container.
        if spec.is_path_hidden(folder) and not spec.include_hidden_folders:
            return "include_hidden_folders"
        if not any(_matches(folder, glob) for glob in reached):
            return "include"
        warn(CONTAINER_WARNING)
        return None

    return _first_claim(path_specs, folder, reason)


def judge_bucket(
    path_specs: Sequence[PathSpec], bucket: str, warn: Callable[[str], None]
) -> Verdict:
    def reason(spec: PathSpec) -> Optional[str]:
        if not _reaches(bucket, spec.glob_include.split("/")[2]):
            return ""
        warn(CONTAINER_WARNING)
        return None

    return _first_claim(path_specs, bucket, reason)


def judge_path_spec_name(
    path_specs: Sequence[PathSpec],
    ctx: VerdictContext,
    *,
    scheme: str,
    bucket_kind: str,
    ignore_ext: bool = False,
) -> Verdict:
    """One `probe filter` name against ingestion-equivalent s3:// specs, for a
    source whose names are written in `scheme` (s3:// or gs://). The judges
    see s3:// URIs, as S3Source matches them; the error text and the reported
    target keep the scheme the caller wrote."""
    kind = str(ctx.kind)
    if kind == bucket_kind:
        if "/" in ctx.name:
            raise ProbeArgumentError(
                f"'{ctx.name}' is not a bucket name; pass bare names, as "
                f"`probe run buckets` lists them"
            )
        return judge_bucket(path_specs, ctx.name, ctx.warn)
    if not ctx.name.startswith(scheme):
        raise ProbeArgumentError(
            f"'{ctx.name}' is not a {scheme} URI; {kind} names are full "
            f"{scheme} URIs, as `probe run datasets` lists them"
        )
    s3_uri = _S3_SCHEME + ctx.name[len(scheme) :]
    if kind == str(DatasetContainerSubTypes.FOLDER):
        verdict = judge_folder(
            path_specs,
            s3_uri,
            ctx.warn,
            bucket_kind=bucket_kind,
            display_uri=ctx.name,
        )
    else:
        verdict = judge_dataset(path_specs, s3_uri, ctx.warn, ignore_ext=ignore_ext)
    judged = verdict.matched_target or s3_uri
    return replace(verdict, matched_target=scheme + judged[len(_S3_SCHEME) :])
