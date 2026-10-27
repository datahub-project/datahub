"""Stage 3 of the migration framework: migrate ``(source, target)`` pairs.

This stage is completely agnostic to *how* the pairs were produced — a CLI
strategy, a programmatic caller, or an explicit user-supplied mapping all funnel
through here. For each pair it clones the source's aspects onto the target
(rewriting the entity's own references), optionally stamps the target platform
instance, repoints incoming references from other entities, and deletes the
source.
"""

import logging
from typing import Callable, Dict, Iterable, List, Optional, Set

from datahub.cli import delete_cli, migration_utils
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SystemMetadataClass
from datahub.migration.models import (
    ConflictStrategy,
    MigrationOptions,
    MigrationPair,
    MigrationReport,
)
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import transform_urns

log = logging.getLogger(__name__)


def _load_checkpoint(path: str) -> Set[str]:
    """Read already-migrated source URNs from *path* (one per line)."""
    try:
        with open(path) as f:
            return {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        return set()


def _append_checkpoint(path: str, source_urn: str) -> None:
    """Append a single source URN to the checkpoint file (created on first call)."""
    with open(path, "a") as f:
        f.write(source_urn + "\n")


def migrate_pairs(
    graph: DataHubGraph,
    pairs: Iterable[MigrationPair],
    options: MigrationOptions,
    on_pair_done: Optional[Callable[[MigrationPair], None]] = None,
) -> MigrationReport:
    """Migrate a collection of source → target pairs.

    Materializes the iterable into a list so a batch-wide URN rewriter can be
    built — cross-pair references (e.g. entity A's lineage pointing to entity B,
    where both are being migrated) are rewritten at clone time regardless of
    processing order.

    ``on_pair_done`` is called after each pair is processed (whether it succeeded
    or was skipped via ``skip_on_error``), allowing callers to drive a progress
    bar or similar UI without coupling the engine to any specific UI library.

    Errors on an individual pair abort the batch unless
    ``options.skip_on_error`` is set, in which case the pair is recorded in the
    report and the batch continues.

    **Known limitation (skip-on-error + cross-pair references):** the batch URN
    rewriter is built from *all* pairs before the loop starts. If pair A fails
    under ``skip_on_error`` and a later pair B's aspects reference A's source
    URN, that reference is still rewritten to A's (never-created) target URN.
    This is an intentional optimistic strategy: when the user retries the failed
    pairs (e.g. via ``--checkpoint-file``), A's target will be created and B's
    rewritten reference becomes correct. Removing failed pairs from the map
    would leave B pointing at A's source (which may be deleted), making retry
    harder to recover from.
    """
    pairs_list: List[MigrationPair] = list(pairs)
    urn_map: Dict[str, str] = {p.source_urn: p.target_urn for p in pairs_list}
    batch_rewrite_urn = migration_utils.make_batch_urn_rewriter(urn_map)

    checkpoint_done: Set[str] = set()
    if options.checkpoint_file:
        checkpoint_done = _load_checkpoint(options.checkpoint_file)

    report = MigrationReport(options.run_id, options.dry_run, options.keep)
    if options.report_file:
        report.open_report_file(options.report_file)
    try:
        _migrate_pairs_loop(
            pairs_list,
            graph,
            options,
            report,
            checkpoint_done,
            batch_rewrite_urn,
            on_pair_done,
        )
    finally:
        report.close_report_file()
    return report


def _migrate_pairs_loop(
    pairs_list: List[MigrationPair],
    graph: DataHubGraph,
    options: MigrationOptions,
    report: MigrationReport,
    checkpoint_done: Set[str],
    batch_rewrite_urn: Callable[[str], str],
    on_pair_done: Optional[Callable[[MigrationPair], None]],
) -> None:
    for pair in pairs_list:
        if pair.source_urn in checkpoint_done:
            log.debug(f"Checkpoint skip: {pair.source_urn}")
            report.pairs_checkpoint_skipped += 1
            if on_pair_done is not None:
                on_pair_done(pair)
            continue

        try:
            migrate_pair(graph, pair, options, report, batch_rewrite_urn)
        except Exception as e:
            if options.skip_on_error:
                log.warning(f"Error migrating {pair.source_urn}, skipping: {e}")
                report.entities_errored.append((pair.source_urn, str(e)))
            else:
                raise
        else:
            if options.checkpoint_file and not options.dry_run:
                _append_checkpoint(options.checkpoint_file, pair.source_urn)
        if on_pair_done is not None:
            on_pair_done(pair)


def migrate_pair(
    graph: DataHubGraph,
    pair: MigrationPair,
    options: MigrationOptions,
    report: MigrationReport,
    rewrite_urn: Optional[Callable[[str], str]] = None,
) -> None:
    """Migrate a single source → target pair.

    When ``rewrite_urn`` is provided (batch migration via ``migrate_pairs``), it
    rewrites cross-pair references at clone time. When omitted, falls back to a
    single-pair rewriter that only rewrites the pair's own URN.
    """
    src = pair.source_urn
    tgt = pair.target_urn

    # Reject an identity pair up front: it would clone onto itself and then hit
    # the source delete below, silently removing the entity it was meant to keep.
    if src == tgt:
        raise ValueError(
            f"Source and target URNs are identical ({src}); refusing to run a "
            f"no-op migration that would delete the source."
        )

    src_type = guess_entity_type(src)
    tgt_type = guess_entity_type(tgt)
    if src_type != tgt_type:
        raise ValueError(
            f"Cannot migrate across entity types: {src} ({src_type}) -> "
            f"{tgt} ({tgt_type}). Source and target must be the same entity type."
        )

    report.set_current_pair(src, tgt)
    log.debug(f"Will migrate {src} to {tgt}")
    # The batch rewriter (from migrate_pairs) rewrites cross-pair references
    # inside the cloned entity's own aspects. For incoming-ref repointing we
    # use a single-pair rewriter: rewriting a third entity's reference to
    # *another* pair's source is premature — if that pair later fails
    # (skip_on_error), the referrer would be left with a dangling target URN.
    self_rewrite_urn = migration_utils.make_self_urn_rewriter(src, tgt)
    if rewrite_urn is None:
        rewrite_urn = self_rewrite_urn

    target_exists = False
    if options.on_conflict is not None:
        try:
            target_exists = graph.exists(tgt)
        except Exception as e:
            # A transient failure must not be read as "target absent" — that would
            # overwrite an existing target and delete the source on a blip. Abort
            # the pair instead (skip_on_error decides whether the batch continues).
            raise RuntimeError(
                f"Could not determine whether target {tgt} exists; refusing to "
                f"migrate {src} to avoid overwriting or deleting data."
            ) from e

    if target_exists and options.on_conflict is not None:
        if options.on_conflict == ConflictStrategy.PRESERVE:
            # Adopt the existing target as-is: leave its aspects untouched, but
            # still repoint referrers and delete the source below.
            log.info(f"Target {tgt} exists — preserving existing aspects")
            report.conflicts_skipped += 1
            report.on_aspect_skipped("*")
        else:
            log.info(f"Target {tgt} exists — merging aspects")
            result = migration_utils.merge_entity(
                src,
                tgt,
                options.on_conflict,
                graph,
                options.dry_run,
                rewrite_urn=rewrite_urn,
            )
            report.aspects_merged += result.merged
            report.conflicts_skipped += result.skipped
            for aspect_name in result.merged_aspects:
                report.on_aspect_merged(aspect_name)
            for aspect_name in result.skipped_aspects:
                report.on_aspect_skipped(aspect_name)
            # Only OVERWRITE may replace an existing target's platform instance;
            # PATCH leaves whatever the target already has.
            if options.on_conflict == ConflictStrategy.OVERWRITE:
                _emit_target_instance(graph, pair, options, report)
    else:
        aspect_names = migration_utils.get_migratable_aspect_names(src_type)
        for mcp in migration_utils.clone_aspect(
            src,
            aspect_names=aspect_names,
            dst_urn=tgt,
            run_id=options.run_id,
            graph=graph,
        ):
            # Rewrite the entity's own self-references inside the aspect body
            # (e.g. fineGrainedLineages schemaField URNs), driven by the aspect's
            # relationship/Urn field markers.
            if mcp.aspect is not None:
                transform_urns(mcp.aspect, rewrite_urn)
            if pair.aspect_mutator is not None:
                pair.aspect_mutator(mcp)
            if not options.dry_run:
                graph.emit_mcp(mcp)
            report.on_entity_create(mcp.entityUrn, mcp.aspectName)  # type: ignore[arg-type]
        _emit_target_instance(graph, pair, options, report)

    # Repoint incoming references. The relationship index tells us which entities
    # reference the migrated URN (across ALL indexed relationship types, so e.g.
    # assertions are included); we then rewrite that reference wherever it appears,
    # across all of the referencing entity's aspects.
    #
    # Note: references held in non-@Relationship fields are not graph-indexed and
    # are therefore not discovered here. The concrete case is a URN *value* of a
    # structured property on another entity (stored as a primitive, not a Urn
    # field) — rewriting those would require a full aspect scan and is out of scope.
    seen_referrers = set()
    for relationship in migration_utils.get_incoming_relationships(src, graph=graph):
        referrer = relationship.urn
        if referrer in seen_referrers:
            continue
        seen_referrers.add(referrer)
        for mcp in migration_utils.rewrite_incoming_references(
            graph, referrer, self_rewrite_urn
        ):
            if not options.dry_run:
                graph.emit_mcp(mcp)
            report.on_entity_affected(mcp.entityUrn, mcp.aspectName)  # type: ignore[arg-type]

    if not options.dry_run and not options.keep:
        log.info(f"will {'hard' if options.hard else 'soft'} delete {src}")
        delete_cli._delete_one_urn(
            graph, src, soft=not options.hard, run_id=options.run_id
        )
    report.on_entity_migrated(src, "COMPLETED")


def _emit_target_instance(
    graph: DataHubGraph,
    pair: MigrationPair,
    options: MigrationOptions,
    report: MigrationReport,
) -> None:
    """Stamp the target's platform instance, if the caller supplied one.

    A platform instance cannot be derived from a target URN (it is prepended into
    the entity name), so the pair must carry it explicitly; when absent we emit
    nothing.
    """
    if pair.data_platform_instance is None:
        return
    if not options.dry_run:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=pair.target_urn,
                aspect=pair.data_platform_instance,
                systemMetadata=SystemMetadataClass(runId=options.run_id),
            )
        )
    report.on_entity_create(pair.target_urn, "dataPlatformInstance")
