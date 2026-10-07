package com.linkedin.metadata.entity;

import static com.linkedin.metadata.search.utils.QueryUtils.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.file.DataHubFileInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.CascadeOperationContext;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * The resumable, references-first delete cascade behind {@link
 * DeleteEntityService#removeReferencesResumable}: every reference to an entity is removed while its
 * graph edges still exist, before the entity's own rows are deleted.
 *
 * <p>It lives outside {@code DeleteEntityService} so that the file shared by OSS and the fork gets
 * only a small delegating hunk. What only {@code DeleteEntityService} knows (helpers whose bodies
 * differ between the repos, and the fork-only subscriptions phase) comes in through {@link Host}.
 * One instance serves one call.
 *
 * <p>The first referrer that cannot be cleaned stops the cascade with an exception. Every step is
 * idempotent and every referrer write is conditional on the version read just before it, so running
 * the cascade again finishes the work.
 */
@Slf4j
final class DeleteCascadeEngine {

  /** The pieces of the cascade that stay in {@code DeleteEntityService}. */
  interface Host {
    @Nonnull
    Map<String, AspectSpec> aspectSpecsReferringTo(
        @Nonnull String relatedEntityType,
        @Nonnull String relationshipType,
        @Nonnull EntitySpec entitySpec);

    boolean shouldDeleteAssetReferencingUrn(@Nonnull Urn assetUrn, @Nonnull Urn deletedUrn);

    @Nonnull
    List<String> aspectsToUpdate(@Nonnull Urn deletedUrn, @Nonnull Urn assetUrn);

    @Nullable
    MetadataChangeProposal updateAspectForSearchReference(
        @Nonnull OperationContext ctx,
        @Nonnull Urn assetUrn,
        @Nonnull Urn deletedUrn,
        @Nonnull String aspectName);

    @Nonnull
    AuditStamp createAuditStamp();

    void deleteStorageObject(@Nonnull Urn fileUrn, @Nonnull DataHubFileInfo fileInfo);

    /** Repo-specific phases, run after the shared ones (the fork adds subscriptions). */
    @Nonnull
    default List<String> extraPhases() {
      return List.of();
    }

    /** Runs one of {@link #extraPhases()}; called for every phase the engine does not know. */
    default void runExtraPhase(
        @Nonnull DeleteCascadeEngine engine,
        @Nonnull OperationContext ctx,
        @Nonnull Urn urn,
        @Nonnull String phase,
        @Nonnull DeleteCascadeListener listener) {
      throw new IllegalStateException("Unknown delete cascade phase " + phase);
    }
  }

  /** An entity-search scan: the entity types to search and the filter that finds referrers. */
  record SearchScan(@Nonnull List<String> entityNames, @Nonnull Filter filter) {}

  private static final String SCROLL_KEEP_ALIVE = "5m";

  /** Page size of every cascade scan. */
  static final int PAGE_SIZE = 250;

  /**
   * search_after needs a total order that holds across a fresh point in time. The default (_score,
   * urn) does not, so cascade scans sort by urn alone.
   */
  private static final List<SortCriterion> URN_SORT =
      List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING));

  private final EntityService<?> _entityService;
  private final GraphService _graphService;
  private final EntitySearchService _searchService;
  @Nullable private final MetricUtils _metricUtils;
  private final Host host;
  private final List<String> cascadePhases;

  // Per call: one instance serves one removeReferencesResumable call.
  private CascadeOperationContext cascade;
  private int referencesRemoved;

  DeleteCascadeEngine(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final GraphService graphService,
      @Nonnull final EntitySearchService searchService,
      @Nullable final MetricUtils metricUtils,
      @Nonnull final Host host) {
    this._entityService = Objects.requireNonNull(entityService, "entityService");
    this._graphService = Objects.requireNonNull(graphService, "graphService");
    this._searchService = Objects.requireNonNull(searchService, "searchService");
    this._metricUtils = metricUtils;
    this.host = Objects.requireNonNull(host, "host");
    this.cascadePhases = cascadePhases(host);
  }

  /** Shared phases in order, then the host's repo-specific ones (the fork's subscriptions). */
  private static List<String> cascadePhases(@Nonnull final Host host) {
    final List<String> phases = new ArrayList<>();
    phases.add(DeleteCascadeCheckpoint.PHASE_GRAPH);
    phases.add(DeleteCascadeCheckpoint.PHASE_SEARCH_REFERENCES);
    phases.add(DeleteCascadeCheckpoint.PHASE_FILES);
    phases.addAll(host.extraPhases());
    return List.copyOf(phases);
  }

  /**
   * Removes every reference to {@code urn} while its graph edges still exist, phase by phase.
   *
   * <p>Scans and reads include soft-deleted documents: the entity being hard-deleted may already be
   * soft-deleted (by a user, or because it is a garbage-collection target), and with graph status
   * filtering on, a default query hides its edges. Soft-deleted referrers still hold their
   * references. {@link DeleteEntityService#deleteReferencesTo} is unchanged.
   *
   * @param resumeFrom the phase an earlier call reached; the phases before it are skipped. Null
   *     starts from the first phase.
   * @param listener called before each page; its exceptions propagate unchanged
   * @return how many referrers had a reference removed
   */
  int removeReferencesResumable(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nullable final DeleteCascadeCheckpoint resumeFrom,
      @Nonnull final DeleteCascadeListener listener) {
    final OperationContext ctx =
        opContext.withSearchFlags(flags -> flags.setIncludeSoftDeleted(true));
    try (CascadeOperationContext operation =
        CascadeOperationContext.begin(_metricUtils, "removeReferencesResumable", urn, -1)) {
      cascade = operation;
      for (int i = startPhaseIndex(urn, resumeFrom); i < cascadePhases.size(); i++) {
        runPhase(ctx, urn, cascadePhases.get(i), listener);
      }
    }
    log.info("Reference cleanup for {}: removed={}", urn, referencesRemoved);
    return referencesRemoved;
  }

  private int startPhaseIndex(
      @Nonnull final Urn urn, @Nullable final DeleteCascadeCheckpoint resumeFrom) {
    if (resumeFrom == null) {
      return 0;
    }
    final int index = cascadePhases.indexOf(resumeFrom.phase());
    if (index < 0) {
      // Every step is idempotent, so restarting is always correct.
      log.warn(
          "Ignoring delete cascade checkpoint with unknown phase {} for {}; restarting",
          resumeFrom.phase(),
          urn);
      return 0;
    }
    return index;
  }

  private void runPhase(
      @Nonnull OperationContext ctx,
      @Nonnull final Urn urn,
      @Nonnull final String phase,
      @Nonnull final DeleteCascadeListener listener) {
    switch (phase) {
      case DeleteCascadeCheckpoint.PHASE_GRAPH -> runGraphPhase(ctx, urn, listener);
      case DeleteCascadeCheckpoint.PHASE_SEARCH_REFERENCES ->
          runSearchPhase(
              ctx,
              searchReferenceScan(urn),
              phase,
              listener,
              asset -> removeSearchReference(ctx, urn, asset));
      case DeleteCascadeCheckpoint.PHASE_FILES ->
          runSearchPhase(ctx, fileScan(urn), phase, listener, file -> removeFileReference(ctx, file));
      default -> host.runExtraPhase(this, ctx, urn, phase, listener);
    }
  }

  /** Counts one removed reference; the host's repo-specific phases call it too. */
  void referenceRemoved() {
    referencesRemoved++;
    cascade.recordEntityProcessed();
  }

  // ---------------------------------------------------------------- graph phase

  private void runGraphPhase(
      @Nonnull OperationContext ctx,
      @Nonnull final Urn urn,
      @Nonnull final DeleteCascadeListener listener) {
    final DeleteCascadeCheckpoint current =
        new DeleteCascadeCheckpoint(DeleteCascadeCheckpoint.PHASE_GRAPH);
    String scrollId = null;
    do {
      listener.onPage(current);
      final RelatedEntitiesScrollResult page =
          _graphService.scrollRelatedEntities(
              ctx,
              null,
              newFilter("urn", urn.toString()),
              null,
              EMPTY_FILTER,
              ImmutableSet.of(),
              newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
              Edge.EDGE_SORT_CRITERION,
              scrollId,
              SCROLL_KEEP_ALIVE,
              PAGE_SIZE,
              null,
              null);
      scrollId = page.getScrollId();
      page.getEntities().forEach(edge -> removeGraphReference(ctx, urn, edge));
    } while (scrollId != null);
  }

  /**
   * Rewrites the edge's source without the reference. An edge no aspect field maps to (e.g. a
   * structured-property edge), or whose source no longer holds the reference, has nothing to
   * rewrite: node removal deletes it with the entity.
   */
  private void removeGraphReference(
      @Nonnull OperationContext ctx,
      @Nonnull final Urn deletedUrn,
      @Nonnull final RelatedEntity edge) {
    final Urn referrer = UrnUtils.getUrn(edge.getUrn());
    final String relationshipType = edge.getRelationshipType();
    final Map<String, AspectSpec> aspectSpecs =
        host.aspectSpecsReferringTo(
            deletedUrn.getEntityType(),
            relationshipType,
            ctx.getEntityRegistry().getEntitySpec(referrer.getEntityType()));
    if (aspectSpecs.isEmpty()) {
      log.debug(
          "No aspect of {} maps relationship {}; leaving the edge to node removal",
          referrer,
          relationshipType);
      return;
    }
    final EntityResponse response = readEntity(ctx, referrer, aspectSpecs.keySet());
    if (response == null) {
      return;
    }
    boolean removed = false;
    for (EnvelopedAspect current : response.getAspects().values()) {
      final AspectSpec aspectSpec = aspectSpecs.get(current.getName());
      if (aspectSpec != null
          && referencesUrn(current.getValue(), aspectSpec, relationshipType, deletedUrn)) {
        removeReference(ctx, referrer, current, aspectSpec, relationshipType, deletedUrn);
        removed = true;
      }
    }
    if (removed) {
      referenceRemoved();
    }
  }

  private static boolean referencesUrn(
      @Nonnull final Aspect value,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final String relationshipType,
      @Nonnull final Urn deletedUrn) {
    final RecordTemplate record =
        RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(), value.data());
    final String target = deletedUrn.toString();
    return FieldExtractor.extractFields(record, aspectSpec.getRelationshipFieldSpecs())
        .entrySet()
        .stream()
        .filter(entry -> entry.getKey().getRelationshipName().equals(relationshipType))
        .flatMap(entry -> entry.getValue().stream())
        .anyMatch(fieldValue -> target.equals(String.valueOf(fieldValue)));
  }

  /**
   * Writes {@code current} without the reference, conditional on the version read. When a required
   * field held it the whole aspect goes, bounded by that version: a newer value written since is
   * never deleted.
   */
  private void removeReference(
      @Nonnull OperationContext ctx,
      @Nonnull final Urn referrer,
      @Nonnull final EnvelopedAspect current,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final String relationshipType,
      @Nonnull final Urn deletedUrn) {
    final long version = DeleteCascadeReferenceChecks.versionOf(current);
    final Aspect updated =
        withReferenceRemoved(current.getValue(), aspectSpec, relationshipType, deletedUrn);
    if (updated == null) {
      final ConditionalDeleteOutcome outcome =
          _entityService.deleteAspectUpToVersion(ctx, referrer, current.getName(), version);
      if (outcome == ConditionalDeleteOutcome.PARTIAL) {
        // The newer latest was kept and may still hold the reference; the next run reads it.
        throw new IllegalStateException(
            String.format(
                "%s of %s changed past version %d while removing a reference to %s",
                current.getName(), referrer, version, deletedUrn));
      }
      return;
    }
    if (current.getValue().equals(updated)) {
      throw new IllegalStateException(
          String.format(
              "Reference to %s in %s of %s is not removable at its relationship path",
              deletedUrn, current.getName(), referrer));
    }
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(referrer);
    proposal.setEntityType(referrer.getEntityType());
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setAspectName(current.getName());
    proposal.setAspect(GenericRecordUtils.serializeAspect(updated));
    proposal.setHeaders(DeleteCascadeReferenceChecks.ifVersionMatch(version));
    ingest(ctx, referrer, proposal);
  }

  /**
   * @return the aspect without the reference, or null when a required field held it (the whole
   *     aspect has to go)
   */
  @Nullable
  private static Aspect withReferenceRemoved(
      @Nonnull final Aspect value,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final String relationshipType,
      @Nonnull final Urn deletedUrn) {
    Aspect current = value;
    for (RelationshipFieldSpec fieldSpec : aspectSpec.getRelationshipFieldSpecs()) {
      if (!fieldSpec.getRelationshipName().equals(relationshipType)) {
        continue;
      }
      current =
          DeleteEntityUtils.getAspectWithReferenceRemoved(
              deletedUrn.toString(), current, aspectSpec.getPegasusSchema(), fieldSpec.getPath());
      if (current == null) {
        return null;
      }
    }
    return current;
  }

  // ---------------------------------------------------------------- search-index phases

  /**
   * Assets found only through the search index: form and structured-property references, which
   * have no graph edge. Null for other entity types.
   */
  @Nullable
  static SearchScan searchReferenceScan(@Nonnull final Urn deletedUrn) {
    if (deletedUrn.getEntityType().equals("form")) {
      return new SearchScan(
          DeleteEntityUtils.getEntityNamesForFormDeletion(),
          DeleteEntityUtils.getFilterForFormDeletion(deletedUrn));
    }
    if (deletedUrn.getEntityType().equals("structuredProperty")) {
      return new SearchScan(
          DeleteEntityUtils.getEntityNamesForStructuredPropertyDeletion(),
          DeleteEntityUtils.getFilterForStructuredPropertyDeletion(deletedUrn));
    }
    return null;
  }

  /** File entities that belong to the deleted entity. */
  @Nonnull
  static SearchScan fileScan(@Nonnull final Urn deletedUrn) {
    return new SearchScan(
        ImmutableList.of(Constants.DATAHUB_FILE_ENTITY_NAME),
        DeleteEntityUtils.getFilterForFileDeletion(deletedUrn));
  }

  /**
   * Scrolls {@code scan} and hands each urn found to {@code perReferrer}, calling the listener
   * before each page. Package-private: the fork's subscriptions phase runs through it.
   */
  void runSearchPhase(
      @Nonnull OperationContext ctx,
      @Nullable final SearchScan scan,
      @Nonnull final String phase,
      @Nonnull final DeleteCascadeListener listener,
      @Nonnull final Consumer<Urn> perReferrer) {
    if (scan == null) {
      return;
    }
    final DeleteCascadeCheckpoint current = new DeleteCascadeCheckpoint(phase);
    String scrollId = null;
    do {
      listener.onPage(current);
      final ScrollResult page =
          _searchService.structuredScroll(
              ctx,
              scan.entityNames(),
              "*",
              scan.filter(),
              URN_SORT,
              scrollId,
              SCROLL_KEEP_ALIVE,
              PAGE_SIZE);
      scrollId = page.getScrollId();
      page.getEntities().stream().map(SearchEntity::getEntity).forEach(perReferrer);
    } while (scrollId != null);
  }

  private void removeSearchReference(
      @Nonnull OperationContext ctx, @Nonnull final Urn deletedUrn, @Nonnull final Urn assetUrn) {
    if (host.shouldDeleteAssetReferencingUrn(assetUrn, deletedUrn)) {
      // A metadata test created for the deleted form; deleteUrn is a no-op once it is gone.
      _entityService.deleteUrn(ctx, assetUrn);
      referenceRemoved();
      return;
    }
    for (String aspectName : host.aspectsToUpdate(deletedUrn, assetUrn)) {
      removeSearchReferenceAspect(ctx, deletedUrn, assetUrn, aspectName);
    }
  }

  /**
   * Reads the aspect (and its version) before the repo-specific builder re-reads it, then makes the
   * write conditional on that first read: any write in between fails the write.
   */
  private void removeSearchReferenceAspect(
      @Nonnull OperationContext ctx,
      @Nonnull final Urn deletedUrn,
      @Nonnull final Urn assetUrn,
      @Nonnull final String aspectName) {
    final EntityResponse response = readEntity(ctx, assetUrn, Set.of(aspectName));
    final EnvelopedAspect current = response == null ? null : response.getAspects().get(aspectName);
    if (current == null
        || !DeleteCascadeReferenceChecks.searchAspectReferences(
            aspectName, current.getValue(), deletedUrn)) {
      return;
    }
    final MetadataChangeProposal proposal =
        host.updateAspectForSearchReference(ctx, assetUrn, deletedUrn, aspectName);
    if (proposal == null) {
      return;
    }
    proposal.setHeaders(
        DeleteCascadeReferenceChecks.ifVersionMatch(
            DeleteCascadeReferenceChecks.versionOf(current)));
    ingest(ctx, assetUrn, proposal);
    referenceRemoved();
  }

  private void removeFileReference(@Nonnull OperationContext ctx, @Nonnull final Urn fileUrn) {
    final EntityResponse file =
        readEntity(
            ctx,
            fileUrn,
            Set.of(Constants.DATAHUB_FILE_INFO_ASPECT_NAME, Constants.STATUS_ASPECT_NAME));
    if (!DeleteCascadeReferenceChecks.isLiveFileReference(file)) {
      // Gone, or soft-deleted by an earlier run: done.
      return;
    }
    host.deleteStorageObject(
        fileUrn,
        new DataHubFileInfo(
            file.getAspects().get(Constants.DATAHUB_FILE_INFO_ASPECT_NAME).getValue().data()));
    ingest(ctx, fileUrn, DeleteEntityUtils.buildSoftDeleteProposal(fileUrn));
    referenceRemoved();
  }

  // ---------------------------------------------------------------- reads and writes

  /**
   * Ingests synchronously, so the next page only starts after the write committed.
   *
   * @throws IllegalStateException when the write was not committed. Without a RequestContext a
   *     failed pre-commit check (e.g. If-Version-Match) is not thrown: the item becomes a failed
   *     MCP, is left out of the results, and ingestProposal returns null.
   */
  private void ingest(
      @Nonnull OperationContext ctx,
      @Nonnull final Urn referrer,
      @Nonnull final MetadataChangeProposal proposal) {
    if (proposal.getSystemMetadata() == null) {
      proposal.setSystemMetadata(new SystemMetadata());
    }
    cascade.attachToSystemMetadata(proposal.getSystemMetadata());
    final IngestResult result =
        _entityService.ingestProposal(ctx, proposal, host.createAuditStamp(), false);
    if (result == null || !result.isSqlCommitted()) {
      throw new IllegalStateException(
          String.format(
              "Write of %s to %s was not committed (it changed since it was read)",
              proposal.getAspectName(), referrer));
    }
  }

  @Nullable
  private EntityResponse readEntity(
      @Nonnull OperationContext ctx,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames) {
    try {
      return _entityService.getEntityV2(ctx, urn.getEntityType(), urn, aspectNames);
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Unreadable urn " + urn, e);
    }
  }
}
