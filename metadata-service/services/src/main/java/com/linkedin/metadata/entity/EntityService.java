package com.linkedin.metadata.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface EntityService<U extends ChangeMCP> {

  /**
   * Just whether the entity/aspect exists
   *
   * @param urns urns for the entities
   * @param aspectName aspect for the entity, if null, assumes key aspect
   * @param includeSoftDelete including soft deleted entities
   * @param forUpdate whether the operation is intending to write to this row in a tx
   * @return set of urns with the specified aspect existing
   */
  Set<Urn> exists(
      @Nonnull OperationContext opContext,
      @Nonnull final Collection<Urn> urns,
      @Nullable String aspectName,
      boolean includeSoftDelete,
      boolean forUpdate);

  /**
   * Just whether the entity/aspect exists, prefer batched method.
   *
   * @param urn urn for the entity
   * @param aspectName aspect for the entity, if null use the key aspect
   * @param includeSoftDelete including soft deleted entities
   * @return boolean if the entity/aspect exists
   */
  default boolean exists(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nullable String aspectName,
      boolean includeSoftDelete) {
    return exists(opContext, Set.of(urn), aspectName, includeSoftDelete, false).contains(urn);
  }

  /**
   * Returns a set of urns of entities that exist (has materialized aspects).
   *
   * @param urns the list of urns of the entities to check
   * @param includeSoftDelete including soft deleted entities
   * @return a set of urns of entities that exist.
   */
  default Set<Urn> exists(
      @Nonnull OperationContext opContext,
      @Nonnull final Collection<Urn> urns,
      boolean includeSoftDelete) {
    return exists(opContext, urns, null, includeSoftDelete, false);
  }

  /**
   * Returns a set of urns of entities that exist (has materialized aspects).
   *
   * @param urns the list of urns of the entities to check
   * @param includeSoftDelete including soft deleted entities
   * @param forUpdate whether the operation is intending to write to this row in a tx
   * @return a set of urns of entities that exist.
   */
  default Set<Urn> exists(
      @Nonnull OperationContext opContext,
      @Nonnull final Collection<Urn> urns,
      boolean includeSoftDelete,
      boolean forUpdate) {
    return exists(opContext, urns, null, includeSoftDelete, forUpdate);
  }

  /**
   * Returns a set of urns of entities that exist (has materialized aspects).
   *
   * @param urns the list of urns of the entities to check
   * @return a set of urns of entities that exist.
   */
  default Set<Urn> exists(
      @Nonnull OperationContext opContext, @Nonnull final Collection<Urn> urns) {
    return exists(opContext, urns, true, false);
  }

  /**
   * Returns whether the urn of the entity exists (has materialized aspects).
   *
   * @param urn the urn of the entity to check
   * @param includeSoftDelete including soft deleted entities
   * @return entities exists.
   */
  default boolean exists(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, boolean includeSoftDelete) {
    return exists(opContext, List.of(urn), includeSoftDelete, false).contains(urn);
  }

  /**
   * Returns whether the urn of the entity exists (has materialized aspects).
   *
   * @param urn the urn of the entity to check
   * @return entities exists.
   */
  default boolean exists(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      boolean includeSoftDelete,
      boolean forUpdate) {
    return exists(opContext, List.of(urn), includeSoftDelete, forUpdate).contains(urn);
  }

  /**
   * Returns whether the urn of the entity exists (has materialized aspects).
   *
   * @param urn the urn of the entity to check
   * @return entities exists.
   */
  default boolean exists(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    return exists(opContext, urn, true, false);
  }

  /**
   * Retrieves the latest aspects corresponding to a batch of {@link Urn}s based on a provided set
   * of aspect names.
   *
   * @param urns set of urns to fetch aspects for
   * @param aspectNames aspects to fetch for each urn in urns set
   * @param alwaysIncludeKeyAspect historically the key aspect was always added, allow disabling
   *     this behavior
   * @return a map of provided {@link Urn} to a List containing the requested aspects.
   */
  Map<Urn, List<RecordTemplate>> getLatestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect);

  @Deprecated
  default Map<Urn, List<RecordTemplate>> getLatestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {
    return getLatestAspects(opContext, urns, aspectNames, true);
  }

  Map<String, RecordTemplate> getLatestAspectsForUrn(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames,
      boolean forUpdate);

  /**
   * Retrieves an aspect having a specific {@link Urn}, name, & version.
   *
   * <p>Note that once we drop support for legacy aspect-specific resources, we should make this a
   * protected method. Only visible for backwards compatibility.
   *
   * @param urn an urn associated with the requested aspect
   * @param aspectName name of the aspect requested
   * @param version specific version of the aspect being requests
   * @return the {@link RecordTemplate} representation of the requested aspect object, or null if
   *     one cannot be found
   */
  RecordTemplate getAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull long version);

  /**
   * Retrieves the latest aspects for the given urn as dynamic aspect objects (Without having to
   * define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urn urn of entity to fetch
   * @param aspectNames set of aspects to fetch
   * @param alwaysIncludeKeyAspect historically the key aspect was always added, allow disabling
   *     this behavior
   * @return a map of {@link Urn} to {@link Entity} object
   */
  EntityResponse getEntityV2(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException;

  @Deprecated
  default EntityResponse getEntityV2(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames)
      throws URISyntaxException {
    return getEntityV2(opContext, entityName, urn, aspectNames, true);
  }

  /**
   * Retrieves the latest aspects for the given set of urns as dynamic aspect objects (Without
   * having to define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @param alwaysIncludeKeyAspect historically the key aspect was always added, allow disabling
   *     this behavior
   * @return a map of {@link Urn} to {@link Entity} object
   */
  Map<Urn, EntityResponse> getEntitiesV2(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException;

  default Map<Urn, EntityResponse> getEntitiesV2(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames)
      throws URISyntaxException {
    return getEntitiesV2(opContext, entityName, urns, aspectNames, true);
  }

  /**
   * Retrieves the aspects for the given set of urns and versions as dynamic aspect objects (Without
   * having to define union objects)
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized
   *     string
   * @param aspectNames set of aspects to fetch
   * @param alwaysIncludeKeyAspect historically the key aspect was always added, allow disabling
   *     this behavior
   * @return a map of {@link Urn} to {@link Entity} object
   */
  Map<Urn, EntityResponse> getEntitiesVersionedV2(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException;

  @Deprecated
  default Map<Urn, EntityResponse> getEntitiesVersionedV2(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nonnull final Set<String> aspectNames)
      throws URISyntaxException {
    return getEntitiesVersionedV2(opContext, versionedUrns, aspectNames, true);
  }

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @param alwaysIncludeKeyAspect historically the key aspect was always added, allow disabling
   *     this behavior
   * @return a map of {@link Urn} to {@link EnvelopedAspect} object
   */
  Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException;

  /**
   * Retrieve the specified aspect versions for the given URNs
   *
   * @param opContext operation context
   * @param urnAspectVersions map of the urn's aspect versions
   * @param alwaysIncludeKeyAspect whether to include the key aspect
   * @return enveloped aspects with the specific version
   * @throws URISyntaxException
   */
  Map<Urn, List<EnvelopedAspect>> getEnvelopedVersionedAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, Map<String, Long>> urnAspectVersions,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException;

  @Deprecated
  default Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> urns, @Nonnull Set<String> aspectNames)
      throws URISyntaxException {
    return getLatestEnvelopedAspects(opContext, urns, aspectNames, true);
  }

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized
   *     string
   * @param aspectNames set of aspects to fetch
   * @param alwaysIncludeKeyAspect historically the key aspect was always added, allow disabling
   *     this behavior
   * @return a map of {@link Urn} to {@link EnvelopedAspect} object
   */
  Map<Urn, List<EnvelopedAspect>> getVersionedEnvelopedAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<VersionedUrn> versionedUrns,
      @Nonnull Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException;

  @Deprecated
  default Map<Urn, List<EnvelopedAspect>> getVersionedEnvelopedAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<VersionedUrn> versionedUrns,
      @Nonnull Set<String> aspectNames)
      throws URISyntaxException {
    return getVersionedEnvelopedAspects(opContext, versionedUrns, aspectNames, true);
  }

  /**
   * Retrieves the latest aspect for the given urn as a list of enveloped aspects
   *
   * @param entityName name of the entity to fetch
   * @param urn urn to fetch
   * @param aspectName name of the aspect to fetch
   * @return {@link EnvelopedAspect} object, or null if one cannot be found
   */
  EnvelopedAspect getLatestEnvelopedAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName)
      throws Exception;

  @Deprecated
  VersionedAspect getVersionedAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      long version);

  ListResult<RecordTemplate> listLatestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int count);

  List<UpdateAspectResult> ingestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull List<Pair<String, RecordTemplate>> aspectRecordsToIngest,
      @Nonnull final AuditStamp auditStamp,
      @Nullable SystemMetadata systemMetadata);

  List<UpdateAspectResult> ingestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull final AspectsBatch aspectsBatch,
      boolean emitMCL,
      boolean overwrite);

  /**
   * Ingests (inserts) a new version of an entity aspect & emits a {@link
   * com.linkedin.mxe.MetadataAuditEvent}.
   *
   * <p>This method runs a read -> write atomically in a single transaction, this is to prevent
   * multiple IDs from being created.
   *
   * <p>Note that in general, this should not be used externally. It is currently serving upgrade
   * scripts and is as such public.
   *
   * @param urn an urn associated with the new aspect
   * @param aspectName name of the aspect being inserted
   * @param newValue value of the aspect being inserted
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time
   * @param systemMetadata
   * @return the {@link RecordTemplate} representation of the written aspect object
   * @deprecated See Conditional Write ChangeType CREATE
   */
  @Deprecated
  RecordTemplate ingestAspectIfNotPresent(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate newValue,
      @Nonnull AuditStamp auditStamp,
      @Nullable SystemMetadata systemMetadata);

  // TODO: Why not in RetentionService?
  String batchApplyRetention(
      @Nonnull OperationContext opContext,
      Integer start,
      Integer count,
      Integer attemptWithVersion,
      String aspectName,
      String urn);

  Integer getCountAspect(
      @Nonnull OperationContext opContext, @Nonnull String aspectName, @Nullable String urnLike);

  // TODO: Extract this to a different service, doesn't need to be here
  List<RestoreIndicesResult> restoreIndices(
      @Nonnull OperationContext opContext,
      @Nonnull RestoreIndicesArgs args,
      @Nonnull Consumer<String> logger);

  // Restore indices from list using key lookups (no scans)
  List<RestoreIndicesResult> restoreIndices(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> inputAspectNames,
      @Nullable Integer inputBatchSize)
      throws RemoteInvocationException, URISyntaxException;

  ListUrnsResult listUrns(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      final int start,
      final int count);

  @Deprecated
  Entity getEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect);

  @Deprecated
  Map<Urn, Entity> getEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect);

  Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog);

  /**
   * Generally should not be necessary, created for delete flow which does not have System Metadata,
   * so it lacks a way to force through index updates synchronously.
   *
   * @param opContext the current operation context
   * @param urn urn to produce event for
   * @param aspectSpec aspect of the entity
   * @param metadataChangeLog the MCL to produce
   * @return list of the mcl produce future along with a boolean indicating if the event was
   *     pre-processed
   */
  Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog,
      boolean forcePreProcessHooks);

  Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable final AspectSpec aspectSpec,
      @Nullable final RecordTemplate oldAspectValue,
      @Nullable final RecordTemplate newAspectValue,
      @Nullable final SystemMetadata oldSystemMetadata,
      @Nullable final SystemMetadata newSystemMetadata,
      @Nonnull AuditStamp auditStamp,
      @Nonnull final ChangeType changeType,
      boolean forcePreProcessHooks);

  Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate oldAspectValue,
      @Nullable final RecordTemplate newAspectValue,
      @Nullable final SystemMetadata oldSystemMetadata,
      @Nullable final SystemMetadata newSystemMetadata,
      @Nonnull AuditStamp auditStamp,
      @Nonnull final ChangeType changeType);

  // RecordTemplate getLatestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName);

  @Deprecated
  void ingestEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Entity> entities,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final List<SystemMetadata> systemMetadata);

  @Deprecated
  SystemMetadata ingestEntity(
      @Nonnull OperationContext opContext, Entity entity, AuditStamp auditStamp);

  @Deprecated
  void ingestEntity(
      @Nonnull OperationContext opContext,
      @Nonnull Entity entity,
      @Nonnull AuditStamp auditStamp,
      @Nonnull SystemMetadata systemMetadata);

  void setRetentionService(RetentionService<U> retentionService);

  default Optional<RollbackResult> deleteAspect(
      @Nonnull OperationContext opContext,
      String urn,
      String aspectName,
      @Nonnull Map<String, String> conditions,
      boolean hardDelete) {
    return deleteAspect(opContext, urn, aspectName, conditions, hardDelete, false);
  }

  default Optional<RollbackResult> deleteAspect(
      @Nonnull OperationContext opContext,
      String urn,
      String aspectName,
      @Nonnull Map<String, String> conditions,
      boolean hardDelete,
      boolean preProcessHooks) {
    AspectRowSummary aspectRowSummary =
        new AspectRowSummary().setUrn(urn).setAspectName(aspectName);
    return rollbackWithConditions(
            opContext, List.of(aspectRowSummary), conditions, hardDelete, preProcessHooks)
        .getRollbackResults()
        .stream()
        .findFirst();
  }

  RollbackRunResult deleteUrn(@Nonnull OperationContext opContext, Urn urn);

  RollbackRunResult rollbackRun(
      @Nonnull OperationContext opContext,
      List<AspectRowSummary> aspectRows,
      String runId,
      boolean hardDelete);

  RollbackRunResult rollbackWithConditions(
      @Nonnull OperationContext opContext,
      List<AspectRowSummary> aspectRows,
      Map<String, String> conditions,
      boolean hardDelete,
      boolean preProcessHooks);

  List<IngestResult> ingestProposal(
      @Nonnull OperationContext opContext, AspectsBatch aspectsBatch, final boolean async);

  /**
   * If you have more than 1 proposal use the {AspectsBatch} method
   *
   * @param proposal the metadata proposal to ingest
   * @param auditStamp audit information
   * @param async async ingestion or sync ingestion
   * @return ingestion result
   */
  IngestResult ingestProposal(
      @Nonnull OperationContext opContext,
      MetadataChangeProposal proposal,
      AuditStamp auditStamp,
      final boolean async);

  void setWritable(boolean canWrite);

  RecordTemplate getLatestAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName);

  SearchIndicesService getUpdateIndicesService();

  void setUpdateIndicesService(@Nullable SearchIndicesService updateIndicesService);
}
