package com.linkedin.metadata.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.entity.transactions.AspectsBatch;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface EntityService {

  /**
   * Just whether the entity/aspect exists
   *
   * @param urn urn for the entity
   * @param aspectName aspect for the entity
   * @return exists or not
   */
  Boolean exists(Urn urn, String aspectName);

  /**
   * Retrieves the latest aspects corresponding to a batch of {@link Urn}s based on a provided set
   * of aspect names.
   *
   * @param urns set of urns to fetch aspects for
   * @param aspectNames aspects to fetch for each urn in urns set
   * @return a map of provided {@link Urn} to a List containing the requested aspects.
   */
  Map<Urn, List<RecordTemplate>> getLatestAspects(
      @Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames);

  Map<String, RecordTemplate> getLatestAspectsForUrn(
      @Nonnull final Urn urn, @Nonnull final Set<String> aspectNames);

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
      @Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version);

  /**
   * Retrieves the latest aspects for the given urn as dynamic aspect objects (Without having to
   * define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urn urn of entity to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  EntityResponse getEntityV2(
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames)
      throws URISyntaxException;

  /**
   * Retrieves the latest aspects for the given set of urns as dynamic aspect objects (Without
   * having to define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  Map<Urn, EntityResponse> getEntitiesV2(
      @Nonnull final String entityName,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames)
      throws URISyntaxException;

  /**
   * Retrieves the aspects for the given set of urns and versions as dynamic aspect objects (Without
   * having to define union objects)
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized
   *     string
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  Map<Urn, EntityResponse> getEntitiesVersionedV2(
      @Nonnull final Set<VersionedUrn> versionedUrns, @Nonnull final Set<String> aspectNames)
      throws URISyntaxException;

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param entityName name of the entity to fetch
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link EnvelopedAspect} object
   */
  Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(
      // TODO: entityName is unused, can we remove this as a param?
      @Nonnull String entityName, @Nonnull Set<Urn> urns, @Nonnull Set<String> aspectNames)
      throws URISyntaxException;

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized
   *     string
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link EnvelopedAspect} object
   */
  Map<Urn, List<EnvelopedAspect>> getVersionedEnvelopedAspects(
      @Nonnull Set<VersionedUrn> versionedUrns, @Nonnull Set<String> aspectNames)
      throws URISyntaxException;

  /**
   * Retrieves the latest aspect for the given urn as a list of enveloped aspects
   *
   * @param entityName name of the entity to fetch
   * @param urn urn to fetch
   * @param aspectName name of the aspect to fetch
   * @return {@link EnvelopedAspect} object, or null if one cannot be found
   */
  EnvelopedAspect getLatestEnvelopedAspect(
      @Nonnull final String entityName, @Nonnull final Urn urn, @Nonnull final String aspectName)
      throws Exception;

  @Deprecated
  VersionedAspect getVersionedAspect(@Nonnull Urn urn, @Nonnull String aspectName, long version);

  ListResult<RecordTemplate> listLatestAspects(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int count);

  List<UpdateAspectResult> ingestAspects(
      @Nonnull final Urn urn,
      @Nonnull List<Pair<String, RecordTemplate>> aspectRecordsToIngest,
      @Nonnull final AuditStamp auditStamp,
      @Nullable SystemMetadata systemMetadata);

  List<UpdateAspectResult> ingestAspects(
      @Nonnull final AspectsBatch aspectsBatch,
      @Nonnull final AuditStamp auditStamp,
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
   */
  RecordTemplate ingestAspectIfNotPresent(
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate newValue,
      @Nonnull AuditStamp auditStamp,
      @Nullable SystemMetadata systemMetadata);

  // TODO: Why not in RetentionService?
  String batchApplyRetention(
      Integer start, Integer count, Integer attemptWithVersion, String aspectName, String urn);

  Integer getCountAspect(@Nonnull String aspectName, @Nullable String urnLike);

  // TODO: Extract this to a different service, doesn't need to be here
  RestoreIndicesResult restoreIndices(
      @Nonnull RestoreIndicesArgs args, @Nonnull Consumer<String> logger);

  ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count);

  @Deprecated
  Entity getEntity(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames);

  @Deprecated
  Map<Urn, Entity> getEntities(@Nonnull final Set<Urn> urns, @Nonnull Set<String> aspectNames);

  Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
      @Nonnull final Urn urn,
      AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog);

  Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
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

  RecordTemplate getLatestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName);

  @Deprecated
  void ingestEntities(
      @Nonnull final List<Entity> entities,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final List<SystemMetadata> systemMetadata);

  @Deprecated
  SystemMetadata ingestEntity(Entity entity, AuditStamp auditStamp);

  @Deprecated
  void ingestEntity(
      @Nonnull Entity entity,
      @Nonnull AuditStamp auditStamp,
      @Nonnull SystemMetadata systemMetadata);

  void setRetentionService(RetentionService retentionService);

  AspectSpec getKeyAspectSpec(@Nonnull final Urn urn);

  Optional<AspectSpec> getAspectSpec(
      @Nonnull final String entityName, @Nonnull final String aspectName);

  String getKeyAspectName(@Nonnull final Urn urn);

  /**
   * Generate default aspects if not present in the database.
   *
   * @param urn entity urn
   * @param includedAspects aspects being written
   * @return additional aspects to be written
   */
  List<Pair<String, RecordTemplate>> generateDefaultAspectsIfMissing(
      @Nonnull final Urn urn, Map<String, RecordTemplate> includedAspects);

  /**
   * Generate default aspects if the entity key aspect is NOT in the database **AND** the key aspect
   * is being written, present in `includedAspects`.
   *
   * <p>Does not automatically create key aspects.
   *
   * @see EntityService#generateDefaultAspectsIfMissing if key aspects need autogeneration
   *     <p>This version is more efficient in that it only generates additional writes when a new
   *     entity is being minted for the first time. The drawback is that it will not automatically
   *     add key aspects, in case the producer is not bothering to ensure that the entity exists
   *     before writing non-key aspects.
   * @param urn entity urn
   * @param includedAspects aspects being written
   * @return whether key aspect exists in database and the additional aspects to be written
   */
  Pair<Boolean, List<Pair<String, RecordTemplate>>> generateDefaultAspectsOnFirstWrite(
      @Nonnull final Urn urn, Map<String, RecordTemplate> includedAspects);

  AspectSpec getKeyAspectSpec(@Nonnull final String entityName);

  Set<String> getEntityAspectNames(final String entityName);

  EntityRegistry getEntityRegistry();

  RollbackResult deleteAspect(
      String urn, String aspectName, @Nonnull Map<String, String> conditions, boolean hardDelete);

  RollbackRunResult deleteUrn(Urn urn);

  RollbackRunResult rollbackRun(
      List<AspectRowSummary> aspectRows, String runId, boolean hardDelete);

  RollbackRunResult rollbackWithConditions(
      List<AspectRowSummary> aspectRows, Map<String, String> conditions, boolean hardDelete);

  Set<IngestResult> ingestProposal(
      AspectsBatch aspectsBatch, AuditStamp auditStamp, final boolean async);

  /**
   * If you have more than 1 proposal use the {AspectsBatch} method
   *
   * @param proposal the metadata proposal to ingest
   * @param auditStamp audit information
   * @param async async ingestion or sync ingestion
   * @return ingestion result
   */
  IngestResult ingestProposal(
      MetadataChangeProposal proposal, AuditStamp auditStamp, final boolean async);

  Boolean exists(Urn urn);

  Boolean isSoftDeleted(@Nonnull final Urn urn);

  void setWritable(boolean canWrite);

  BrowsePaths buildDefaultBrowsePath(final @Nonnull Urn urn) throws URISyntaxException;

  /**
   * Builds the default browse path V2 aspects for all entities.
   *
   * <p>This method currently supports datasets, charts, dashboards, and data jobs best. Everything
   * else will have a basic "Default" folder added to their browsePathV2.
   */
  @Nonnull
  BrowsePathsV2 buildDefaultBrowsePathV2(final @Nonnull Urn urn, boolean useContainerPaths)
      throws URISyntaxException;

  /**
   * Allow internal use of the system entity client. Solves recursive dependencies between the
   * EntityService and the SystemJavaEntityClient
   *
   * @param systemEntityClient system entity client
   */
  void setSystemEntityClient(SystemEntityClient systemEntityClient);
}
