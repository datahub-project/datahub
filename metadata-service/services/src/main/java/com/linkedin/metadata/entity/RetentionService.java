package com.linkedin.metadata.entity;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.retention.RetentionBatchEntry;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.key.DataHubRetentionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;

/**
 * Service coupled with an {@link EntityService} to handle aspect record retention.
 *
 * <p>TODO: This class is abstract with storage-specific implementations. It'd be nice to pull
 * storage and retention concerns apart, let AspectDaos deal with storage, and merge all retention
 * concerns into a single class.
 */
public abstract class RetentionService<U extends ChangeMCP> {
  protected static final String ALL = "*";

  protected RetentionService() {}

  protected abstract EntityService<U> getEntityService();

  protected abstract SystemEntityClient getSystemEntityClient();

  /**
   * Fetch retention policies given the entityName and aspectName. Resolution walks the prioritized
   * list of retention keys: (entity, aspect), (entity, *), (*, aspect), (*, *). Reads go through
   * {@link SystemEntityClient}'s entity/aspect cache (see {@code
   * cache.client.entityClient.entityAspectTTLSeconds.dataHubRetention.dataHubRetentionConfig} in
   * application.yaml), so repeated lookups for the same policy avoid a primary-storage read until
   * the TTL expires or {@link #setRetention}/{@link #deleteRetention} invalidates the entry.
   *
   * @param entityName Name of the entity
   * @param aspectName Name of the aspect
   * @return retention policies to apply to the input entity and aspect
   */
  @SneakyThrows
  public Retention getRetention(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String aspectName) {
    // Prioritized list of retention keys to fetch
    List<Urn> retentionUrns = getRetentionKeys(entityName, aspectName);
    Map<Urn, EntityResponse> fetchedAspects =
        getSystemEntityClient()
            .batchGetV2(
                opContext,
                new HashSet<>(retentionUrns),
                Set.of(Constants.DATAHUB_RETENTION_ASPECT));
    // Find the first retention info that is set among the prioritized list of retention keys above
    Optional<DataHubRetentionConfig> retentionInfo =
        retentionUrns.stream()
            .map(fetchedAspects::get)
            .filter(Objects::nonNull)
            .map(response -> response.getAspects().get(Constants.DATAHUB_RETENTION_ASPECT))
            .filter(Objects::nonNull)
            .findFirst()
            .map(envelopedAspect -> new DataHubRetentionConfig(envelopedAspect.getValue().data()));
    return retentionInfo.map(DataHubRetentionConfig::getRetention).orElse(new Retention());
  }

  /**
   * Returns the effective max versions to keep for the given entity/aspect for the write path. When
   * this is <= 1, callers should not create version-history rows (only update version 0). When > 1,
   * callers should insert the previous version as history. Resolution uses the same policy lookup
   * as {@link #getRetention}: (entity, aspect), (entity, *), (*, aspect), (*, *).
   *
   * <p>When there is no version-based retention policy (time-only or no policy), returns 1 so only
   * the current version is retained—consistent with retention service not being enabled.
   *
   * @param opContext operation context
   * @param entityName entity type
   * @param aspectName aspect name
   * @return 1 if no version policy; else version.maxVersions if set
   */
  public int getMaxVersionsToKeepForWrite(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String aspectName) {
    Retention retention = getRetention(opContext, entityName, aspectName);
    if (retention.hasVersion()) {
      return retention.getVersion().getMaxVersions();
    }
    return 1;
  }

  /**
   * Returns the retention policy stored at the exact {@code (entityName, aspectName)} key only,
   * without wildcard fallback.
   */
  public Optional<DataHubRetentionConfig> getRetentionConfigAtExactKey(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String aspectName) {
    Urn retentionUrn = toRetentionUrn(entityName, aspectName);
    Map<Urn, List<RecordTemplate>> fetchedAspects =
        getEntityService()
            .getLatestAspects(
                opContext, Set.of(retentionUrn), Set.of(Constants.DATAHUB_RETENTION_ASPECT));
    return fetchedAspects.getOrDefault(retentionUrn, Collections.emptyList()).stream()
        .filter(aspect -> aspect instanceof DataHubRetentionConfig)
        .map(aspect -> (DataHubRetentionConfig) aspect)
        .findFirst();
  }

  /** Compares two retention configs for equality (exact-key policy comparison). */
  public boolean retentionConfigEquals(
      @Nonnull DataHubRetentionConfig a, @Nonnull DataHubRetentionConfig b) {
    return Objects.equals(a, b);
  }

  /**
   * Returns true if there is no policy at the exact key, or the policy was last written by the
   * system actor.
   */
  public boolean isRetentionPolicySystemManaged(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String aspectName) {
    if (getRetentionConfigAtExactKey(opContext, entityName, aspectName).isEmpty()) {
      return true;
    }
    return getRetentionPolicyLastWriter(opContext, entityName, aspectName)
        .map(actor -> Constants.SYSTEM_ACTOR.equals(actor.toString()))
        .orElse(false);
  }

  /** Last writer of the retention config aspect at the exact key, if known. */
  @Nonnull
  public Optional<Urn> getRetentionPolicyLastWriter(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String aspectName) {
    try {
      Urn retentionUrn = toRetentionUrn(entityName, aspectName);
      EnvelopedAspect envelopedAspect =
          getEntityService()
              .getLatestEnvelopedAspect(
                  opContext,
                  Constants.DATAHUB_RETENTION_ENTITY,
                  retentionUrn,
                  Constants.DATAHUB_RETENTION_ASPECT);
      if (envelopedAspect != null
          && envelopedAspect.hasCreated()
          && envelopedAspect.getCreated().hasActor()) {
        return Optional.of(envelopedAspect.getCreated().getActor());
      }
    } catch (Exception ignored) {
      // Treat as non-system-managed when provenance cannot be read.
    }
    return Optional.empty();
  }

  protected static Urn toRetentionUrn(@Nonnull String entityName, @Nonnull String aspectName) {
    DataHubRetentionKey retentionKey = new DataHubRetentionKey();
    retentionKey.setEntityName(entityName);
    retentionKey.setAspectName(aspectName);
    return EntityKeyUtils.convertEntityKeyToUrn(retentionKey, Constants.DATAHUB_RETENTION_ENTITY);
  }

  // Get list of datahub retention keys that match the input entity name and aspect name
  protected static List<Urn> getRetentionKeys(
      @Nonnull String entityName, @Nonnull String aspectName) {
    return ImmutableList.of(
            new DataHubRetentionKey().setEntityName(entityName).setAspectName(aspectName),
            new DataHubRetentionKey().setEntityName(entityName).setAspectName(ALL),
            new DataHubRetentionKey().setEntityName(ALL).setAspectName(aspectName),
            new DataHubRetentionKey().setEntityName(ALL).setAspectName(ALL))
        .stream()
        .map(key -> EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_RETENTION_ENTITY))
        .collect(Collectors.toList());
  }

  /**
   * Set retention policy for given entity and aspect. If entity or aspect names are null, the
   * policy is set as default
   *
   * @param entityName Entity name to apply policy to. If null, set as "*", meaning it will be the
   *     default for any entities without specified policy
   * @param aspectName Aspect name to apply policy to. If null, set as "*", meaning it will be the
   *     default for any aspects without specified policy
   * @param retentionConfig Retention policy
   */
  @SneakyThrows
  public boolean setRetention(
      @Nonnull OperationContext opContext,
      @Nullable String entityName,
      @Nullable String aspectName,
      @Nonnull DataHubRetentionConfig retentionConfig) {
    validateRetention(retentionConfig.getRetention());
    DataHubRetentionKey retentionKey = new DataHubRetentionKey();
    retentionKey.setEntityName(entityName != null ? entityName : ALL);
    retentionKey.setAspectName(aspectName != null ? aspectName : ALL);
    Urn retentionUrn =
        EntityKeyUtils.convertEntityKeyToUrn(retentionKey, Constants.DATAHUB_RETENTION_ENTITY);

    MetadataChangeProposal keyProposal = new MetadataChangeProposal();
    GenericAspect keyAspect = GenericRecordUtils.serializeAspect(retentionKey);
    keyProposal.setAspect(keyAspect);
    keyProposal.setAspectName(Constants.DATAHUB_RETENTION_KEY_ASPECT);
    keyProposal.setEntityType(Constants.DATAHUB_RETENTION_ENTITY);
    keyProposal.setChangeType(ChangeType.UPSERT);
    keyProposal.setEntityUrn(retentionUrn);

    MetadataChangeProposal aspectProposal = keyProposal.clone();
    GenericAspect retentionAspect = GenericRecordUtils.serializeAspect(retentionConfig);
    aspectProposal.setAspect(retentionAspect);
    aspectProposal.setAspectName(Constants.DATAHUB_RETENTION_ASPECT);

    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    AspectsBatch batch =
        buildAspectsBatch(opContext, List.of(keyProposal, aspectProposal), auditStamp);

    boolean committed =
        getEntityService().ingestProposal(opContext, batch, false).stream()
            .anyMatch(IngestResult::isSqlCommitted);
    getSystemEntityClient()
        .getEntityClientCache()
        .invalidate(retentionUrn, Set.of(Constants.DATAHUB_RETENTION_ASPECT));
    return committed;
  }

  protected abstract AspectsBatch buildAspectsBatch(
      @Nonnull OperationContext opContext,
      List<MetadataChangeProposal> mcps,
      @Nonnull AuditStamp auditStamp);

  /**
   * Delete the retention policy set for given entity and aspect.
   *
   * @param entityName Entity name to apply policy to. If null, set as "*", meaning it will delete
   *     the default policy for any entities without specified policy
   * @param aspectName Aspect name to apply policy to. If null, set as "*", meaning it will delete
   *     the default policy for any aspects without specified policy
   */
  public void deleteRetention(
      @Nonnull OperationContext opContext,
      @Nullable String entityName,
      @Nullable String aspectName) {
    DataHubRetentionKey retentionKey = new DataHubRetentionKey();
    retentionKey.setEntityName(entityName != null ? entityName : ALL);
    retentionKey.setAspectName(aspectName != null ? aspectName : ALL);
    Urn retentionUrn =
        EntityKeyUtils.convertEntityKeyToUrn(retentionKey, Constants.DATAHUB_RETENTION_ENTITY);
    getEntityService().deleteUrn(opContext, retentionUrn);
    getSystemEntityClient()
        .getEntityClientCache()
        .invalidate(retentionUrn, Set.of(Constants.DATAHUB_RETENTION_ASPECT));
  }

  private void validateRetention(Retention retention) {
    if (retention.hasVersion()) {
      if (retention.getVersion().getMaxVersions() <= 0) {
        throw new IllegalArgumentException(
            "Invalid maxVersions: " + retention.getVersion().getMaxVersions());
      }
    }
    if (retention.hasTime()) {
      if (retention.getTime().getMaxAgeInSeconds() <= 0) {
        throw new IllegalArgumentException(
            "Invalid maxAgeInSeconds: " + retention.getTime().getMaxAgeInSeconds());
      }
    }
  }

  /**
   * Apply retention policies given the urn and aspect name
   *
   * @param retentionContexts urn, aspect name, and additional context that could be used to apply
   *     retention
   */
  public void applyRetentionWithPolicyDefaults(
      @Nonnull OperationContext opContext, @Nonnull List<RetentionContext> retentionContexts) {
    List<RetentionContext> withDefaults =
        retentionContexts.stream()
            .map(
                context -> {
                  if (context.getRetentionPolicy().isEmpty()) {
                    Retention retentionPolicy =
                        getRetention(
                            opContext, context.getUrn().getEntityType(), context.getAspectName());
                    return context.toBuilder()
                        .retentionPolicy(Optional.of(retentionPolicy))
                        .build();
                  } else {
                    return context;
                  }
                })
            .filter(
                context ->
                    context.getRetentionPolicy().isPresent()
                        && !context.getRetentionPolicy().get().data().isEmpty())
            .collect(Collectors.toList());

    applyRetention(opContext, withDefaults);
  }

  /**
   * Batch variant of {@link #applyRetentionWithPolicyDefaults} intended for the post-commit drain
   * path. Applies each pair's DELETE with per-pair failure isolation where supported by the storage
   * backend (see {@code EbeanRetentionService} for the per-context transaction implementation).
   * Returns the keys that were durably committed — callers should clear only those keys from the
   * buffer.
   *
   * <p>The drainer passes a single {@code List<RetentionBatchEntry>}; each entry structurally pairs
   * a {@link RetentionKey} with its {@link RetentionContext}, so the same-size / same-index
   * invariant between keys and contexts is guaranteed by construction (not by a runtime check). The
   * service uses each entry's context for the DELETE and echoes back the original key at each
   * committed index. Cross-off in the drainer is by {@link RetentionKey} equals, which is explicit
   * per subtype — so a key subtype that carries routing metadata is matched with that metadata
   * intact (two requests for the same URN routed to different underlying databases do not
   * cross-clear).
   *
   * <p>Default implementation falls back to {@link #applyRetentionWithPolicyDefaults} and returns
   * the full input keys list — treating every pair as committed <b>with no per-pair failure
   * isolation</b>. On a backend using this default (e.g. Cassandra) a partial failure that does not
   * throw leaves those keys reported as committed, so the drainer clears them and they are silently
   * under-pruned until the next enqueue re-adds them. Storage-specific subclasses (see {@code
   * EbeanRetentionService}) override to apply each context in its own transaction (per-pair failure
   * isolation).
   *
   * <p>Empty-policy contexts' keys are returned as committed (no-op DELETEs) so their buffer keys
   * are cleared rather than retried forever.
   *
   * @param opContext operation context
   * @param entries pairs of (key, context) to apply retention for
   * @return the subset of keys whose corresponding context was durably committed (empty on
   *     full-batch failure — all keys stay for retry)
   */
  @Nonnull
  public List<RetentionKey> applyRetentionBatchWithPolicyDefaults(
      @Nonnull OperationContext opContext, @Nonnull List<RetentionBatchEntry> entries) {
    applyRetentionWithPolicyDefaults(
        opContext, entries.stream().map(RetentionBatchEntry::context).collect(Collectors.toList()));
    return entries.stream().map(RetentionBatchEntry::key).collect(Collectors.toList());
  }

  /**
   * Apply retention policies given the urn and aspect name and policies. This protected method
   * assumes that the policy is provided, however we likely need to fetch these from system
   * configuration.
   *
   * <p>Users of this should use {@link #applyRetentionWithPolicyDefaults(List<RetentionContext>)})
   *
   * @param retentionContexts Additional context that could be used to apply retention
   */
  protected abstract void applyRetention(
      @Nonnull OperationContext opContext, List<RetentionContext> retentionContexts);

  /**
   * Batch apply retention to all records that match the input entityName and aspectName
   *
   * @param opContext operation context for the current call; used by storage implementations to
   *     route raw-SQL statements to the correct underlying database
   * @param entityName Name of the entity to apply retention to. If null, applies to all entities
   * @param aspectName Name of the aspect to apply retention to. If null, applies to all aspects
   */
  public abstract void batchApplyRetention(
      @Nonnull OperationContext opContext,
      @Nullable String entityName,
      @Nullable String aspectName);

  /** Batch apply retention to all records within the start, end count */
  public abstract BulkApplyRetentionResult batchApplyRetentionEntities(
      @Nonnull BulkApplyRetentionArgs args);

  @Value
  @Builder(toBuilder = true)
  public static class RetentionContext {
    @Nonnull Urn urn;
    @Nonnull String aspectName;
    @Builder.Default Optional<Retention> retentionPolicy = Optional.empty();
    @Builder.Default Optional<Long> maxVersion = Optional.empty();
  }
}
