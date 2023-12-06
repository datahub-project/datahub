package com.linkedin.metadata.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.transactions.AspectsBatch;
import com.linkedin.metadata.key.DataHubRetentionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;

/**
 * Service coupled with an {@link EntityServiceImpl} to handle aspect record retention.
 *
 * <p>TODO: This class is abstract with storage-specific implementations. It'd be nice to pull
 * storage and retention concerns apart, let (into {@link AspectDao}) deal with storage, and merge
 * all retention concerns into a single class.
 */
public abstract class RetentionService {
  protected static final String ALL = "*";

  protected abstract EntityService getEntityService();

  /**
   * Fetch retention policies given the entityName and aspectName Uses the entity service to fetch
   * the latest retention policies set for the input entity and aspect
   *
   * @param entityName Name of the entity
   * @param aspectName Name of the aspect
   * @return retention policies to apply to the input entity and aspect
   */
  public Retention getRetention(@Nonnull String entityName, @Nonnull String aspectName) {
    // Prioritized list of retention keys to fetch
    List<Urn> retentionUrns = getRetentionKeys(entityName, aspectName);
    Map<Urn, List<RecordTemplate>> fetchedAspects =
        getEntityService()
            .getLatestAspects(
                new HashSet<>(retentionUrns), ImmutableSet.of(Constants.DATAHUB_RETENTION_ASPECT));
    // Find the first retention info that is set among the prioritized list of retention keys above
    Optional<DataHubRetentionConfig> retentionInfo =
        retentionUrns.stream()
            .flatMap(
                urn ->
                    fetchedAspects.getOrDefault(urn, Collections.emptyList()).stream()
                        .filter(aspect -> aspect instanceof DataHubRetentionConfig))
            .map(retention -> (DataHubRetentionConfig) retention)
            .findFirst();
    return retentionInfo.map(DataHubRetentionConfig::getRetention).orElse(new Retention());
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
    AspectsBatch batch = buildAspectsBatch(List.of(keyProposal, aspectProposal));

    return getEntityService().ingestProposal(batch, auditStamp, false).stream()
        .anyMatch(IngestResult::isSqlCommitted);
  }

  protected abstract AspectsBatch buildAspectsBatch(List<MetadataChangeProposal> mcps);

  /**
   * Delete the retention policy set for given entity and aspect.
   *
   * @param entityName Entity name to apply policy to. If null, set as "*", meaning it will delete
   *     the default policy for any entities without specified policy
   * @param aspectName Aspect name to apply policy to. If null, set as "*", meaning it will delete
   *     the default policy for any aspects without specified policy
   */
  public void deleteRetention(@Nullable String entityName, @Nullable String aspectName) {
    DataHubRetentionKey retentionKey = new DataHubRetentionKey();
    retentionKey.setEntityName(entityName != null ? entityName : ALL);
    retentionKey.setAspectName(aspectName != null ? aspectName : ALL);
    Urn retentionUrn =
        EntityKeyUtils.convertEntityKeyToUrn(retentionKey, Constants.DATAHUB_RETENTION_ENTITY);
    getEntityService().deleteUrn(retentionUrn);
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
  public void applyRetentionWithPolicyDefaults(@Nonnull List<RetentionContext> retentionContexts) {
    List<RetentionContext> withDefaults =
        retentionContexts.stream()
            .map(
                context -> {
                  if (context.getRetentionPolicy().isEmpty()) {
                    Retention retentionPolicy =
                        getRetention(context.getUrn().getEntityType(), context.getAspectName());
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

    applyRetention(withDefaults);
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
  protected abstract void applyRetention(List<RetentionContext> retentionContexts);

  /**
   * Batch apply retention to all records that match the input entityName and aspectName
   *
   * @param entityName Name of the entity to apply retention to. If null, applies to all entities
   * @param aspectName Name of the aspect to apply retention to. If null, applies to all aspects
   */
  public abstract void batchApplyRetention(
      @Nullable String entityName, @Nullable String aspectName);

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
