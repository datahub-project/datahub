package com.linkedin.metadata.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataHubRetentionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.retention.DataHubRetentionInfo;
import com.linkedin.retention.Retention;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.Value;


/**
 * Service coupled with an entity service to handle retention
 */
public abstract class RetentionService {
  protected static final String ALL = "*";
  protected static final String DATAHUB_RETENTION_ENTITY = "dataHubRetention";
  protected static final String DATAHUB_RETENTION_ASPECT = "dataHubRetentionInfo";
  protected static final String DATAHUB_RETENTION_KEY_ASPECT = "dataHubRetentionKey";

  protected abstract EntityService getEntityService();

  /**
   * Check whether or not any retention policies have been ingested
   */
  public boolean hasRetention() {
    return getEntityService().listUrns(DATAHUB_RETENTION_ENTITY, 0, 1).getTotal() > 0;
  }

  /**
   * Fetch retention policies given the entityName and aspectName
   * Uses the entity service to fetch the latest retention policies set for the input entity and aspect
   *
   * @param entityName Name of the entity
   * @param aspectName Name of the aspect
   * @return retention policies to apply to the input entity and aspect
   */
  public List<Retention> getRetention(@Nonnull String entityName, @Nonnull String aspectName) {
    // Prioritized list of retention keys to fetch
    List<Urn> retentionUrns = getRetentionKeys(entityName, aspectName);
    Map<Urn, List<RecordTemplate>> fetchedAspects =
        getEntityService().getLatestAspects(new HashSet<>(retentionUrns), ImmutableSet.of(DATAHUB_RETENTION_ASPECT));
    // Find the first retention info that is set among the prioritized list of retention keys above
    Optional<DataHubRetentionInfo> retentionInfo = retentionUrns.stream()
        .flatMap(urn -> fetchedAspects.getOrDefault(urn, Collections.emptyList())
            .stream()
            .filter(aspect -> aspect instanceof DataHubRetentionInfo))
        .map(retention -> (DataHubRetentionInfo) retention)
        .findFirst();
    return retentionInfo.<List<Retention>>map(DataHubRetentionInfo::getRetentionPolicies).orElse(
        Collections.emptyList());
  }

  // Get list of datahub retention keys that match the input entity name and aspect name
  protected List<Urn> getRetentionKeys(@Nonnull String entityName, @Nonnull String aspectName) {
    return ImmutableList.of(new DataHubRetentionKey().setEntityName(entityName).setAspectName(aspectName),
        new DataHubRetentionKey().setEntityName(entityName).setAspectName(ALL),
        new DataHubRetentionKey().setEntityName(ALL).setAspectName(aspectName),
        new DataHubRetentionKey().setEntityName(ALL).setAspectName(ALL))
        .stream()
        .map(key -> EntityKeyUtils.convertEntityKeyToUrn(key, DATAHUB_RETENTION_ENTITY))
        .collect(Collectors.toList());
  }

  /**
   * Set retention policy for given entity and aspect. If entity or aspect names are null, the policy is set as default
   *
   * @param entityName Entity name to apply policy to. If null, set as "*",
   *                   meaning it will be the default for any entities without specified policy
   * @param aspectName Aspect name to apply policy to. If null, set as "*",
   *                   meaning it will be the default for any aspects without specified policy
   * @param retentionInfo Retention policy
   */
  @SneakyThrows
  public void setRetention(@Nullable String entityName, @Nullable String aspectName,
      @Nonnull DataHubRetentionInfo retentionInfo) {
    validateRetentionInfo(retentionInfo);
    DataHubRetentionKey retentionKey = new DataHubRetentionKey();
    retentionKey.setEntityName(entityName != null ? entityName : ALL);
    retentionKey.setAspectName(aspectName != null ? aspectName : ALL);
    Urn retentionUrn = EntityKeyUtils.convertEntityKeyToUrn(retentionKey, DATAHUB_RETENTION_ENTITY);
    MetadataChangeProposal keyProposal = new MetadataChangeProposal();
    GenericAspect keyAspect = GenericAspectUtils.serializeAspect(retentionKey);
    keyProposal.setAspect(keyAspect);
    keyProposal.setAspectName(DATAHUB_RETENTION_KEY_ASPECT);
    keyProposal.setEntityType(DATAHUB_RETENTION_ENTITY);
    keyProposal.setChangeType(ChangeType.UPSERT);
    keyProposal.setEntityUrn(retentionUrn);
    AuditStamp auditStamp =
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
    getEntityService().ingestProposal(keyProposal, auditStamp);
    MetadataChangeProposal aspectProposal = keyProposal.clone();
    GenericAspect retentionAspect = GenericAspectUtils.serializeAspect(retentionInfo);
    aspectProposal.setAspect(retentionAspect);
    aspectProposal.setAspectName(DATAHUB_RETENTION_ASPECT);
    getEntityService().ingestProposal(aspectProposal, auditStamp);

    // Apply retention to affected records
    batchApplyRetention(entityName, aspectName);
  }

  /**
   * Delete the retention policy set for given entity and aspect.
   *
   * @param entityName Entity name to apply policy to. If null, set as "*",
   *                   meaning it will delete the default policy for any entities without specified policy
   * @param aspectName Aspect name to apply policy to. If null, set as "*",
   *                   meaning it will delete the default policy for any aspects without specified policy
   */
  public void deleteRetention(@Nullable String entityName, @Nullable String aspectName) {
    DataHubRetentionKey retentionKey = new DataHubRetentionKey();
    retentionKey.setEntityName(entityName != null ? entityName : ALL);
    retentionKey.setAspectName(aspectName != null ? aspectName : ALL);
    Urn retentionUrn = EntityKeyUtils.convertEntityKeyToUrn(retentionKey, DATAHUB_RETENTION_ENTITY);
    getEntityService().deleteUrn(retentionUrn);
  }

  private void validateRetentionInfo(DataHubRetentionInfo retentionInfo) {
    Set<String> retentionsSoFar = new HashSet<>();
    for (Retention retention : retentionInfo.getRetentionPolicies()) {
      if (retention.data().size() != 1) {
        throw new IllegalArgumentException("Exactly one retention policy should be set per element");
      }
      if (retention.hasIndefinite() && retentionInfo.getRetentionPolicies().size() > 1) {
        throw new IllegalArgumentException("Indefinite policy cannot be combined with any other policy");
      }
      String retentionType = retention.data().keySet().stream().findFirst().get();
      if (retentionsSoFar.contains(retentionType)) {
        throw new IllegalArgumentException("Type of policies in the list must be unique");
      }
      retentionsSoFar.add(retentionType);
      validateRetention(retention);
    }
  }

  private void validateRetention(Retention retention) {
    if (retention.hasVersion()) {
      if (retention.getVersion().getMaxVersions() <= 0) {
        throw new IllegalArgumentException("Invalid maxVersions: " + retention.getVersion().getMaxVersions());
      }
    }
    if (retention.hasTime()) {
      if (retention.getTime().getMaxAgeInSeconds() <= 0) {
        throw new IllegalArgumentException("Invalid maxAgeInSeconds: " + retention.getTime().getMaxAgeInSeconds());
      }
    }
  }

  /**
   * Apply retention policies given the urn and aspect name asynchronously
   *
   * @param urn Urn of the entity
   * @param aspectName Name of the aspect
   * @param context Additional context that could be used to apply retention
   */
  public void applyRetentionAsync(@Nonnull Urn urn, @Nonnull String aspectName, Optional<RetentionContext> context) {
    CompletableFuture.runAsync(() -> applyRetention(urn, aspectName, context));
  }

  /**
   * Apply retention policies given the urn and aspect name
   *
   * @param urn Urn of the entity
   * @param aspectName Name of the aspect
   * @param context Additional context that could be used to apply retention
   */
  public void applyRetention(@Nonnull Urn urn, @Nonnull String aspectName, Optional<RetentionContext> context) {
    applyRetention(urn, aspectName, getRetention(urn.getEntityType(), aspectName), context);
  }

  /**
   * Apply retention policies given the urn and aspect name and policies
   *
   * @param urn Urn of the entity
   * @param aspectName Name of the aspect
   * @param retentionPolicies Retention policies to apply
   * @param context Additional context that could be used to apply retention
   */
  public abstract void applyRetention(@Nonnull Urn urn, @Nonnull String aspectName, List<Retention> retentionPolicies,
      Optional<RetentionService.RetentionContext> context);

  /**
   * Batch apply retention to all records that match the input entityName and aspectName
   *
   * @param entityName Name of the entity to apply retention to. If null, applies to all entities
   * @param aspectName Name of the aspect to apply retention to. If null, applies to all aspects
   */
  public abstract void batchApplyRetention(@Nullable String entityName, @Nullable String aspectName);

  @Value
  public static class RetentionContext {
    Optional<Long> maxVersion;
  }
}
