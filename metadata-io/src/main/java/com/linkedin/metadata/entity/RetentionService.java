package com.linkedin.metadata.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.key.DataHubRetentionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.retention.DataHubRetentionInfo;
import com.linkedin.retention.Retention;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;


/**
 * Service coupled with an entity service to handle retention
 */
public interface RetentionService {
  String ALL = "*";

  EntityService getEntityService();

  /**
   * Fetch retention policies given the entityName and aspectName
   * Uses the entity service to fetch the latest retention policies set for the input entity and aspect
   *
   * @param entityName Name of the entity
   * @param aspectName Name of the aspect
   * @return retention policies to apply to the input entity and aspect
   */
  default List<Retention> getRetention(@Nonnull String entityName, @Nonnull String aspectName) {
    // Prioritized list of retention keys to fetch
    List<Urn> retentionUrns =
        ImmutableList.of(new DataHubRetentionKey().setEntityName(entityName).setAspectName(aspectName),
            new DataHubRetentionKey().setEntityName(entityName).setAspectName(ALL),
            new DataHubRetentionKey().setEntityName(ALL).setAspectName(aspectName),
            new DataHubRetentionKey().setEntityName(ALL).setAspectName(ALL))
            .stream()
            .map(key -> EntityKeyUtils.convertEntityKeyToUrn(key, "dataHubRetention"))
            .collect(Collectors.toList());
    Map<Urn, List<RecordTemplate>> fetchedAspects =
        getEntityService().getLatestAspects(new HashSet<>(retentionUrns), ImmutableSet.of("dataHubRetentionInfo"));
    // Find the first retention info that is set among the prioritized list of retention keys above
    Optional<DataHubRetentionInfo> retentionInfo = retentionUrns.stream()
        .map(fetchedAspects::get)
        .filter(retentionList -> retentionList != null && !retentionList.isEmpty())
        .map(retentionList -> (DataHubRetentionInfo) retentionList.get(0))
        .findFirst();
    return retentionInfo.<List<Retention>>map(DataHubRetentionInfo::getRetentionPolicies).orElse(
        Collections.emptyList());
  }

  void applyRetention(@Nonnull Urn urn, @Nonnull String aspectName,
      Optional<RetentionContext> context);

  @Value
  class RetentionContext {
    Optional<Long> maxVersion;
  }
}
