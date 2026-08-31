package com.linkedin.datahub.upgrade.system.dataprocessinstances;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackfillDataProcessInstances implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public BackfillDataProcessInstances(
      OperationContext opContext,
      EntityService<?> entityService,
      ElasticSearchService elasticSearchService,
      SearchClientShim<?> restHighLevelClient,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize,
      Integer batchDelayMs,
      Integer totalDays,
      Integer windowDays) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new BackfillDataProcessInstancesHasRunEventsStep(
                  opContext,
                  entityService,
                  elasticSearchService,
                  restHighLevelClient,
                  reprocessEnabled,
                  batchSize,
                  batchDelayMs,
                  totalDays,
                  windowDays));
    } else {
      log.info(
          "{} is disabled (systemUpdate.processInstanceHasRunEvents.enabled=false); no steps registered.",
          id());
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillDataProcessInstances";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
