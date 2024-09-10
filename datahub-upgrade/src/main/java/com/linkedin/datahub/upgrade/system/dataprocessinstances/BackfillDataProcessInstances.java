package com.linkedin.datahub.upgrade.system.dataprocessinstances;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.opensearch.client.RestHighLevelClient;

public class BackfillDataProcessInstances implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public BackfillDataProcessInstances(
      OperationContext opContext,
      EntityService<?> entityService,
      ElasticSearchService elasticSearchService,
      RestHighLevelClient restHighLevelClient,
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
