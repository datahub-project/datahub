package com.linkedin.datahub.upgrade.buildindices;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.shared.ElasticSearchIndexed;

import com.linkedin.metadata.version.GitVersion;
import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.tasks.TaskInfo;


@Slf4j
@RequiredArgsConstructor
public class BuildIndicesStep implements UpgradeStep {

  private final List<ElasticSearchIndexed> _services;
  private final GitVersion _gitVersion;
  private final RestHighLevelClient _searchClient;

  @Override
  public String id() {
    return "BuildIndicesStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        for (ElasticSearchIndexed service : _services) {
          service.reindexAll();
        }
      } catch (Exception e) {
        log.error("BuildIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
