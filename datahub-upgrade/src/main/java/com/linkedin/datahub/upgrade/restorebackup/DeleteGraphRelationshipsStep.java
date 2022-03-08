package com.linkedin.datahub.upgrade.restorebackup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.neo4j.Neo4jGraphService;
import java.util.function.Function;


public class DeleteGraphRelationshipsStep implements UpgradeStep {

  private final String deletePattern = ".*";

  private final GraphService _graphClient;

  public DeleteGraphRelationshipsStep(final GraphService graphClient) {
    _graphClient = graphClient;
  }

  @Override
  public String id() {
    return "DeleteGraphRelationshipsStep";
  }

  @Override
  public int retryCount() {
    return 1;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        ((Neo4jGraphService) _graphClient).removeNodesMatchingLabel(deletePattern);
      } catch (Exception e) {
        context.report().addLine(String.format("Failed to delete legacy data from graph: %s", e.toString()));
        return new DefaultUpgradeStepResult(
            id(),
            UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
