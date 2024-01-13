package com.linkedin.datahub.upgrade.nocodecleanup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.neo4j.Neo4jGraphService;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

// Do we need SQL-tech specific migration paths?
@Slf4j
public class DeleteLegacyGraphRelationshipsStep implements UpgradeStep {

  private final String deletePattern = "com.linkedin.*";

  private final GraphService _graphClient;

  public DeleteLegacyGraphRelationshipsStep(final GraphService graphClient) {
    _graphClient = graphClient;
  }

  @Override
  public String id() {
    return "DeleteLegacyGraphRelationshipStep";
  }

  @Override
  public int retryCount() {
    return 1;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        if (_graphClient instanceof Neo4jGraphService) {
          ((Neo4jGraphService) _graphClient).removeNodesMatchingLabel(deletePattern);
        } else {
          log.info("Skipping DeleteLegacyGraphRelationshipsStep for non-neo4j graphClient.");
        }
      } catch (Exception e) {
        context.report().addLine("Failed to delete legacy data from graph", e);
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
