package com.linkedin.datahub.upgrade.nocodecleanup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.graph.Neo4jGraphClient;
import java.util.function.Function;


// Do we need SQL-tech specific migration paths?
public class DeleteLegacyGraphRelationshipsStep implements UpgradeStep<Void> {

  private final String deletePattern = "com.linkedin.*";

  private final Neo4jGraphClient _graphClient;

  public DeleteLegacyGraphRelationshipsStep(final Neo4jGraphClient graphClient) {
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
  public Function<UpgradeContext, UpgradeStepResult<Void>> executable() {
    return (context) -> {
      try {
        _graphClient.removeNodesMatchingLabel(deletePattern);
      } catch (Exception e) {
        return new DefaultUpgradeStepResult<>(
            id(),
            UpgradeStepResult.Result.FAILED,
            String.format("Failed to delete legacy data from graph: %s", e.toString()));
      }
      return new DefaultUpgradeStepResult<>(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
