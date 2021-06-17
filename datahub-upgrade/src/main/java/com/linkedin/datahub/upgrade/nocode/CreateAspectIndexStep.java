package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import io.ebean.EbeanServer;
import java.util.function.Function;


public class CreateAspectIndexStep implements UpgradeStep {

  private final EbeanServer _server;

  public CreateAspectIndexStep(final EbeanServer server) {
    _server = server;
  }

  @Override
  public String id() {
    return "CreateAspectIndexStep";
  }

  @Override
  public int retryCount() {
    return 1;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        _server.execute(_server.createSqlUpdate(
            "CREATE INDEX aspectName ON metadata_aspect_v2 (aspect)"));
      } catch (Exception e) {
        context.report().addLine(String.format("Failed to create aspect index for metadata_aspect_v2: %s", e.toString()));
        return new DefaultUpgradeStepResult(
            id(),
            UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
