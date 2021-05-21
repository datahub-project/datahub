package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeResult;
import io.ebean.EbeanServer;
import java.util.function.BiConsumer;


/**
 * Removes the Aspect Table on Upgrade Failure.
 */
public class RemoveAspectTableStep implements UpgradeCleanupStep {

  private final EbeanServer _server;

  public RemoveAspectTableStep(final EbeanServer server) {
    _server = server;
  }

  @Override
  public String id() {
    return "RemoveAspectTableStep";
  }

  @Override
  public BiConsumer<UpgradeContext, UpgradeResult> executable() {
    return (context, result) -> {
      if (UpgradeResult.Result.FAILED.equals(result.result())) {
        _server.execute(_server.createSqlUpdate("DROP TABLE metadata_aspect_v2"));
      }
    };
  }
}
