package com.linkedin.datahub.upgrade.restorebackup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import io.ebean.Database;
import java.util.function.Function;

/** Optional step for removing Aspect V2 table. */
public class ClearAspectV2TableStep implements UpgradeStep {

  private final Database _server;

  public ClearAspectV2TableStep(final Database server) {
    _server = server;
  }

  @Override
  public String id() {
    return "ClearAspectV2TableStep";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      _server.find(EbeanAspectV2.class).delete();
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
