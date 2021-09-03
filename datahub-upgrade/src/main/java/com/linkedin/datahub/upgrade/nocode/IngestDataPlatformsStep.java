package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.resources.dataplatform.utils.DataPlatformsUtil;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Map;
import java.util.function.Function;


public class IngestDataPlatformsStep implements UpgradeStep {

  private final EntityService _entityService;

  public IngestDataPlatformsStep(final EntityService entityService) {
    _entityService = entityService;
  }

  @Override
  public String id() {
    return "IngestDataPlatformsStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      context.report().addLine("Preparing to ingest DataPlatforms...");

      Map<DataPlatformUrn, DataPlatformInfo> urnToInfo = DataPlatformsUtil.getDataPlatformInfoMap();

      context.report().addLine(String.format("Found %s DataPlatforms", urnToInfo.keySet().size()));

      for (final Map.Entry<DataPlatformUrn, DataPlatformInfo> entry : urnToInfo.entrySet()) {
        AuditStamp auditStamp;
        try {
          auditStamp = new AuditStamp().setActor(Urn.createFromString("urn:li:principal:system")).setTime(
              Clock.systemUTC().millis());
        } catch (URISyntaxException e) {
          throw new RuntimeException("Failed to create Actor Urn");
        }

        _entityService.ingestAspect(
            entry.getKey(),
            PegasusUtils.getAspectNameFromSchema(entry.getValue().schema()),
            entry.getValue(),
            auditStamp
        );
      }

      context.report().addLine(String.format("Successfully ingested %s DataPlatforms.", urnToInfo.keySet().size()));
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
