package com.linkedin.datahub.upgrade.system.vianodes;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReindexDataJobViaNodesCLLStep implements UpgradeStep {

  public static final String UPGRADE_ID = "via-node-cll-reindex-datajob-v2";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final EntityService<?> entityService;
  private final Integer batchSize;

  public ReindexDataJobViaNodesCLLStep(EntityService<?> entityService, Integer batchSize) {
    this.entityService = entityService;
    this.batchSize = batchSize;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      RestoreIndicesArgs args =
          new RestoreIndicesArgs()
              .aspectName(DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)
              .urnLike("urn:li:" + DATA_JOB_ENTITY_NAME + ":%")
              .batchSize(batchSize);

      entityService
          .streamRestoreIndices(args, x -> context.report().addLine((String) x))
          .forEach(
              result -> {
                context.report().addLine("Rows migrated: " + result.rowsMigrated);
                context.report().addLine("Rows ignored: " + result.ignored);
              });

      BootstrapStep.setUpgradeResult(UPGRADE_ID_URN, entityService);
      context.report().addLine("State updated: " + UPGRADE_ID_URN);

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return false;
  }

  @Override
  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variable SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    boolean previouslyRun =
        entityService.exists(UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    boolean envFlagRecommendsSkip =
        Boolean.parseBoolean(System.getenv("SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT"));
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    if (envFlagRecommendsSkip) {
      log.info("Environment variable SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT is set to true. Skipping.");
    }
    return (previouslyRun || envFlagRecommendsSkip);
  }
}
