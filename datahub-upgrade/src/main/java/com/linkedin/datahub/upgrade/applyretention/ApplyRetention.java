package com.linkedin.datahub.upgrade.applyretention;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.common.steps.GMSQualificationStep;
import com.linkedin.metadata.entity.RetentionService;
import io.ebean.EbeanServer;
import java.util.ArrayList;
import java.util.List;


public class ApplyRetention implements Upgrade {
  public static final String BATCH_SIZE_ARG_NAME = "batchSize";
  public static final String BATCH_DELAY_MS_ARG_NAME = "batchDelayMs";

  private final List<UpgradeStep> _steps;

  public ApplyRetention(final EbeanServer server, final RetentionService retentionService) {
    _steps = buildSteps(server, retentionService);
  }

  @Override
  public String id() {
    return "ApplyRetention";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  private List<UpgradeStep> buildSteps(final EbeanServer server, final RetentionService retentionService) {
    final List<UpgradeStep> steps = new ArrayList<>();
    steps.add(new GMSQualificationStep(ImmutableMap.of("retention", "true")));
    steps.add(new ApplyRetentionStep(server, retentionService));
    return steps;
  }

  @Override
  public List<UpgradeCleanupStep> cleanupSteps() {
    return ImmutableList.of();
  }
}
