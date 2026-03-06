package com.linkedin.datahub.upgrade.system.retention;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * Non-blocking system upgrade that ingests retention policies from YAML (boot/retention.yaml and
 * plugin path). Runs only during system-update; retention configuration is no longer part of GMS
 * bootstrap.
 */
@Slf4j
public class IngestRetentionPolicies implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> stepList;

  public IngestRetentionPolicies(
      @Nonnull final RetentionService<?> retentionService,
      @Nonnull final EntityService<?> entityService,
      final boolean enabled,
      final boolean applyAfterIngest,
      @Nonnull final String pluginPath) {
    stepList =
        ImmutableList.of(
            new IngestRetentionPoliciesUpgradeStep(
                enabled,
                retentionService,
                entityService,
                applyAfterIngest,
                pluginPath,
                new PathMatchingResourcePatternResolver()));
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return stepList;
  }
}
