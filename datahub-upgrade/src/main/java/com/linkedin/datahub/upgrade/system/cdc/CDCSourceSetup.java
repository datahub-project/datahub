package com.linkedin.datahub.upgrade.system.cdc;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.config.CDCSourceConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract base class for CDC setup implementations. Provides common functionality and structure
 * for different CDC types.
 */
@Slf4j
public abstract class CDCSourceSetup implements BlockingSystemUpgrade {

  OperationContext opContext;
  CDCSourceConfiguration cdcSourceConfig;

  protected CDCSourceSetup(OperationContext opContext, CDCSourceConfiguration cdcSourceConfig) {
    this.opContext = opContext;
    this.cdcSourceConfig = cdcSourceConfig;
    // Log configuration for debugging CDC setup issues
    try {
      log.info(
          "CDC source configuration: {}",
          opContext
              .getObjectMapper()
              .writerWithDefaultPrettyPrinter()
              .writeValueAsString(cdcSourceConfig));
    } catch (Exception e) {
      log.warn("Failed to serialize CDC source configuration for logging: {}", e.getMessage());
    }
  }

  /**
   * Creates the list of upgrade steps for this CDC implementation. Subclasses must implement this
   * to provide their specific steps.
   */
  protected abstract List<UpgradeStep> createSteps();

  @Override
  public List<UpgradeStep> steps() {
    try {
      List<UpgradeStep> steps = createSteps();
      log.info("Created {} steps for CDC setup type '{}'", steps.size(), getCdcType());
      return steps;
    } catch (Exception e) {
      log.error(
          "Failed to create steps for CDC setup type '{}': {}", getCdcType(), e.getMessage(), e);
      return ImmutableList.of(); // Graceful degradation - prevents upgrade system failure
    }
  }

  /**
   * Validates that this CDC setup can run with the current configuration. Subclasses can override
   * this for type-specific validation.
   */
  public boolean canRun() {
    if (cdcSourceConfig == null) {
      log.warn("CDC source configuration is null");
      return false;
    }

    if (!cdcSourceConfig.isEnabled()) {
      log.info("CDC processing is disabled");
      return false;
    }

    if (!cdcSourceConfig.isConfigureSource()) {
      log.info("CDC source configuration is disabled");
      return false;
    }
    String cdcType = cdcSourceConfig.getType();
    if (cdcType == null || cdcType.trim().isEmpty()) {
      log.warn("CDC type not specified");
      return false;
    }
    if (cdcSourceConfig.getCdcImplConfig() == null) {
      log.warn("CDC implementation configuration is null");
      return false;
    }
    return true;
  }

  /** Gets the CDC type for this setup (e.g., "debezium", "maxwell"). */
  public String getCdcType() {
    return cdcSourceConfig != null ? cdcSourceConfig.getType() : null;
  }
}
