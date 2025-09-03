package com.linkedin.datahub.upgrade.system.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.config.CDCSourceConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Abstract base class for CDC setup implementations. Provides common functionality and structure
 * for different CDC types.
 */
@Slf4j
public abstract class CDCSourceSetup implements BlockingSystemUpgrade {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final OperationContext _opContext;
  protected final CDCSourceConfiguration _cdcSourceConfig;

  // TODO: EbeanConfiguration is not available if using cassandra - need alternative for
  // non-relational stores
  protected final EbeanConfiguration _ebeanConfig;
  protected final KafkaConfiguration _kafkaConfig;
  protected final KafkaProperties _kafkaProperties;

  protected CDCSourceSetup(
      OperationContext opContext,
      CDCSourceConfiguration cdcSourceConfig,
      EbeanConfiguration ebeanConfig,
      KafkaConfiguration kafkaConfig,
      KafkaProperties kafkaProperties) {
    this._opContext = opContext;
    this._cdcSourceConfig = cdcSourceConfig;
    this._ebeanConfig = ebeanConfig;
    this._kafkaConfig = kafkaConfig;
    this._kafkaProperties = kafkaProperties;

    // Log configuration for debugging CDC setup issues
    try {
      log.info(
          "CDC source configuration: {}",
          OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(cdcSourceConfig));
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
    if (_cdcSourceConfig == null) {
      log.warn("CDC source configuration is null");
      return false;
    }

    if (!_cdcSourceConfig.isEnabled()) {
      log.info("CDC processing is disabled");
      return false;
    }

    if (!_cdcSourceConfig.isConfigureSource()) {
      log.info("CDC source configuration is disabled");
      return false;
    }
    String cdcType = _cdcSourceConfig.getType();
    if (cdcType == null || cdcType.trim().isEmpty()) {
      log.warn("CDC type not specified");
      return false;
    }
    if (_cdcSourceConfig.getCdcImplConfig() == null) {
      log.warn("CDC implementation configuration is null");
      return false;
    }
    if (_ebeanConfig == null) {
      log.warn("Ebean configuration is null");
      return false;
    }

    return true;
  }

  /** Gets the CDC type for this setup (e.g., "debezium", "maxwell"). */
  public String getCdcType() {
    return _cdcSourceConfig != null ? _cdcSourceConfig.getType() : null;
  }
}
