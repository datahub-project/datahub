package com.linkedin.datahub.upgrade.system.cdc.debezium;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.cdc.CDCSourceSetup;
import com.linkedin.metadata.config.CDCSourceConfiguration;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Debezium-specific implementation of CDC setup. Handles the creation and configuration of Debezium
 * connectors.
 */
@Slf4j
public class DebeziumCDCSourceSetup extends CDCSourceSetup {

  /** The CDC type identifier for Debezium implementations. */
  public static final String DEBEZIUM_TYPE = "debezium";

  private final DebeziumConfiguration _debeziumConfig;
  private final List<UpgradeStep> _steps;

  public DebeziumCDCSourceSetup(
      OperationContext opContext,
      CDCSourceConfiguration cdcSourceConfig,
      EbeanConfiguration ebeanConfig,
      KafkaConfiguration kafkaConfig,
      KafkaProperties kafkaProperties) {
    super(opContext, cdcSourceConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    this._debeziumConfig = (DebeziumConfiguration) cdcSourceConfig.getCdcImplConfig();

    if (_debeziumConfig == null) {
      throw new IllegalArgumentException("Debezium configuration is required");
    }

    _steps =
        ImmutableList.of(
            new WaitForDebeziumReadyStep(opContext, _debeziumConfig, kafkaConfig, kafkaProperties),
            new ConfigureDebeziumConnectorStep(
                opContext, _debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties));

    log.info(
        "Created DebeziumCDCSetup with {} steps for connector '{}'",
        _steps.size(),
        _debeziumConfig.getName());
  }

  @Override
  public String id() {
    return "DebeziumCDCSetup-"
        + (_debeziumConfig.getName() != null ? _debeziumConfig.getName() : "default");
  }

  @Override
  protected List<UpgradeStep> createSteps() {
    return _steps;
  }

  @Override
  public boolean canRun() {
    if (!super.canRun()) {
      return false;
    }

    // Verify required Debezium connector.class property is present
    Object connectorClass =
        _debeziumConfig.getConfig() != null
            ? _debeziumConfig.getConfig().get("connector.class")
            : null;
    if (connectorClass == null) {
      log.warn("Debezium configuration missing required 'connector.class' property");
      return false;
    }

    return true;
  }

  @Override
  public String getCdcType() {
    return DEBEZIUM_TYPE;
  }
}
