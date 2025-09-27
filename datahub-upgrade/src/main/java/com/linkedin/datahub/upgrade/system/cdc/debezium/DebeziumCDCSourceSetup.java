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

  private final DebeziumConfiguration debeziumConfig;
  private final List<UpgradeStep> steps;

  public DebeziumCDCSourceSetup(
      OperationContext opContext,
      CDCSourceConfiguration cdcSourceConfig,
      EbeanConfiguration ebeanConfig,
      KafkaConfiguration kafkaConfig,
      KafkaProperties kafkaProperties) {
    super(opContext, cdcSourceConfig, ebeanConfig, kafkaConfig, kafkaProperties);

    this.debeziumConfig = (DebeziumConfiguration) cdcSourceConfig.getCdcImplConfig();

    if (debeziumConfig == null) {
      throw new IllegalArgumentException("Debezium configuration is required");
    }

    steps =
        ImmutableList.of(
            new WaitForDebeziumReadyStep(opContext, debeziumConfig, kafkaConfig, kafkaProperties),
            new ConfigureDebeziumConnectorStep(
                opContext, debeziumConfig, ebeanConfig, kafkaConfig, kafkaProperties));

    log.info(
        "Created DebeziumCDCSetup with {} steps for connector '{}'",
        steps.size(),
        debeziumConfig.getName());
  }

  @Override
  public String id() {
    return "DebeziumCDCSetup";
  }

  @Override
  protected List<UpgradeStep> createSteps() {
    return steps;
  }

  @Override
  public boolean canRun() {
    if (!super.canRun()) {
      return false;
    }

    // Verify required Debezium connector.class property is present
    Object connectorClass =
        debeziumConfig.getConfig() != null
            ? debeziumConfig.getConfig().get("connector.class")
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
