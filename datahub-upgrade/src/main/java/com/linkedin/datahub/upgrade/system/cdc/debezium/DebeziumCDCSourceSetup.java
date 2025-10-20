package com.linkedin.datahub.upgrade.system.cdc.debezium;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.cdc.CDCSourceSetup;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CDCSourceConfiguration;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

/**
 * Debezium-specific implementation of CDC setup. Handles the creation and configuration of Debezium
 * connectors.
 */
@Slf4j
@Component
@ConditionalOnExpression(
    "'${mclProcessing.cdcSource.configureSource:false}' == 'true' && '${mclProcessing.cdcSource.type:}' == 'debezium-kafka-connector'")
public class DebeziumCDCSourceSetup extends CDCSourceSetup {

  /** The CDC type identifier for Debezium implementations. */
  public static final String DEBEZIUM_TYPE = "debezium";

  private final DebeziumConfiguration debeziumConfig;
  private final List<UpgradeStep> steps;

  protected final OperationContext opContext;

  // TODO: EbeanConfiguration is not available if using cassandra - need alternative for
  // non-relational stores
  protected final EbeanConfiguration ebeanConfig;
  protected final KafkaConfiguration kafkaConfig;
  protected final KafkaProperties kafkaProperties;

  public DebeziumCDCSourceSetup(
      OperationContext opContext,
      ConfigurationProvider configurationProvider,
      KafkaProperties kafkaProperties) {
    super(opContext, configurationProvider.getMclProcessing().getCdcSource());
    this.opContext = opContext;
    this.ebeanConfig = configurationProvider.getEbean();
    this.kafkaConfig = configurationProvider.getKafka();
    this.kafkaProperties = kafkaProperties;

    CDCSourceConfiguration cdcSourceConfig =
        configurationProvider.getMclProcessing().getCdcSource();

    if (cdcSourceConfig == null) {
      throw new IllegalArgumentException("CDC configuration is required");
    }
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

    if (ebeanConfig == null) {
      log.warn("Ebean configuration is null");
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
