package com.linkedin.datahub.upgrade.system.kafka;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

@Slf4j
public class KafkaNonBlockingSetup implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public KafkaNonBlockingSetup(
      OperationContext opContext,
      KafkaConfiguration kafkaConfiguration,
      KafkaProperties kafkaProperties) {
    _steps =
        ImmutableList.of(
            new ConfluentSchemaRegistryCleanupPolicyStep(
                opContext, kafkaConfiguration, kafkaProperties));
  }

  @Override
  public String id() {
    return "KafkaNonBlockingSetup";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
