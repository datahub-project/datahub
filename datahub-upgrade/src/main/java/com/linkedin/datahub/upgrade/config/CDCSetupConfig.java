package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.cdc.CDCSourceSetup;
import jakarta.annotation.PostConstruct;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for CDC setup discovery.
 *
 * <p>CDC implementations (e.g. {@code DebeziumCDCSourceSetup}) are {@code @Component} beans that
 * already implement {@code BlockingSystemUpgrade}. Do <b>not</b> re-export them as a second {@code
 * BlockingSystemUpgrade} bean — {@code SystemUpdate} injects {@code List<BlockingSystemUpgrade>} by
 * bean definition, so a duplicate registration runs Wait/Configure Debezium steps twice and the
 * second create fails with Kafka Connect HTTP 409.
 */
@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
@Slf4j
public class CDCSetupConfig {

  @Autowired(required = false)
  private List<CDCSourceSetup> cdcSourceSetups;

  @PostConstruct
  void logCdcSetups() {
    logCdcSetups(cdcSourceSetups);
  }

  /**
   * Validates and logs discovered CDC source setups. Package-visible for unit tests.
   *
   * @param setups discovered CDC source implementations, or null/empty when CDC is disabled
   */
  void logCdcSetups(List<CDCSourceSetup> setups) {
    if (setups == null || setups.isEmpty()) {
      log.info("No CDC source setups found - CDC configuration is disabled or not configured");
      return;
    }

    if (setups.size() > 1) {
      log.warn(
          "Multiple CDC source setups detected ({}). Only one should be enabled at a time. Using"
              + " component discovery order. Found: {}",
          setups.size(),
          setups.stream().map(CDCSourceSetup::id).toList());
    } else {
      log.info("CDC source setup enabled: {}", setups.get(0).id());
    }
  }
}
