package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.cdc.CDCSourceSetup;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * Spring configuration for CDC setup. Automatically discovers and injects enabled CDC source
 * implementations based on application.yaml configuration.
 *
 * <p>Only one CDC implementation should be active at a time. If multiple implementations are
 * enabled, a warning will be logged.
 */
@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
@Slf4j
public class CDCSetupConfig {

  /**
   * Creates the CDC setup upgrade bean from available CDC source implementations.
   *
   * @param cdcSourceSetups List of conditionally-enabled CDC source implementations
   * @return The first CDC setup implementation, or null if none are enabled
   */
  @Order(Integer.MAX_VALUE)
  @Bean(name = "cdcSetup")
  public BlockingSystemUpgrade cdcSetup(
      @Autowired(required = false) List<CDCSourceSetup> cdcSourceSetups) {
    if (cdcSourceSetups == null || cdcSourceSetups.isEmpty()) {
      log.info("No CDC source setups found - CDC configuration is disabled or not configured");
      return null;
    }

    if (cdcSourceSetups.size() > 1) {
      log.warn(
          "Multiple CDC source setups detected ({}). Only one should be enabled at a time. Using first: {}. Found: {}",
          cdcSourceSetups.size(),
          cdcSourceSetups.get(0).id(),
          cdcSourceSetups.stream().map(CDCSourceSetup::id).toList());
    } else {
      log.info("CDC source setup enabled: {}", cdcSourceSetups.get(0).id());
    }

    return cdcSourceSetups.get(0);
  }
}
