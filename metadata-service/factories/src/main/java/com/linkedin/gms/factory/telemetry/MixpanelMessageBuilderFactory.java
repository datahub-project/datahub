package com.linkedin.gms.factory.telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.telemetry.MixpanelConfiguration;
import com.mixpanel.mixpanelapi.MessageBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Slf4j
@Configuration
public class MixpanelMessageBuilderFactory {
  private static final String MIXPANEL_TOKEN = "5ee83d940754d63cacbf7d34daa6f44a";

  @Autowired private ConfigurationProvider configurationProvider;

  @Bean(name = "mixpanelMessageBuilder")
  @ConditionalOnProperty("telemetry.enabledServer")
  @Scope("singleton")
  protected MessageBuilder getInstance() throws Exception {
    // Log the environment variables
    log.info(
        "Environment variables: DATAHUB_TELEMETRY_ENABLED={}, TELEMETRY_ENABLED_SERVER={}, MIXPANEL_ENABLED={}, MIXPANEL_TOKEN={}, MIXPANEL_USE_STANDARD_ENDPOINTS={}",
        System.getenv("DATAHUB_TELEMETRY_ENABLED"),
        System.getenv("TELEMETRY_ENABLED_SERVER"),
        System.getenv("MIXPANEL_ENABLED"),
        System.getenv("MIXPANEL_TOKEN"),
        System.getenv("MIXPANEL_USE_STANDARD_ENDPOINTS"));

    MixpanelConfiguration mixpanelConfiguration =
        configurationProvider.getTelemetry().getMixpanel();

    // Log the Mixpanel configuration
    log.info("Mixpanel configuration: {}", mixpanelConfiguration);
    if (mixpanelConfiguration != null) {
      log.info("Mixpanel enabled: {}", mixpanelConfiguration.isEnabled());
      log.info("Mixpanel token: {}", mixpanelConfiguration.getToken());
    } else {
      log.info("Mixpanel configuration is null, this is not expected");
      throw new RuntimeException("Failed to load mixpanel configuration");
    }
    // Create and return the MessageBuilder
    return new MessageBuilder(mixpanelConfiguration.getToken());
  }
}
