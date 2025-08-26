package com.linkedin.gms.factory.telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.telemetry.MixpanelConfiguration;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class MixpanelApiFactory {
  // DataHub's custom endpoints
  private static final String DATAHUB_EVENTS_ENDPOINT = "https://track.datahubproject.io/mp/track";
  private static final String DATAHUB_ENGAGE_ENDPOINT = "https://track.datahubproject.io/mp/engage";

  @Autowired private ConfigurationProvider configurationProvider;

  @Bean(name = "mixpanelApi")
  @Nonnull
  @ConditionalOnProperty(name = "telemetry.mixpanel.enabled", havingValue = "true")
  protected MixpanelAPI getInstance() {

    MixpanelConfiguration config = configurationProvider.getTelemetry().getMixpanel();
    log.info(
        "Creating Mixpanel API with configuration: enabled={}, useStandardEndpoints={}",
        config.isEnabled(),
        config.isUseStandardEndpoints());

    if (!config.isEnabled()) {
      log.warn("Mixpanel is not enabled, but API bean is being created");
    }

    // Use custom endpoints if not using standard endpoints
    if (!config.isUseStandardEndpoints()) {
      log.info("Using DataHub custom endpoints for Mixpanel API");
      return new MixpanelAPI(DATAHUB_EVENTS_ENDPOINT, DATAHUB_ENGAGE_ENDPOINT);
    }

    log.info("Using standard Mixpanel endpoints");
    return new MixpanelAPI();
  }
}
