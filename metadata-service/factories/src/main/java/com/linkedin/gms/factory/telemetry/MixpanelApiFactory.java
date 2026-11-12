package com.linkedin.gms.factory.telemetry;

import com.datahub.telemetry.MixpanelEventSender;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class MixpanelApiFactory {

  @Bean(name = "mixpanelApi")
  @ConditionalOnProperty("telemetry.enabledServer")
  @Scope("singleton")
  protected MixpanelAPI getInstance() throws Exception {
    return new MixpanelAPI(
        TelemetryUtils.DATAHUB_EVENTS_ENDPOINT, TelemetryUtils.DATAHUB_ENGAGE_ENDPOINT);
  }

  /** Spring infers {@code close()} as the destroy method, which stops the sender's worker. */
  @Bean(name = "mixpanelEventSender")
  @Nonnull
  @ConditionalOnProperty("telemetry.enabledServer")
  protected MixpanelEventSender mixpanelEventSender(
      @Qualifier("mixpanelApi") @Nonnull final MixpanelAPI mixpanelAPI,
      @Nullable final MetricUtils metricUtils) {
    return new MixpanelEventSender("tracking", mixpanelAPI, metricUtils);
  }
}
