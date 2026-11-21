package com.linkedin.gms.factory.telemetry;

import static com.linkedin.gms.factory.telemetry.TelemetryUtils.DATAHUB_ENGAGE_ENDPOINT;
import static com.linkedin.gms.factory.telemetry.TelemetryUtils.DATAHUB_EVENTS_ENDPOINT;

import com.datahub.telemetry.MixpanelEventSender;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.version.GitVersion;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@Configuration
@EnableScheduling
public class ScheduledAnalyticsFactory {

  @Bean
  @ConditionalOnProperty("telemetry.enabledServer")
  public DailyReport dailyReport(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      ConfigurationProvider configurationProvider,
      EntityService<?> entityService,
      GitVersion gitVersion,
      @Nullable MetricUtils metricUtils) {
    // The daily report always goes to DataHub's own collector with DataHub's token, on a client
    // of its own rather than the one TrackingService uses. DailyReport.close() stops the sender
    // when the bean is destroyed.
    MixpanelEventSender sender =
        new MixpanelEventSender(
            "daily-report",
            new MixpanelAPI(DATAHUB_EVENTS_ENDPOINT, DATAHUB_ENGAGE_ENDPOINT),
            metricUtils);
    return new DailyReport(
        systemOperationContext, configurationProvider, entityService, gitVersion, sender);
  }
}
