package com.linkedin.gms.factory.telemetry;

import static com.linkedin.gms.factory.telemetry.TelemetryUtils.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.version.GitVersion;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
public class DailyReport {
  private final OperationContext systemOperationContext;
  private final RestHighLevelClient _elasticClient;
  private final ConfigurationProvider _configurationProvider;
  private final EntityService<?> _entityService;
  private final GitVersion _gitVersion;

  private static final String MIXPANEL_TOKEN = "5ee83d940754d63cacbf7d34daa6f44a";
  private MixpanelAPI mixpanel;
  private MessageBuilder mixpanelBuilder;

  public DailyReport(
      @Nonnull OperationContext systemOperationContext,
      RestHighLevelClient elasticClient,
      ConfigurationProvider configurationProvider,
      EntityService<?> entityService,
      GitVersion gitVersion) {
    this.systemOperationContext = systemOperationContext;
    this._elasticClient = elasticClient;
    this._configurationProvider = configurationProvider;
    this._entityService = entityService;
    this._gitVersion = gitVersion;
    try {
      String clientId = getClientId(systemOperationContext, entityService);

      // initialize MixPanel instance and message builder
      mixpanel =
          new MixpanelAPI(
              "https://track.datahubproject.io/mp/track",
              "https://track.datahubproject.io/mp/engage");
      mixpanelBuilder = new MessageBuilder(MIXPANEL_TOKEN);

      // set user-level properties
      JSONObject props = new JSONObject();
      props.put("java_version", System.getProperty("java.version"));
      props.put("os", System.getProperty("os.name"));
      props.put("server_version", _gitVersion.getVersion());
      JSONObject update = mixpanelBuilder.set(clientId, props);
      try {
        mixpanel.sendMessage(update);
      } catch (IOException e) {
        log.error("Error sending telemetry profile:", e);
      }
    } catch (Exception e) {
      log.warn("Unable to set up telemetry.", e);
    }
  }

  // statistics to send daily
  @Scheduled(fixedDelay = 24 * 60 * 60 * 1000)
  public void dailyReport() {
    AnalyticsService analyticsService =
        new AnalyticsService(
            _elasticClient, systemOperationContext.getSearchContext().getIndexConvention());

    DateTime endDate = DateTime.now();
    DateTime yesterday = endDate.minusDays(1);
    DateTime lastWeek = endDate.minusWeeks(1);
    DateTime lastMonth = endDate.minusMonths(1);

    DateRange dayRange =
        new DateRange(String.valueOf(yesterday.getMillis()), String.valueOf(endDate.getMillis()));
    DateRange weekRange =
        new DateRange(String.valueOf(lastWeek.getMillis()), String.valueOf(endDate.getMillis()));
    DateRange monthRange =
        new DateRange(String.valueOf(lastMonth.getMillis()), String.valueOf(endDate.getMillis()));

    int dailyActiveUsers =
        analyticsService.getHighlights(
            analyticsService.getUsageIndexName(),
            Optional.of(dayRange),
            ImmutableMap.of(),
            ImmutableMap.of(),
            Optional.of("browserId"));
    int weeklyActiveUsers =
        analyticsService.getHighlights(
            analyticsService.getUsageIndexName(),
            Optional.of(weekRange),
            ImmutableMap.of(),
            ImmutableMap.of(),
            Optional.of("browserId"));
    int monthlyActiveUsers =
        analyticsService.getHighlights(
            analyticsService.getUsageIndexName(),
            Optional.of(monthRange),
            ImmutableMap.of(),
            ImmutableMap.of(),
            Optional.of("browserId"));

    // floor to nearest power of 10
    dailyActiveUsers =
        dailyActiveUsers <= 0
            ? 0
            : (int) Math.pow(2, (int) (Math.log(dailyActiveUsers) / Math.log(2)));
    weeklyActiveUsers =
        weeklyActiveUsers <= 0
            ? 0
            : (int) Math.pow(2, (int) (Math.log(weeklyActiveUsers) / Math.log(2)));
    monthlyActiveUsers =
        monthlyActiveUsers <= 0
            ? 0
            : (int) Math.pow(2, (int) (Math.log(monthlyActiveUsers) / Math.log(2)));

    // set user-level properties
    JSONObject report = new JSONObject();
    report.put("dau", dailyActiveUsers);
    report.put("wau", weeklyActiveUsers);
    report.put("mau", monthlyActiveUsers);
    report.put("server_type", _configurationProvider.getDatahub().getServerType());
    report.put("server_version", _gitVersion.getVersion());

    ping("service-daily", report);
  }

  public void ping(String eventName, JSONObject properties) {
    if (mixpanel == null || mixpanelBuilder == null) {
      log.error("Unable to send telemetry metrics, MixPanel API not initialized");
      return;
    }

    try {
      JSONObject event =
          mixpanelBuilder.event(
              getClientId(systemOperationContext, _entityService), eventName, properties);
      mixpanel.sendMessage(event);
    } catch (IOException e) {
      log.error("Error reporting telemetry:", e);
    }
  }
}
