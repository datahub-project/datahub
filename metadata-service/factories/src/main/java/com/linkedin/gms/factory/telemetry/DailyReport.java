package com.linkedin.gms.factory.telemetry;

import static com.linkedin.gms.factory.telemetry.TelemetryUtils.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
public class DailyReport {
  private final OperationContext systemOperationContext;
  private final SearchClientShim<?> _elasticClient;
  private final ConfigurationProvider _configurationProvider;
  private final EntityService<?> _entityService;
  private final GitVersion _gitVersion;

  private static final String MIXPANEL_TOKEN = "5ee83d940754d63cacbf7d34daa6f44a";

  /** SubType value used to identify service accounts in CorpUser entities. */
  private static final String SERVICE_ACCOUNT_SUB_TYPE = "SERVICE_ACCOUNT";

  private MixpanelAPI mixpanel;
  private MessageBuilder mixpanelBuilder;

  public DailyReport(
      @Nonnull OperationContext systemOperationContext,
      SearchClientShim<?> elasticClient,
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

    // set user-level properties (all counts anonymized to nearest power of 2)
    JSONObject report = new JSONObject();
    report.put("dau", anonymizeCount(dailyActiveUsers));
    report.put("wau", anonymizeCount(weeklyActiveUsers));
    report.put("mau", anonymizeCount(monthlyActiveUsers));
    report.put("server_type", _configurationProvider.getDatahub().getServerType());
    report.put("server_version", _gitVersion.getVersion());

    // Add total user count (anonymized to nearest power of 2)
    int totalUserCount = getTotalUserCount();
    report.put("total_user_count", anonymizeCount(totalUserCount));

    // Add total service account count (anonymized to nearest power of 2)
    int totalServiceAccountCount = getServiceAccountCount();
    report.put("total_service_account_count", anonymizeCount(totalServiceAccountCount));

    ping("service-daily", report);
  }

  /**
   * Anonymizes a count by rounding down to the nearest power of 2. This provides privacy while
   * still giving useful order-of-magnitude information.
   *
   * <p>Examples: 1 -> 1, 2 -> 2, 3 -> 2, 4 -> 4, 5 -> 4, 7 -> 4, 8 -> 8, 100 -> 64, 1000 -> 512
   *
   * @param count the raw count
   * @return the anonymized count (0 if count <= 0, otherwise nearest power of 2)
   */
  // Visible for testing
  int anonymizeCount(int count) {
    return count <= 0 ? 0 : (int) Math.pow(2, (int) (Math.log(count) / Math.log(2)));
  }

  /**
   * Counts the total number of users (CorpUser entities) in the system.
   *
   * @return the count of users, or 0 if an error occurs
   */
  private int getTotalUserCount() {
    try {
      String corpUserIndex =
          systemOperationContext
              .getSearchContext()
              .getIndexConvention()
              .getEntityIndexName(Constants.CORP_USER_ENTITY_NAME);

      SearchRequest searchRequest = new SearchRequest(corpUserIndex);
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(0); // We only need the count
      searchSourceBuilder.query(QueryBuilders.matchAllQuery());
      searchSourceBuilder.trackTotalHits(true);
      searchRequest.source(searchSourceBuilder);

      SearchResponse searchResponse = _elasticClient.search(searchRequest, RequestOptions.DEFAULT);
      return (int) searchResponse.getHits().getTotalHits().value;
    } catch (Exception e) {
      log.warn("Failed to count users for telemetry: {}", e.getMessage());
      return 0;
    }
  }

  /**
   * Counts the number of service accounts in the system. Service accounts are CorpUser entities
   * with a SubTypes aspect containing "SERVICE_ACCOUNT" in typeNames.
   *
   * @return the count of service accounts, or 0 if an error occurs
   */
  private int getServiceAccountCount() {
    try {
      String corpUserIndex =
          systemOperationContext
              .getSearchContext()
              .getIndexConvention()
              .getEntityIndexName(Constants.CORP_USER_ENTITY_NAME);

      SearchRequest searchRequest = new SearchRequest(corpUserIndex);
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(0); // We only need the count
      searchSourceBuilder.query(QueryBuilders.termQuery("typeNames", SERVICE_ACCOUNT_SUB_TYPE));
      searchSourceBuilder.trackTotalHits(true);
      searchRequest.source(searchSourceBuilder);

      SearchResponse searchResponse = _elasticClient.search(searchRequest, RequestOptions.DEFAULT);
      return (int) searchResponse.getHits().getTotalHits().value;
    } catch (Exception e) {
      log.warn("Failed to count service accounts for telemetry: {}", e.getMessage());
      return 0;
    }
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
