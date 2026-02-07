package com.linkedin.gms.factory.telemetry;

import static com.linkedin.gms.factory.telemetry.TelemetryUtils.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.NamedBar;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  /** Entity types to report for metadata analytics */
  private static final List<EntityType> REPORTING_ENTITY_TYPES =
      Arrays.asList(
          // Data Assets
          EntityType.DATASET,
          EntityType.DASHBOARD,
          EntityType.CHART,
          EntityType.DATA_JOB,
          EntityType.DATA_FLOW,
          EntityType.NOTEBOOK,
          EntityType.DOCUMENT,

          // Users & Organization
          EntityType.CORP_USER,
          EntityType.CORP_GROUP,
          EntityType.APPLICATION,

          // Governance & Metadata
          EntityType.TAG,
          EntityType.GLOSSARY_TERM,
          EntityType.DOMAIN,
          EntityType.DATA_PRODUCT,
          EntityType.DATA_CONTRACT,

          // Quality & Operations
          EntityType.ASSERTION,
          EntityType.TEST,
          EntityType.INCIDENT,
          EntityType.CONTAINER);

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

    // Add metadata analytics
    try {
      addMetadataAnalytics(analyticsService, report);
    } catch (Exception e) {
      log.warn("Failed to collect metadata analytics: {}", e.getMessage());
      // Continue with report even if metadata collection fails
    }

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

  /**
   * Adds metadata analytics to the daily report: total assets, platform statistics, and entity type
   * distribution.
   *
   * @param analyticsService the analytics service for querying metadata
   * @param report the JSON report object to populate
   */
  private void addMetadataAnalytics(AnalyticsService analyticsService, JSONObject report) {
    // Collect entity counts by type
    Map<String, Integer> entityCounts = collectEntityCounts(analyticsService);

    // Calculate total assets - use long to avoid overflow
    long totalAssets = entityCounts.values().stream().mapToLong(Integer::longValue).sum();
    report.put("total_assets", anonymizeToBucket((int) Math.min(totalAssets, Integer.MAX_VALUE)));

    // Add entity type counts as flattened properties
    for (Map.Entry<String, Integer> entry : entityCounts.entrySet()) {
      String propertyName = "entity_count_" + entry.getKey().toLowerCase();
      report.put(propertyName, anonymizeToBucket(entry.getValue()));
    }

    // Collect platform statistics
    collectPlatformStatistics(analyticsService, report);
  }

  /**
   * Collects entity counts for reported entity types. Only includes entity types with non-zero
   * counts in the result.
   *
   * @param analyticsService the analytics service for querying
   * @return map of entity type name to count
   */
  private Map<String, Integer> collectEntityCounts(AnalyticsService analyticsService) {
    Map<String, Integer> counts = new HashMap<>();

    // Iterate only tracked entity types (not all)
    for (EntityType entityType : REPORTING_ENTITY_TYPES) {
      try {
        String index = analyticsService.getEntityIndexName(entityType);
        int count =
            analyticsService.getHighlights(
                index,
                Optional.empty(), // No date range
                ImmutableMap.of(), // No filters
                ImmutableMap.of("removed", ImmutableList.of("true")), // Exclude removed
                Optional.empty()); // No unique field

        // Only include entity types with non-zero counts to keep report concise
        if (count > 0) {
          counts.put(entityType.name(), count);
        }
      } catch (Exception e) {
        log.debug("Failed to count entities for type {}: {}", entityType, e.getMessage());
        // Don't add to counts if query failed - keeps report cleaner
      }
    }

    return counts;
  }

  /**
   * Collects platform statistics: platform count and distribution of assets per platform.
   *
   * @param analyticsService the analytics service for querying
   * @param report the JSON report object to populate
   */
  private void collectPlatformStatistics(AnalyticsService analyticsService, JSONObject report) {
    try {
      // Query for platform distribution
      List<NamedBar> platformBars =
          analyticsService.getBarChart(
              analyticsService.getAllEntityIndexName(),
              Optional.empty(),
              ImmutableList.of("platform.keyword"),
              Collections.emptyMap(),
              ImmutableMap.of("removed", ImmutableList.of("true")),
              Optional.empty(),
              false); // Don't show missing

      // Add platform count
      report.put("platform_count", platformBars.size());

      // Add platform distribution as flattened properties
      for (NamedBar bar : platformBars) {
        String platformName = bar.getName();

        // Add null/bounds check
        if (bar.getSegments() == null || bar.getSegments().isEmpty()) {
          log.debug("Platform {} has no segments, skipping", platformName);
          continue;
        }

        int count = bar.getSegments().get(0).getValue();

        // Anonymize platform name for privacy
        String anonymizedName = anonymizePlatformName(platformName);
        String propertyName = "platform_" + anonymizedName;
        report.put(propertyName, anonymizeToBucket(count));
      }

    } catch (Exception e) {
      log.warn("Failed to collect platform statistics: {}", e.getMessage());
      report.put("platform_count", 0);
    }
  }

  /**
   * Anonymizes platform name by truncating to 10 chars and appending hash. Example:
   * "snowflake_production_finance" -> "snowflake_p_a1b2c3d4"
   *
   * @param platformName the raw platform name
   * @return anonymized platform name
   */
  private String anonymizePlatformName(String platformName) {
    if (platformName == null || platformName.isEmpty()) {
      return "unknown";
    }

    if (platformName.length() <= 10) {
      return platformName;
    }

    String prefix = platformName.substring(0, 10);
    String hash = Integer.toHexString(platformName.hashCode());
    return prefix + "_" + hash;
  }

  /**
   * Anonymize count into buckets for privacy. Buckets: 0-10, 10-100, 100-1K, 1K-10K, 10K-100K,
   * 100K-1M, 1M+
   *
   * @param count the raw count
   * @return the bucket string
   */
  private String anonymizeToBucket(int count) {
    if (count == 0) {
      return "0";
    } else if (count <= 10) {
      return "0-10";
    } else if (count <= 100) {
      return "10-100";
    } else if (count <= 1000) {
      return "100-1K";
    } else if (count <= 10000) {
      return "1K-10K";
    } else if (count <= 100000) {
      return "10K-100K";
    } else if (count <= 1000000) {
      return "100K-1M";
    } else {
      return "1M+";
    }
  }
}
