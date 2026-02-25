package com.linkedin.metadata.dataHubUsage;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;

import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageService;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchRequest;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchResponse;
import com.linkedin.metadata.datahubusage.InternalUsageEventResult;
import com.linkedin.metadata.datahubusage.event.EventSource;
import com.linkedin.metadata.datahubusage.event.LoginSource;
import com.linkedin.metadata.datahubusage.event.UsageEventResult;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

@Slf4j
public class DataHubUsageServiceImpl implements DataHubUsageService {

  private final SearchClientShim<?> elasticClient;
  private final IndexConvention indexConvention;

  public DataHubUsageServiceImpl(
      SearchClientShim<?> elasticClient, IndexConvention indexConvention) {
    this.elasticClient = elasticClient;
    this.indexConvention = indexConvention;
  }

  @Override
  public String getUsageIndexName() {
    return indexConvention.getIndexName(DATAHUB_USAGE_EVENT_INDEX);
  }

  /** Searches the DataHub Usage index for backend tracing events */
  @Override
  public ExternalAuditEventsSearchResponse externalAuditEventsSearch(
      OperationContext opContext,
      ExternalAuditEventsSearchRequest externalAuditEventsSearchRequest) {
    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery();
    filterQuery.filter(
        dateRangeQuery(
            externalAuditEventsSearchRequest.getStartTime(),
            externalAuditEventsSearchRequest.getEndTime()));
    if (CollectionUtils.isNotEmpty(externalAuditEventsSearchRequest.getEventTypes())) {
      filterQuery.filter(eventTypeFilterQuery(externalAuditEventsSearchRequest.getEventTypes()));
    }
    if (CollectionUtils.isNotEmpty(externalAuditEventsSearchRequest.getAspectTypes())) {
      filterQuery.filter(aspectNameFilterQuery(externalAuditEventsSearchRequest.getAspectTypes()));
    }
    if (CollectionUtils.isNotEmpty(externalAuditEventsSearchRequest.getEntityTypes())) {
      filterQuery.filter(entityTypeFilterQuery(externalAuditEventsSearchRequest.getEntityTypes()));
    }
    if (CollectionUtils.isNotEmpty(externalAuditEventsSearchRequest.getActorUrns())) {
      filterQuery.filter(actorUrnFilterQuery(externalAuditEventsSearchRequest.getActorUrns()));
    }
    filterQuery.filter(getBackendOnlyEvents());

    SearchRequest searchRequest = new SearchRequest(getUsageIndexName());
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(externalAuditEventsSearchRequest.getSize());
    searchSourceBuilder.query(filterQuery);

    searchSourceBuilder.sort(
        SortBuilders.fieldSort(DataHubUsageEventConstants.TIMESTAMP).order(SortOrder.DESC));
    searchSourceBuilder.sort(
        SortBuilders.fieldSort(DataHubUsageEventConstants.TYPE).order(SortOrder.ASC));
    searchSourceBuilder.sort(
        SortBuilders.fieldSort(keywordField(DataHubUsageEventConstants.ACTOR_URN))
            .order(SortOrder.ASC));

    if (StringUtils.isNotBlank(externalAuditEventsSearchRequest.getScrollId())) {
      searchSourceBuilder.searchAfter(
          SearchAfterWrapper.fromScrollId(externalAuditEventsSearchRequest.getScrollId())
              .getSort());
    }

    searchRequest.source(searchSourceBuilder);
    SearchResponse response = executeAndExtractDocuments(searchRequest);
    return mapExternalAuditEventsSearchResponse(
        opContext, response, externalAuditEventsSearchRequest);
  }

  private ExternalAuditEventsSearchResponse mapExternalAuditEventsSearchResponse(
      OperationContext opContext,
      SearchResponse searchResponse,
      ExternalAuditEventsSearchRequest analyticsSearchRequest) {
    ExternalAuditEventsSearchResponse.ExternalAuditEventsSearchResponseBuilder response =
        ExternalAuditEventsSearchResponse.builder();
    SearchHits searchHits = searchResponse.getHits();
    response.count(searchHits.getHits().length);
    response.total((int) searchHits.getTotalHits().value);
    response.nextScrollId(
        SearchAfterWrapper.nextScrollId(searchHits.getHits(), analyticsSearchRequest.getSize()));
    List<UsageEventResult> usageEventResults = new ArrayList<>();
    for (SearchHit searchHit : searchHits.getHits()) {
      UsageEventResult usageEventResult = mapUsageEventResult(searchHit, opContext);
      usageEventResults.add(usageEventResult);
    }
    response.usageEvents(usageEventResults);
    return response.build();
  }

  @Nonnull
  private static UsageEventResult mapUsageEventResult(
      SearchHit searchHit, OperationContext opContext) {
    InternalUsageEventResult.InternalUsageEventResultBuilder usageEventResultBuilder =
        InternalUsageEventResult.builder();
    Map<String, Object> searchResultMap = searchHit.getSourceAsMap();
    usageEventResultBuilder.rawUsageEvent(new LinkedHashMap<>(searchResultMap));
    if (searchResultMap.get(DataHubUsageEventConstants.TYPE) instanceof String) {
      usageEventResultBuilder.eventType(
          (String) searchResultMap.get(DataHubUsageEventConstants.TYPE));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.ACTOR_URN) instanceof String) {
      usageEventResultBuilder.actorUrn(
          (String) searchResultMap.get(DataHubUsageEventConstants.ACTOR_URN));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.TIMESTAMP) instanceof Long) {
      usageEventResultBuilder.timestamp(
          (Long) searchResultMap.get(DataHubUsageEventConstants.TIMESTAMP));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.SOURCE_IP) instanceof String) {
      usageEventResultBuilder.sourceIP(
          (String) searchResultMap.get(DataHubUsageEventConstants.SOURCE_IP));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.EVENT_SOURCE) instanceof String) {
      usageEventResultBuilder.eventSource(
          EventSource.getSource(
              (String) searchResultMap.get(DataHubUsageEventConstants.EVENT_SOURCE)));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.LOGIN_SOURCE) instanceof String) {
      usageEventResultBuilder.loginSource(
          LoginSource.getSource(
              (String) searchResultMap.get(DataHubUsageEventConstants.LOGIN_SOURCE)));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.ENTITY_TYPE) instanceof String) {
      usageEventResultBuilder.entityType(
          (String) searchResultMap.get(DataHubUsageEventConstants.ENTITY_TYPE));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.ENTITY_URN) instanceof String) {
      usageEventResultBuilder.entityUrn(
          (String) searchResultMap.get(DataHubUsageEventConstants.ENTITY_URN));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.ASPECT_NAME) instanceof String) {
      usageEventResultBuilder.aspectName(
          (String) searchResultMap.get(DataHubUsageEventConstants.ASPECT_NAME));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.TRACE_ID) instanceof String) {
      usageEventResultBuilder.telemetryTraceId(
          (String) searchResultMap.get(DataHubUsageEventConstants.TRACE_ID));
    }
    if (searchResultMap.get(DataHubUsageEventConstants.USER_AGENT) instanceof String) {
      usageEventResultBuilder.userAgent(
          (String) searchResultMap.get(DataHubUsageEventConstants.USER_AGENT));
    }
    InternalUsageEventResult usageEventResult = usageEventResultBuilder.build();
    // Identify subtype automatically so that serialization happens correctly
    return opContext.getObjectMapper().convertValue(usageEventResult, UsageEventResult.class);
  }

  private SearchResponse executeAndExtractDocuments(SearchRequest searchRequest) {
    try {
      return elasticClient.search(searchRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error(String.format("Search query failed: %s", e.getMessage()));
      throw new RuntimeException("Search query failed:", e);
    }
  }

  private QueryBuilder dateRangeQuery(long start, long end) {
    if (start < 0) {
      start = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
    }
    if (end <= 0) {
      end = Instant.now().toEpochMilli();
    }
    return QueryBuilders.rangeQuery(DataHubUsageEventConstants.TIMESTAMP).gte(start).lt(end);
  }

  private QueryBuilder eventTypeFilterQuery(List<String> eventTypes) {
    return QueryBuilders.termsQuery(DataHubUsageEventConstants.TYPE, eventTypes);
  }

  private QueryBuilder aspectNameFilterQuery(List<String> aspectNames) {
    return QueryBuilders.termsQuery(
        keywordField(DataHubUsageEventConstants.ASPECT_NAME), aspectNames);
  }

  private QueryBuilder entityTypeFilterQuery(List<String> entityTypes) {
    return QueryBuilders.termsQuery(
        keywordField(DataHubUsageEventConstants.ENTITY_TYPE), entityTypes);
  }

  private QueryBuilder actorUrnFilterQuery(List<String> actorUrns) {
    return QueryBuilders.termsQuery(keywordField(DataHubUsageEventConstants.ACTOR_URN), actorUrns);
  }

  private QueryBuilder getBackendOnlyEvents() {
    return QueryBuilders.termQuery(
        keywordField(DataHubUsageEventConstants.USAGE_SOURCE),
        DataHubUsageEventConstants.BACKEND_SOURCE);
  }

  private static String keywordField(String field) {
    return field + ".keyword";
  }
}
