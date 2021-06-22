package com.linkedin.metadata.usage.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.exception.ESQueryException;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.usage.UsageService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.common.WindowDuration;
import com.linkedin.usage.UsageAggregation;
import com.linkedin.usage.UsageAggregationMetrics;
import com.linkedin.usage.UserUsageCounts;
import com.linkedin.usage.UserUsageCountsArray;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ElasticUsageService implements UsageService {
    private static final String USAGE_STATS_BASE_INDEX_NAME = "usageStats_v1";

    // ElasticSearch defaults to a size of 10. We need to set size to a large number
    // to avoid this restriction.
    // See https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-from-size.html.
    private static final int ELASTIC_FETCH_ALL_SIZE = 10000;

    private final RestHighLevelClient elasticClient;
    private final IndexConvention indexConvention;
    private final BulkProcessor bulkProcessor;

    public ElasticUsageService(RestHighLevelClient elasticClient, IndexConvention indexConvention,
                      int bulkRequestsLimit, int bulkFlushPeriod, int numRetries, long retryInterval) {
        this.elasticClient = elasticClient;
        this.indexConvention = indexConvention;
        this.bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) -> elasticClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                BulkListener.getInstance())
                .setBulkActions(bulkRequestsLimit)
                .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(retryInterval), numRetries))
                .build();
    }
    @Override
    public void configure() {
        try {
            new IndexBuilder(elasticClient,
                    indexConvention.getIndexName(USAGE_STATS_BASE_INDEX_NAME),
                    this.getMappings(),
                    SettingsBuilder.getSettings()).buildIndex();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Object> getMappings() {
        Map<String, Object> mappings = new HashMap<>();

        Map<String, Object> dateType = ImmutableMap.<String, Object>builder().put("type", "date").put("format", "epoch_millis").build();
        mappings.put("bucket", dateType);
        mappings.put("bucket_end", dateType);

        Map<String, Object> textType = ImmutableMap.<String, Object>builder().put("type", "keyword").build();
        mappings.put("duration", textType);
        mappings.put("resource", textType);

        return ImmutableMap.of("properties", mappings);
    }

    @Override
    public void upsertDocument(@Nonnull String document, @Nonnull String docId) {
        final String indexName = indexConvention.getIndexName(USAGE_STATS_BASE_INDEX_NAME);
        final IndexRequest indexRequest = new IndexRequest(indexName).id(docId).source(document, XContentType.JSON);
        final UpdateRequest updateRequest = new UpdateRequest(indexName, docId).doc(document, XContentType.JSON)
                .detectNoop(false)
                .upsert(indexRequest);
        bulkProcessor.add(updateRequest);
    }

    @Nonnull
    @Override
    public List<UsageAggregation> query(@Nonnull String resource, @Nonnull WindowDuration duration, Long startTime, Long endTime, Integer maxBuckets) {
        final BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
        finalQuery.must(QueryBuilders.matchQuery("resource", resource));
        finalQuery.must(QueryBuilders.matchQuery("duration", duration.name()));
        if (startTime != null) {
            finalQuery.must(QueryBuilders.rangeQuery("bucket").gte(startTime));
        }
        if (endTime != null) {
            finalQuery.must(QueryBuilders.rangeQuery("bucket_end").lte(endTime));
        }
        // TODO handle "latest N buckets" style queries

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(finalQuery);
        searchSourceBuilder.sort(new FieldSortBuilder("bucket").order(SortOrder.DESC));
        if (maxBuckets != null) {
           searchSourceBuilder.size(maxBuckets);
        } else {
            searchSourceBuilder.size(ELASTIC_FETCH_ALL_SIZE);
        }

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        log.debug("Search request is: " + searchRequest.toString());

        try {
            final SearchResponse searchResponse = elasticClient.search(searchRequest, RequestOptions.DEFAULT);
            final SearchHits hits = searchResponse.getHits();

            return Arrays.stream(hits.getHits())
                    .map(ElasticUsageService::parseDocument)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Search query failed:" + e.getMessage());
            throw new ESQueryException("Search query failed:", e);
        }
    }

    @Nonnull
    static UsageAggregation parseDocument(@Nonnull SearchHit doc) {
        try {
            Map<String, Object> docFields = doc.getSourceAsMap();
            UsageAggregation agg = new UsageAggregation();
            UsageAggregationMetrics metrics = new UsageAggregationMetrics();

            agg.setBucket(Long.valueOf(((Number) docFields.get("bucket")).longValue()));
            agg.setDuration(WindowDuration.valueOf((String) docFields.get("duration")));
            agg.setResource(Urn.createFromString((String) docFields.get("resource")));
            agg.setMetrics(metrics);

            metrics.setUnique_user_count((Integer) docFields.get("metrics.unique_user_count"));
            if (docFields.containsKey("metrics.users")) {
                UserUsageCountsArray users = new UserUsageCountsArray();
                List<Map<String, Object>> docUsers = (List<Map<String, Object>>) docFields.get("metrics.users");
                for (Map<String, Object> map : docUsers) {
                    UserUsageCounts userUsage = new UserUsageCounts();
                    userUsage.setUser(Urn.createFromString((String) map.get("user")));
                    userUsage.setCount((Integer) map.get("count"));
                    users.add(userUsage);
                }
                metrics.setUsers(users);
            }

            metrics.setTotal_sql_queries((Integer) docFields.get("metrics.top_sql_queries"));
            if (docFields.containsKey("metrics.top_sql_queries")) {
                StringArray queries = new StringArray();
                List<String> docQueries = (List<String>) docFields.get("metrics.top_sql_queries");
                queries.addAll(docQueries);
                metrics.setTop_sql_queries(queries);
            }

            return agg;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
