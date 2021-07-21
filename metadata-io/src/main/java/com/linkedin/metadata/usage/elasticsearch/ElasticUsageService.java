package com.linkedin.metadata.usage.elasticsearch;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.exception.ESQueryException;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.usage.UsageService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.common.WindowDuration;
import com.linkedin.usage.FieldUsageCounts;
import com.linkedin.usage.FieldUsageCountsArray;
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
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class ElasticUsageService implements UsageService {
    private static final String USAGE_STATS_BASE_INDEX_NAME = "usageStats_v1";

    private static final String ES_KEY_BUCKET = "bucket";
    private static final String ES_KEY_BUCKET_END = "bucket_end";
    private static final String ES_KEY_DURATION = "duration";
    private static final String ES_KEY_RESOURCE = "resource";

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
        mappings.put(ES_KEY_BUCKET, dateType);
        mappings.put(ES_KEY_BUCKET_END, dateType);

        Map<String, Object> textType = ImmutableMap.<String, Object>builder().put("type", "keyword").build();
        mappings.put(ES_KEY_DURATION, textType);
        mappings.put(ES_KEY_RESOURCE, textType);

        return ImmutableMap.of("properties", mappings);
    }

    @Nonnull
    String constructDocId(@Nonnull UsageAggregation bucket) {
        return String.format("(%d,%s,%s)", bucket.getBucket(), bucket.getDuration(), bucket.getResource());
    }

    @Nonnull
    String constructDocument(@Nonnull UsageAggregation bucket) {
        ObjectNode document = JsonNodeFactory.instance.objectNode();
        document.set(ES_KEY_BUCKET, JsonNodeFactory.instance.numberNode(bucket.getBucket()));
        document.set(ES_KEY_DURATION, JsonNodeFactory.instance.textNode(bucket.getDuration().toString()));
        document.set(ES_KEY_BUCKET_END, JsonNodeFactory.instance.numberNode(bucket.getBucket() + windowDurationToMillis(bucket.getDuration())));
        document.set(ES_KEY_RESOURCE, JsonNodeFactory.instance.textNode(bucket.getResource().toString()));

        document.set("metrics.unique_user_count", JsonNodeFactory.instance.numberNode(bucket.getMetrics().getUniqueUserCount()));
        Optional.ofNullable(bucket.getMetrics().getUsers()).ifPresent(usersUsageCounts -> {
            ArrayNode users = JsonNodeFactory.instance.arrayNode();
            usersUsageCounts.forEach(userUsage -> {
                ObjectNode userDocument = JsonNodeFactory.instance.objectNode();
                if (userUsage.getUser() != null) {
                    userDocument.set("user", JsonNodeFactory.instance.textNode(userUsage.getUser().toString()));
                }
                userDocument.set("user_email", JsonNodeFactory.instance.textNode(userUsage.getUserEmail()));
                userDocument.set("count", JsonNodeFactory.instance.numberNode(userUsage.getCount()));
                users.add(userDocument);
            });
            document.set("metrics.users", users);
        });

        document.set("metrics.total_sql_queries", JsonNodeFactory.instance.numberNode(bucket.getMetrics().getTotalSqlQueries()));
        Optional.ofNullable(bucket.getMetrics().getTopSqlQueries()).ifPresent(top_sql_queries -> {
            ArrayNode sqlQueriesDocument = JsonNodeFactory.instance.arrayNode();
            top_sql_queries.forEach(sqlQueriesDocument::add);
            document.set("metrics.top_sql_queries", sqlQueriesDocument);
        });

        Optional.ofNullable(bucket.getMetrics().getFields()).ifPresent(fields -> {
            ArrayNode fieldsDocument = JsonNodeFactory.instance.arrayNode();
            fields.forEach(fieldUsage -> {
                ObjectNode fieldDocument = JsonNodeFactory.instance.objectNode();
                fieldDocument.set("field_name", JsonNodeFactory.instance.textNode(fieldUsage.getFieldName()));
                fieldDocument.set("count", JsonNodeFactory.instance.numberNode(fieldUsage.getCount()));
                fieldsDocument.add(fieldDocument);
            });
            document.set("metrics.fields", fieldsDocument);
        });
        return document.toString();
    }

    @Override
    public void upsertDocument(@Nonnull UsageAggregation bucket) {
        final String docId = this.constructDocId(bucket);
        final String document = this.constructDocument(bucket);

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
        finalQuery.must(QueryBuilders.matchQuery(ES_KEY_RESOURCE, resource));
        finalQuery.must(QueryBuilders.matchQuery(ES_KEY_DURATION, duration.name()));
        if (startTime != null) {
            finalQuery.must(QueryBuilders.rangeQuery(ES_KEY_BUCKET).gte(startTime));
        }
        if (endTime != null) {
            finalQuery.must(QueryBuilders.rangeQuery(ES_KEY_BUCKET_END).lte(endTime));
        }

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(finalQuery);
        searchSourceBuilder.sort(new FieldSortBuilder(ES_KEY_BUCKET).order(SortOrder.DESC));
        if (maxBuckets != null) {
           searchSourceBuilder.size(maxBuckets);
        } else {
            searchSourceBuilder.size(ELASTIC_FETCH_ALL_SIZE);
        }

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);

        final String indexName = indexConvention.getIndexName(USAGE_STATS_BASE_INDEX_NAME);
        searchRequest.indices(indexName);

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

            agg.setBucket(Long.valueOf(((Number) docFields.get(ES_KEY_BUCKET)).longValue()));
            agg.setDuration(WindowDuration.valueOf((String) docFields.get(ES_KEY_DURATION)));
            agg.setResource(Urn.createFromString((String) docFields.get(ES_KEY_RESOURCE)));
            agg.setMetrics(metrics);

            metrics.setUniqueUserCount((Integer) docFields.get("metrics.unique_user_count"), SetMode.REMOVE_IF_NULL);
            if (docFields.containsKey("metrics.users")) {
                UserUsageCountsArray users = new UserUsageCountsArray();
                List<Map<String, Object>> docUsers = (List<Map<String, Object>>) docFields.get("metrics.users");
                for (Map<String, Object> map : docUsers) {
                    UserUsageCounts userUsage = new UserUsageCounts();
                    if (map.containsKey("user")) {
                        userUsage.setUser(Urn.createFromString((String) map.get("user")));
                    }
                    userUsage.setUserEmail((String) map.get("user_email"), SetMode.REMOVE_IF_NULL);
                    userUsage.setCount((Integer) map.get("count"));
                    users.add(userUsage);
                }
                metrics.setUsers(users);
            }

            metrics.setTotalSqlQueries((Integer) docFields.get("metrics.total_sql_queries"), SetMode.REMOVE_IF_NULL);
            if (docFields.containsKey("metrics.top_sql_queries")) {
                StringArray queries = new StringArray();
                List<String> docQueries = (List<String>) docFields.get("metrics.top_sql_queries");
                queries.addAll(docQueries);
                metrics.setTopSqlQueries(queries);
            }

            if (docFields.containsKey("metrics.fields")) {
                FieldUsageCountsArray fields = new FieldUsageCountsArray();
                List<Map<String, Object>> docUsers = (List<Map<String, Object>>) docFields.get("metrics.fields");
                for (Map<String, Object> map : docUsers) {
                    FieldUsageCounts fieldUsage = new FieldUsageCounts();
                    fieldUsage.setFieldName((String) map.get("field_name"));
                    fieldUsage.setCount((Integer) map.get("count"));
                    fields.add(fieldUsage);
                }
                metrics.setFields(fields);
            }

            return agg;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static int windowDurationToMillis(@Nonnull WindowDuration duration) {
        if (duration == WindowDuration.DAY) {
            return 24 * 60 * 60 * 1000;
        } else if (duration == WindowDuration.HOUR) {
            return 60 * 60 * 1000;
        } else {
            throw new IllegalArgumentException("invalid WindowDuration enum state: " + duration.name());
        }
    }

}
