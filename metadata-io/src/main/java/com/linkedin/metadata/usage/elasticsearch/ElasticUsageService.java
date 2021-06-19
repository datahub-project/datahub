package com.linkedin.metadata.usage.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.exception.ESQueryException;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.usage.UsageService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
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
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ElasticUsageService implements UsageService {
    private static final String USAGE_STATS_BASE_INDEX_NAME = "usageStats_v1";

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
    public SearchResult query(@Nonnull String resource, @Nonnull WindowDuration window, Long start_time, Long end_time) {
        final BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
        finalQuery.must(QueryBuilders.matchQuery("resource", resource));
        finalQuery.must(QueryBuilders.matchQuery("window", window.name()));
        if (start_time != null) {
            finalQuery.must(QueryBuilders.rangeQuery("bucket").gte(start_time));
        }
        if (end_time != null) {
            finalQuery.must(QueryBuilders.rangeQuery("bucket_end").lte(end_time));
        }
        // TODO handle top N queries

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(finalQuery);
        searchSourceBuilder.sort(new FieldSortBuilder("bucket").order(SortOrder.DESC));

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        log.debug("Search request is: " + searchRequest.toString());

        try {
            final SearchResponse searchResponse = elasticClient.search(searchRequest, RequestOptions.DEFAULT);
            final SearchHits hits = searchResponse.getHits();

            hits.getHits()[0].getSourceAsMap();

//            List<Urn> resultList = getResults(searchResponse);
//            SearchResultMetadata searchResultMetadata = extractSearchResultMetadata(searchResponse);
//            searchResultMetadata.setUrns(new UrnArray(resultList));
//
//            return new SearchResult().setEntities(new UrnArray(resultList))
//                    .setMetadata(searchResultMetadata)
//                    .setFrom(from)
//                    .setPageSize(size)
//                    .setNumEntities(totalCount);
//            hits.getHits();
            return null;
        } catch (Exception e) {
            log.error("Search query failed:" + e.getMessage());
            throw new ESQueryException("Search query failed:", e);
        }
    }

}
