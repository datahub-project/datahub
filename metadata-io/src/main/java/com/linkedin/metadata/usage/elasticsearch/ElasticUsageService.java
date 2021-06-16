package com.linkedin.metadata.usage.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.usage.UsageService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticUsageService implements UsageService {
    private static final String BASE_INDEX_NAME = "usageStats";

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
                    indexConvention.getIndexName(BASE_INDEX_NAME),
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
        mappings.put("resource", dateType);

        return ImmutableMap.of("properties", mappings);
    }

    @Override
    public void upsertDocument(@Nonnull String document, @Nonnull String docId) {
        final String indexName = indexConvention.getIndexName(BASE_INDEX_NAME);
        final IndexRequest indexRequest = new IndexRequest(indexName).id(docId).source(document, XContentType.JSON);
        final UpdateRequest updateRequest = new UpdateRequest(indexName, docId).doc(document, XContentType.JSON)
                .detectNoop(false)
                .upsert(indexRequest);
        bulkProcessor.add(updateRequest);
    }
}
