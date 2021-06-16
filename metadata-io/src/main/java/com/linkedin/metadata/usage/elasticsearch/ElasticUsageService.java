package com.linkedin.metadata.usage.elasticsearch;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexBuilder;
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

public class ElasticUsageService implements UsageService {
    private static final String baseIndexName = "usageStats";

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
            // TODO configure this
            new IndexBuilder(elasticClient, indexConvention.getIndexName(baseIndexName), null, null).buildIndex();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void upsertDocument(@Nonnull String document, @Nonnull String docId) {
        final String indexName = indexConvention.getIndexName(baseIndexName);
        final IndexRequest indexRequest = new IndexRequest(indexName).id(docId).source(document, XContentType.JSON);
        final UpdateRequest updateRequest = new UpdateRequest(indexName, docId).doc(document, XContentType.JSON)
                .detectNoop(false)
                .upsert(indexRequest);
        bulkProcessor.add(updateRequest);
    }
}
