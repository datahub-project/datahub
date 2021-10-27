package com.linkedin.metadata;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.testng.TestException;

import java.net.SocketTimeoutException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ElasticSearchTestUtils {

    // request options for all requests
    private static final RequestOptions OPTIONS = RequestOptions.DEFAULT;

    // retry logic for ES requests
    private static final Retry RETRY = Retry.of("ElasticSearchTestUtils", RetryConfig.custom()
            .retryExceptions(SocketTimeoutException.class, ElasticsearchStatusException.class)
            .failAfterMaxAttempts(true)
            .maxAttempts(3)
            .build()
    );

    // allow for Supplier<T> that throw exceptions
    private interface ThrowingSupplier<T, E extends Exception> {
        T get() throws E;
    }

    // We are retrying requests, otherwise concurrency tests will see exceptions like these:
    //   java.net.SocketTimeoutException: 30,000 milliseconds timeout on connection http-outgoing-1 [ACTIVE]
    private static <T> T retry(ThrowingSupplier<T, Exception> func) {
        return RETRY.executeSupplier(() -> {
            try {
                return func.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private ElasticSearchTestUtils() {
    }

    public static void syncAfterWrite(RestHighLevelClient client) throws Exception {
        syncAfterWrite(client, "test-sync-flag");
    }

    public static void syncAfterWrite(RestHighLevelClient searchClient, String indexName) throws Exception {
        // we add some more data (a sync flag) and wait for it to appear
        // we pick a random flag so that this can be used concurrently
        String syncFlag = UUID.randomUUID().toString();

        // add the flag and wait for it to appear, preferably to the indexed modified outside
        addSyncFlag(searchClient, syncFlag, indexName);
        waitForSyncFlag(searchClient, syncFlag, indexName, true);

        // flush changes for all indices in ES to disk
        FlushResponse fResponse = retry(() -> searchClient.indices().flush(new FlushRequest(), OPTIONS));
        if (fResponse.getFailedShards() > 0) {
            throw new RuntimeException("Failed to flush " + fResponse.getFailedShards() + " of " + fResponse.getTotalShards() + " shards");
        }

        // wait for all indices to be refreshed
        RefreshResponse rResponse = retry(() -> searchClient.indices().refresh(new RefreshRequest(), OPTIONS));
        if (rResponse.getFailedShards() > 0) {
            throw new RuntimeException("Failed to refresh " + rResponse.getFailedShards() + " of " + rResponse.getTotalShards() + " shards");
        }

        // remove the flag again and wait for it to disappear
        removeSyncFlag(searchClient, syncFlag, indexName);
        waitForSyncFlag(searchClient, syncFlag, indexName, false);
    }

    private static void addSyncFlag(RestHighLevelClient searchClient, String docId, String indexName) {
        String document = "{ }";
        final IndexRequest indexRequest = new IndexRequest(indexName).id(docId).source(document, XContentType.JSON);
        final UpdateRequest updateRequest = new UpdateRequest(indexName, docId).doc(document, XContentType.JSON)
                .detectNoop(false)
                .retryOnConflict(3)
                .upsert(indexRequest);
        retry(() -> searchClient.update(updateRequest, OPTIONS));
    }

    private static void removeSyncFlag(RestHighLevelClient searchClient, String docId, String indexName) {
        final DeleteRequest deleteRequest = new DeleteRequest(indexName).id(docId);
        retry(() -> searchClient.delete(deleteRequest, OPTIONS));
    }

    private static void waitForSyncFlag(RestHighLevelClient searchClient, String docId, String indexName, boolean toExist)
            throws InterruptedException {
        GetRequest request = new GetRequest(indexName).id(docId);
        long timeout = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
        while (System.currentTimeMillis() < timeout) {
            GetResponse response = retry(() -> searchClient.get(request, OPTIONS));
            if (response.isExists() == toExist) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new TestException("Waiting for sync timed out");
    }

}
