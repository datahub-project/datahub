package com.linkedin.metadata;

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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ElasticSearchTestUtils {

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
        FlushResponse fResponse = searchClient.indices().flush(new FlushRequest(), RequestOptions.DEFAULT);
        if (fResponse.getFailedShards() > 0) {
            throw new RuntimeException("Failed to flush " + fResponse.getFailedShards() + " of " + fResponse.getTotalShards() + " shards");
        }

        // wait for all indices to be refreshed
        RefreshResponse rResponse = searchClient.indices().refresh(new RefreshRequest(), RequestOptions.DEFAULT);
        if (rResponse.getFailedShards() > 0) {
            throw new RuntimeException("Failed to refresh " + rResponse.getFailedShards() + " of " + rResponse.getTotalShards() + " shards");
        }

        // remove the flag again and wait for it to disappear
        removeSyncFlag(searchClient, syncFlag, indexName);
        waitForSyncFlag(searchClient, syncFlag, indexName, false);
    }

    private static void addSyncFlag(RestHighLevelClient searchClient, String docId, String indexName) throws IOException {
        String document = "{ }";
        final IndexRequest indexRequest = new IndexRequest(indexName).id(docId).source(document, XContentType.JSON);
        final UpdateRequest updateRequest = new UpdateRequest(indexName, docId).doc(document, XContentType.JSON)
                .detectNoop(false)
                .upsert(indexRequest);
        searchClient.update(updateRequest, RequestOptions.DEFAULT);
    }

    private static void removeSyncFlag(RestHighLevelClient searchClient, String docId, String indexName) throws IOException {
        final DeleteRequest deleteRequest = new DeleteRequest(indexName).id(docId);
        searchClient.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    private static void waitForSyncFlag(RestHighLevelClient searchClient, String docId, String indexName, boolean toExist)
            throws IOException, InterruptedException {
        GetRequest request = new GetRequest(indexName).id(docId);
        long timeout = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
        while (System.currentTimeMillis() < timeout) {
            GetResponse response = searchClient.get(request, RequestOptions.DEFAULT);
            if (response.isExists() == toExist) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new TestException("Waiting for sync timed out");
    }

}
