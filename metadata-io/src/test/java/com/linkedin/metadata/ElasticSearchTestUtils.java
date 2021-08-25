package com.linkedin.metadata;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchTestUtils {

    private ElasticSearchTestUtils() {
    }

    public static void syncAfterWrite(RestHighLevelClient searchClient) throws Exception {
        // flush changes in ES to disk
        FlushResponse fResponse = searchClient.indices().flush(new FlushRequest(), RequestOptions.DEFAULT);
        if (fResponse.getFailedShards() > 0) {
            throw new RuntimeException("Failed to flush " + fResponse.getFailedShards() + " of " + fResponse.getTotalShards() + " shards");
        }

        // wait for all indices to be refreshed
        RefreshResponse rResponse = searchClient.indices().refresh(new RefreshRequest(), RequestOptions.DEFAULT);
        if (rResponse.getFailedShards() > 0) {
            throw new RuntimeException("Failed to refresh " + rResponse.getFailedShards() + " of " + rResponse.getTotalShards() + " shards");
        }
    }

}
