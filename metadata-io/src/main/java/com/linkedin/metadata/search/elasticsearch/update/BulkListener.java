package com.linkedin.metadata.search.elasticsearch.update;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.stream.Collectors;


@Slf4j
public class BulkListener implements BulkProcessor.Listener {
  private static final BulkListener INSTANCE = new BulkListener();

  public static BulkListener getInstance() {
    return INSTANCE;
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request) {

  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    if (response.hasFailures()) {
      log.error("Failed to feed bulk request. Number of events: " + response.getItems().length + " Took time ms: "
              + response.getIngestTookInMillis() + " Message: " + response.buildFailureMessage());
    } else {
      log.info("Successfully fed bulk request. Number of events: " + response.getItems().length + " Took time ms: "
              + response.getIngestTookInMillis());
    }
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    log.error("Error feeding bulk request. No retries left. Request: {}", bulkRequestSummary(request), failure);
  }

  private static String bulkRequestSummary(BulkRequest request) {
    return request.requests().stream().map(req -> String.format(
            "Failed to perform bulk request: index [%s], optype: [%s], type [%s], id [%s]",
            req.index(), req.opType(), req.type(), req.id())
    ).collect(Collectors.joining(";"));
  }
}
