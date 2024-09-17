package io.datahubproject.test.search;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.springframework.test.util.ReflectionTestUtils;

public class BulkProcessorTestUtils {
  private BulkProcessorTestUtils() {}

  public static void syncAfterWrite(ESBulkProcessor bulkProcessor)
      throws InterruptedException, IOException {
    bulkProcessor.flush();
    final RestHighLevelClient searchClient = getRestHighLevelClient(bulkProcessor);
    // if the bulks are big it takes time for Elastic/OpenSearch to process these bulk requests
    getBulkProcessorListener(bulkProcessor).waitForBulkProcessed();
    waitForCompletion(searchClient);
    // some tasks might have refresh = false, so we need to refresh manually
    searchClient.indices().refresh(new RefreshRequest(), RequestOptions.DEFAULT);
    waitForCompletion(searchClient);
  }

  private static void waitForCompletion(RestHighLevelClient searchClient)
      throws IOException, InterruptedException {
    while (!searchClient
        .tasks()
        .list(
            new ListTasksRequest()
                .setActions("indices:*,*/put,*/update")
                .setWaitForCompletion(true)
                .setTimeout(TimeValue.timeValueSeconds(30)),
            RequestOptions.DEFAULT)
        .getTasks()
        .isEmpty()) {
      // Mostly this is not reached, but in some rare cases it might
      TimeUnit.MILLISECONDS.sleep(5);
    }
  }

  private static RestHighLevelClient getRestHighLevelClient(ESBulkProcessor esBulkProcessor) {
    return (RestHighLevelClient) ReflectionTestUtils.getField(esBulkProcessor, "searchClient");
  }

  private static BulkProcessorProxyListener getBulkProcessorListener(
      ESBulkProcessor esBulkProcessor) {
    var bulkProcessor = ReflectionTestUtils.getField(esBulkProcessor, "bulkProcessor");
    var bulkRequestHandler = ReflectionTestUtils.getField(bulkProcessor, "bulkRequestHandler");
    return (BulkProcessorProxyListener)
        ReflectionTestUtils.getField(bulkRequestHandler, "listener");
  }

  public static void replaceBulkProcessorListener(ESBulkProcessor esBulkProcessor) {
    var bulkProcessor =
        (BulkProcessor) ReflectionTestUtils.getField(esBulkProcessor, "bulkProcessor");
    var bulkRequestHandler = ReflectionTestUtils.getField(bulkProcessor, "bulkRequestHandler");
    var bulkProcessorListener =
        (BulkProcessor.Listener) ReflectionTestUtils.getField(bulkRequestHandler, "listener");
    ReflectionTestUtils.setField(
        bulkRequestHandler, "listener", new BulkProcessorProxyListener(bulkProcessorListener));
  }
}
