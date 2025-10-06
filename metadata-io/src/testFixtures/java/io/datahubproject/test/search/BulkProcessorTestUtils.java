package io.datahubproject.test.search;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearch2SearchClientShim;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.unit.TimeValue;
import org.springframework.test.util.ReflectionTestUtils;

public class BulkProcessorTestUtils {
  private BulkProcessorTestUtils() {}

  public static void syncAfterWrite(ESBulkProcessor bulkProcessor)
      throws InterruptedException, IOException {
    bulkProcessor.flush();
    final SearchClientShim<?> searchClient = getRestHighLevelClient(bulkProcessor);
    // if the bulks are big it takes time for Elastic/OpenSearch to process these bulk requests
    if (searchClient instanceof OpenSearch2SearchClientShim) {
      getBulkProcessorListener(bulkProcessor).waitForBulkProcessed();
    } else if (searchClient instanceof Es8SearchClientShim) {
      getBulkListener(bulkProcessor).waitForBulkProcessed();
    }
    waitForCompletion(searchClient);
    // some tasks might have refresh = false, so we need to refresh manually
    searchClient.refreshIndex(new RefreshRequest(), RequestOptions.DEFAULT);
    waitForCompletion(searchClient);
  }

  private static void waitForCompletion(SearchClientShim<?> searchClient)
      throws IOException, InterruptedException {
    while (!searchClient
        .listTasks(
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

  private static SearchClientShim<?> getRestHighLevelClient(ESBulkProcessor esBulkProcessor) {
    return (SearchClientShim<?>) ReflectionTestUtils.getField(esBulkProcessor, "searchClient");
  }

  private static BulkProcessorProxyListener getBulkProcessorListener(
      ESBulkProcessor esBulkProcessor) {
    var searchClient =
        (SearchClientShim<?>) ReflectionTestUtils.getField(esBulkProcessor, "searchClient");
    var bulkProcessor = ReflectionTestUtils.getField(searchClient, "bulkProcessor");
    var bulkRequestHandler = ReflectionTestUtils.getField(bulkProcessor, "bulkRequestHandler");
    return (BulkProcessorProxyListener)
        ReflectionTestUtils.getField(bulkRequestHandler, "listener");
  }

  private static ESBulkProcessorProxyListener getBulkListener(ESBulkProcessor esBulkProcessor) {
    var searchClient =
        (SearchClientShim<?>) ReflectionTestUtils.getField(esBulkProcessor, "searchClient");
    var bulkProcessor = ReflectionTestUtils.getField(searchClient, "bulkProcessor");
    return (ESBulkProcessorProxyListener) ReflectionTestUtils.getField(bulkProcessor, "listener");
  }

  public static void replaceBulkProcessorListener(ESBulkProcessor esBulkProcessor) {
    var searchClient =
        (SearchClientShim<?>) ReflectionTestUtils.getField(esBulkProcessor, "searchClient");
    var bulkProcessor = ReflectionTestUtils.getField(searchClient, "bulkProcessor");
    if (bulkProcessor instanceof BulkProcessor) {
      var bulkRequestHandler = ReflectionTestUtils.getField(bulkProcessor, "bulkRequestHandler");
      var bulkProcessorListener =
          (BulkProcessor.Listener) ReflectionTestUtils.getField(bulkRequestHandler, "listener");
      ReflectionTestUtils.setField(
          bulkRequestHandler, "listener", new BulkProcessorProxyListener(bulkProcessorListener));
    } else if (bulkProcessor instanceof BulkIngester<?>) {
      var bulkProcessorListener =
          (BulkListener<Object>) ReflectionTestUtils.getField(bulkProcessor, "listener");
      ReflectionTestUtils.setField(
          bulkProcessor, "listener", new ESBulkProcessorProxyListener(bulkProcessorListener));
    }
  }
}
