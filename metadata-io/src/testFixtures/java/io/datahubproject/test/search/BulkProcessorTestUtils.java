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
    var bulkProcessors = ReflectionTestUtils.getField(searchClient, "bulkProcessors");
    if (bulkProcessors instanceof BulkProcessor[]) {
      // For multiple BulkProcessors, get the first one for testing purposes
      BulkProcessor[] processors = (BulkProcessor[]) bulkProcessors;
      if (processors.length > 0) {
        var bulkRequestHandler = ReflectionTestUtils.getField(processors[0], "bulkRequestHandler");
        return (BulkProcessorProxyListener)
            ReflectionTestUtils.getField(bulkRequestHandler, "listener");
      }
    } else if (bulkProcessors instanceof BulkProcessor) {
      // Fallback for single BulkProcessor (backward compatibility)
      var bulkRequestHandler = ReflectionTestUtils.getField(bulkProcessors, "bulkRequestHandler");
      return (BulkProcessorProxyListener)
          ReflectionTestUtils.getField(bulkRequestHandler, "listener");
    }
    return null;
  }

  private static ESBulkProcessorProxyListener getBulkListener(ESBulkProcessor esBulkProcessor) {
    var searchClient =
        (SearchClientShim<?>) ReflectionTestUtils.getField(esBulkProcessor, "searchClient");
    var bulkProcessors = ReflectionTestUtils.getField(searchClient, "bulkProcessors");
    if (bulkProcessors instanceof BulkIngester<?>[]) {
      // For multiple BulkIngesters, get the first one for testing purposes
      BulkIngester<?>[] processors = (BulkIngester<?>[]) bulkProcessors;
      if (processors.length > 0) {
        return (ESBulkProcessorProxyListener) ReflectionTestUtils.getField(processors[0], "listener");
      }
    } else if (bulkProcessors instanceof BulkIngester<?>) {
      // Fallback for single BulkIngester (backward compatibility)
      return (ESBulkProcessorProxyListener) ReflectionTestUtils.getField(bulkProcessors, "listener");
    }
    return null;
  }

  public static void replaceBulkProcessorListener(ESBulkProcessor esBulkProcessor) {
    var searchClient =
        (SearchClientShim<?>) ReflectionTestUtils.getField(esBulkProcessor, "searchClient");
    var bulkProcessors = ReflectionTestUtils.getField(searchClient, "bulkProcessors");
    
    if (bulkProcessors instanceof BulkProcessor[]) {
      // Handle multiple BulkProcessors
      BulkProcessor[] processors = (BulkProcessor[]) bulkProcessors;
      for (BulkProcessor processor : processors) {
        var bulkRequestHandler = ReflectionTestUtils.getField(processor, "bulkRequestHandler");
        var bulkProcessorListener =
            (BulkProcessor.Listener) ReflectionTestUtils.getField(bulkRequestHandler, "listener");
        ReflectionTestUtils.setField(
            bulkRequestHandler, "listener", new BulkProcessorProxyListener(bulkProcessorListener));
      }
    } else if (bulkProcessors instanceof BulkIngester<?>[]) {
      // Handle multiple BulkIngesters (ES8)
      BulkIngester<?>[] processors = (BulkIngester<?>[]) bulkProcessors;
      for (BulkIngester<?> processor : processors) {
        var bulkProcessorListener =
            (BulkListener<Object>) ReflectionTestUtils.getField(processor, "listener");
        ReflectionTestUtils.setField(
            processor, "listener", new ESBulkProcessorProxyListener(bulkProcessorListener));
      }
    } else if (bulkProcessors instanceof BulkProcessor) {
      // Fallback for single BulkProcessor (backward compatibility)
      var bulkRequestHandler = ReflectionTestUtils.getField(bulkProcessors, "bulkRequestHandler");
      var bulkProcessorListener =
          (BulkProcessor.Listener) ReflectionTestUtils.getField(bulkRequestHandler, "listener");
      ReflectionTestUtils.setField(
          bulkRequestHandler, "listener", new BulkProcessorProxyListener(bulkProcessorListener));
    } else if (bulkProcessors instanceof BulkIngester<?>) {
      // Fallback for single BulkIngester (backward compatibility)
      var bulkProcessorListener =
          (BulkListener<Object>) ReflectionTestUtils.getField(bulkProcessors, "listener");
      ReflectionTestUtils.setField(
          bulkProcessors, "listener", new ESBulkProcessorProxyListener(bulkProcessorListener));
    }
  }
}
