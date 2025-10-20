package io.datahubproject.test.search;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.AbstractBulkProcessorShim;
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
      getBulkProcessorListener((AbstractBulkProcessorShim<?>) searchClient).waitForBulkProcessed();
    } else if (searchClient instanceof Es8SearchClientShim) {
      getBulkListener((AbstractBulkProcessorShim<?>) searchClient).waitForBulkProcessed();
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
      AbstractBulkProcessorShim<?> abstractShim) {
    var bulkProcessors = ReflectionTestUtils.getField(abstractShim, "bulkProcessors");

    if (bulkProcessors instanceof Object[]) {
      Object[] processors = (Object[]) bulkProcessors;
      if (processors.length > 0 && processors[0] instanceof BulkProcessor) {
        BulkProcessor processor = (BulkProcessor) processors[0];
        var bulkRequestHandler = ReflectionTestUtils.getField(processor, "bulkRequestHandler");
        return (BulkProcessorProxyListener)
            ReflectionTestUtils.getField(bulkRequestHandler, "listener");
      }
    }
    return null;
  }

  private static ESBulkProcessorProxyListener getBulkListener(
      AbstractBulkProcessorShim<?> abstractShim) {
    var bulkProcessors = ReflectionTestUtils.getField(abstractShim, "bulkProcessors");

    if (bulkProcessors instanceof Object[]) {
      Object[] processors = (Object[]) bulkProcessors;
      if (processors.length > 0 && processors[0] instanceof BulkIngester<?>) {
        BulkIngester<?> processor = (BulkIngester<?>) processors[0];
        return (ESBulkProcessorProxyListener) ReflectionTestUtils.getField(processor, "listener");
      }
    }
    return null;
  }

  public static void replaceBulkProcessorListener(ESBulkProcessor esBulkProcessor) {
    var searchClient =
        (SearchClientShim<?>) ReflectionTestUtils.getField(esBulkProcessor, "searchClient");

    // Cast to AbstractBulkProcessorShim to access bulkProcessors field
    if (searchClient instanceof AbstractBulkProcessorShim<?>) {
      replaceBulkProcessorListener((AbstractBulkProcessorShim<?>) searchClient);
      return;
    }

    throw new IllegalStateException("Failed to replaceBulkProcessorListener");
  }

  public static void replaceBulkProcessorListener(AbstractBulkProcessorShim<?> abstractShim) {
    var bulkProcessors = ReflectionTestUtils.getField(abstractShim, "bulkProcessors");

    if (bulkProcessors instanceof Object[]) {
      Object[] processors = (Object[]) bulkProcessors;
      for (Object processor : processors) {
        if (processor instanceof BulkProcessor) {
          var bulkRequestHandler = ReflectionTestUtils.getField(processor, "bulkRequestHandler");
          var bulkProcessorListener =
              (BulkProcessor.Listener) ReflectionTestUtils.getField(bulkRequestHandler, "listener");
          ReflectionTestUtils.setField(
              bulkRequestHandler,
              "listener",
              new BulkProcessorProxyListener(bulkProcessorListener));
        } else if (processor instanceof BulkIngester<?>) {
          var bulkProcessorListener =
              (BulkListener<Object>) ReflectionTestUtils.getField(processor, "listener");
          ReflectionTestUtils.setField(
              processor, "listener", new ESBulkProcessorProxyListener(bulkProcessorListener));
        }
      }
    }
  }
}
