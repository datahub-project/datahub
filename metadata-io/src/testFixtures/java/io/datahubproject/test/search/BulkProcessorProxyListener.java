package io.datahubproject.test.search;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;

public class BulkProcessorProxyListener implements BulkProcessor.Listener {
  private final BulkProcessor.Listener listener;
  private final AtomicInteger unsentItemsCounter = new AtomicInteger();

  public BulkProcessorProxyListener(BulkProcessor.Listener listener) {
    this.listener = listener;
  }

  @Override
  public void beforeBulk(long l, BulkRequest bulkRequest) {
    unsentItemsCounter.addAndGet(bulkRequest.numberOfActions());
    listener.beforeBulk(l, bulkRequest);
  }

  @Override
  public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
    unsentItemsCounter.addAndGet(-bulkResponse.getItems().length);
    listener.afterBulk(l, bulkRequest, bulkResponse);
  }

  @Override
  public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
    listener.afterBulk(l, bulkRequest, throwable);
  }

  void waitForBulkProcessed() throws InterruptedException {
    for (int i = 0; i < 6000; i++) {
      if (unsentItemsCounter.get() == 0) {
        break;
      }
      TimeUnit.MILLISECONDS.sleep(5);
    }
    // reset the counter just in case
    unsentItemsCounter.set(0);
  }
}
