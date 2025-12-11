/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.test.search;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ESBulkProcessorProxyListener implements BulkListener<Object> {
  private final BulkListener<Object> listener;
  private final AtomicInteger unsentItemsCounter = new AtomicInteger();

  public ESBulkProcessorProxyListener(BulkListener<Object> listener) {
    this.listener = listener;
  }

  @Override
  public void beforeBulk(long l, BulkRequest bulkRequest, List<Object> objectList) {
    unsentItemsCounter.addAndGet(bulkRequest.operations().size());
    listener.beforeBulk(l, bulkRequest, objectList);
  }

  @Override
  public void afterBulk(
      long l, BulkRequest bulkRequest, List<Object> objectList, BulkResponse bulkResponse) {
    unsentItemsCounter.addAndGet(-bulkResponse.items().size());
    listener.afterBulk(l, bulkRequest, objectList, bulkResponse);
  }

  @Override
  public void afterBulk(
      long l, BulkRequest bulkRequest, List<Object> objectList, Throwable throwable) {
    listener.afterBulk(l, bulkRequest, objectList, throwable);
  }

  void waitForBulkProcessed() throws InterruptedException {
    for (int i = 0; i < 6000; i++) {
      if (unsentItemsCounter.get() <= 0) {
        break;
      }
      TimeUnit.MILLISECONDS.sleep(5);
    }
    // reset the counter just in case
    unsentItemsCounter.set(0);
  }
}
