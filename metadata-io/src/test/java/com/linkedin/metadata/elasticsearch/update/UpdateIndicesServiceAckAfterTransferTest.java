package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.update.BulkTransferException;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.service.UpdateGraphIndicesService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

public class UpdateIndicesServiceAckAfterTransferTest {

  @Test
  public void testFlushAndWaitSkippedWhenWriteDaoNull() {
    ElasticSearchService elasticSearchService = mock(ElasticSearchService.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(null);

    UpdateIndicesService service =
        new UpdateIndicesService(
            mock(UpdateGraphIndicesService.class),
            elasticSearchService,
            mock(SystemMetadataService.class),
            Collections.emptyList(),
            null,
            false,
            false,
            false);
    service.flushAndWaitIfConfigured();
  }

  @Test
  public void testFlushAndWaitSkippedWhenBulkProcessorNull() {
    ElasticSearchService elasticSearchService = mock(ElasticSearchService.class);
    ESWriteDAO writeDAO = mock(ESWriteDAO.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(writeDAO);
    when(writeDAO.getBulkProcessor()).thenReturn(null);

    UpdateIndicesService service =
        new UpdateIndicesService(
            mock(UpdateGraphIndicesService.class),
            elasticSearchService,
            mock(SystemMetadataService.class),
            Collections.emptyList(),
            null,
            false,
            false,
            false);
    service.flushAndWaitIfConfigured();
  }

  @Test
  public void testFlushAndWaitSkippedWhenDisabled() throws Exception {
    ESBulkProcessor bulkProcessor = mock(ESBulkProcessor.class);
    when(bulkProcessor.isAckAfterTransfer()).thenReturn(false);

    UpdateIndicesService service = serviceWith(bulkProcessor);
    service.flushAndWaitIfConfigured();
    verify(bulkProcessor, never()).flushAndWait(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testFlushAndWaitInvokedWhenEnabled() throws Exception {
    ESBulkProcessor bulkProcessor = mock(ESBulkProcessor.class);
    when(bulkProcessor.isAckAfterTransfer()).thenReturn(true);
    when(bulkProcessor.getAckAfterTransferTimeoutSeconds()).thenReturn(30);
    doNothing().when(bulkProcessor).flushAndWait(Duration.ofSeconds(30));

    UpdateIndicesService service = serviceWith(bulkProcessor);
    service.flushAndWaitIfConfigured();
    verify(bulkProcessor).flushAndWait(Duration.ofSeconds(30));
  }

  @Test
  public void testFlushAndWaitPropagatesTransferFailure() throws Exception {
    ESBulkProcessor bulkProcessor = mock(ESBulkProcessor.class);
    when(bulkProcessor.isAckAfterTransfer()).thenReturn(true);
    when(bulkProcessor.getAckAfterTransferTimeoutSeconds()).thenReturn(10);
    doThrow(new BulkTransferException(2, "failed"))
        .when(bulkProcessor)
        .flushAndWait(Duration.ofSeconds(10));

    UpdateIndicesService service = serviceWith(bulkProcessor);
    expectThrows(RuntimeException.class, service::flushAndWaitIfConfigured);
  }

  @Test
  public void testFlushAndWaitPropagatesTimeout() throws Exception {
    ESBulkProcessor bulkProcessor = mock(ESBulkProcessor.class);
    when(bulkProcessor.isAckAfterTransfer()).thenReturn(true);
    when(bulkProcessor.getAckAfterTransferTimeoutSeconds()).thenReturn(10);
    doThrow(new TimeoutException("timeout"))
        .when(bulkProcessor)
        .flushAndWait(Duration.ofSeconds(10));

    UpdateIndicesService service = serviceWith(bulkProcessor);
    expectThrows(RuntimeException.class, service::flushAndWaitIfConfigured);
  }

  private static UpdateIndicesService serviceWith(ESBulkProcessor bulkProcessor) {
    ElasticSearchService elasticSearchService = mock(ElasticSearchService.class);
    ESWriteDAO writeDAO = mock(ESWriteDAO.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(writeDAO);
    when(writeDAO.getBulkProcessor()).thenReturn(bulkProcessor);

    return new UpdateIndicesService(
        mock(UpdateGraphIndicesService.class),
        elasticSearchService,
        mock(SystemMetadataService.class),
        Collections.emptyList(),
        null,
        false,
        false,
        false);
  }
}
