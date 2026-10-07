package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESBulkProcessorProceedOnConflictTest {
  private static final String INDEX = "graph_service_v1";
  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();
  private SearchClientShim<?> searchClient;
  private MetricUtils metricUtils;
  private ESBulkProcessor processor;

  @BeforeMethod
  public void setup() {
    searchClient = mock(SearchClientShim.class);
    metricUtils = mock(MetricUtils.class);
    processor = ESBulkProcessor.builder(searchClient, metricUtils).build();
  }

  @Test
  public void aCleanDeleteProceedsPastConflictsOnTheGivenIndex() throws IOException {
    answer(response(0L, List.of(), false));

    processor.deleteByQueryProceedOnConflict(opContext, QueryBuilders.matchAllQuery(), true, INDEX);

    ArgumentCaptor<DeleteByQueryRequest> request =
        ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    verify(searchClient)
        .deleteByQuery(any(OperationContext.class), request.capture(), any(RequestOptions.class));
    assertFalse(request.getValue().isAbortOnVersionConflict());
    assertEquals(request.getValue().indices(), new String[] {INDEX});
  }

  /** A conflicted document was not deleted, so the call fails instead of reporting success. */
  @Test
  public void versionConflictsThrow() throws IOException {
    answer(response(2L, List.of(), false));

    expectThrows(
        IllegalStateException.class,
        () ->
            processor.deleteByQueryProceedOnConflict(
                opContext, QueryBuilders.matchAllQuery(), true, INDEX));
  }

  @Test
  public void bulkFailuresThrow() throws IOException {
    answer(response(0L, List.of(mock(BulkItemResponse.Failure.class)), false));

    expectThrows(
        IllegalStateException.class,
        () ->
            processor.deleteByQueryProceedOnConflict(
                opContext, QueryBuilders.matchAllQuery(), true, INDEX));
  }

  @Test
  public void aTimeoutThrows() throws IOException {
    answer(response(0L, List.of(), true));

    expectThrows(
        IllegalStateException.class,
        () ->
            processor.deleteByQueryProceedOnConflict(
                opContext, QueryBuilders.matchAllQuery(), true, INDEX));
  }

  @Test
  public void aFailedRequestThrowsWithItsCause() throws IOException {
    IOException unavailable = new IOException("cluster unavailable");
    when(searchClient.deleteByQuery(
            any(OperationContext.class),
            any(DeleteByQueryRequest.class),
            any(RequestOptions.class)))
        .thenThrow(unavailable);

    IllegalStateException thrown =
        expectThrows(
            IllegalStateException.class,
            () ->
                processor.deleteByQueryProceedOnConflict(
                    opContext, QueryBuilders.matchAllQuery(), true, INDEX));

    assertSame(thrown.getCause(), unavailable);
    verify(metricUtils).exceptionIncrement(eq(ESBulkProcessor.class), anyString(), eq(unavailable));
  }

  private void answer(BulkByScrollResponse response) throws IOException {
    when(searchClient.deleteByQuery(
            any(OperationContext.class),
            any(DeleteByQueryRequest.class),
            any(RequestOptions.class)))
        .thenReturn(response);
  }

  private static BulkByScrollResponse response(
      long conflicts, List<BulkItemResponse.Failure> failures, boolean timedOut) {
    BulkByScrollResponse response = mock(BulkByScrollResponse.class);
    when(response.getVersionConflicts()).thenReturn(conflicts);
    when(response.getBulkFailures()).thenReturn(failures);
    when(response.getSearchFailures()).thenReturn(List.of());
    when(response.isTimedOut()).thenReturn(timedOut);
    return response;
  }
}
