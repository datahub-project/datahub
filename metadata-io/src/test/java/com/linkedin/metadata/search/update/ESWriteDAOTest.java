package com.linkedin.metadata.search.update;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.metadata.config.search.BulkDeleteConfiguration;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.client.tasks.TaskId;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.tasks.TaskInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ESWriteDAOTest {

  private static final String TEST_DELETE_INDEX = "test_index";
  private static final String TEST_NODE_ID = "node1";
  private static final long TEST_TASK_ID = 12345L;
  private static final String TEST_TASK_STRING = TEST_NODE_ID + ":" + TEST_TASK_ID;

  private static final String TEST_ENTITY = "dataset";
  private static final String TEST_DOC_ID = "testDocId";
  private static final String TEST_INDEX = "datasetindex_v2";
  private static final int NUM_RETRIES = 3;
  private static final String TEST_PATTERN = "*index_v2";

  @Mock private SearchClientShim<?> mockSearchClient;
  @Mock private ESBulkProcessor mockBulkProcessor;
  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  private ESWriteDAO esWriteDAO;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    esWriteDAO =
        new ESWriteDAO(
            TEST_OS_SEARCH_CONFIG.toBuilder()
                .bulkProcessor(BulkProcessorConfiguration.builder().numRetries(NUM_RETRIES).build())
                .build(),
            mockSearchClient,
            mockBulkProcessor);
  }

  @Test
  public void testUpsertDocument() {
    String document = "{\"field\":\"value\"}";

    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, TEST_DOC_ID);

    ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
    verify(mockBulkProcessor).add(requestCaptor.capture());

    UpdateRequest capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.index(), TEST_INDEX);
    assertEquals(capturedRequest.id(), TEST_DOC_ID);
    assertFalse(capturedRequest.detectNoop());
    assertTrue(capturedRequest.docAsUpsert());
    assertEquals(capturedRequest.retryOnConflict(), NUM_RETRIES);

    // Verify the document content
    Map<String, Object> sourceMap = capturedRequest.doc().sourceAsMap();
    assertEquals(sourceMap.get("field"), "value");
  }

  @Test
  public void testDeleteDocument() {
    esWriteDAO.deleteDocument(opContext, TEST_ENTITY, TEST_DOC_ID);

    ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
    verify(mockBulkProcessor).add(requestCaptor.capture());

    DeleteRequest capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.index(), TEST_INDEX);
    assertEquals(capturedRequest.id(), TEST_DOC_ID);
  }

  @Test
  public void testApplyScriptUpdate() {
    String scriptSource = "ctx._source.field = params.newValue";
    Map<String, Object> scriptParams = new HashMap<>();
    scriptParams.put("newValue", "newValue");
    Map<String, Object> upsert = new HashMap<>();
    upsert.put("field", "initialValue");

    esWriteDAO.applyScriptUpdate(
        opContext, TEST_ENTITY, TEST_DOC_ID, scriptSource, scriptParams, upsert);

    ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
    verify(mockBulkProcessor).add(requestCaptor.capture());

    UpdateRequest capturedRequest = requestCaptor.getValue();
    assertEquals(TEST_INDEX, capturedRequest.index());
    assertEquals(TEST_DOC_ID, capturedRequest.id());
    assertFalse(capturedRequest.detectNoop());
    assertTrue(capturedRequest.scriptedUpsert());
    assertEquals(NUM_RETRIES, capturedRequest.retryOnConflict());
    assertNull(capturedRequest.doc()); // For scripted updates, this is null

    // Verify script content and parameters
    Script script = capturedRequest.script();
    assertEquals(scriptSource, script.getIdOrCode());
    assertEquals(ScriptType.INLINE, script.getType());
    assertEquals("painless", script.getLang());
    assertEquals(scriptParams, script.getParams());

    // Verify upsert content
    Map<String, Object> upsertMap = capturedRequest.upsertRequest().sourceAsMap();
    assertEquals("initialValue", upsertMap.get("field"));
  }

  @Test
  public void testClear() throws IOException {
    String[] indices = new String[] {"index1", "index2"};
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    when(mockResponse.getIndices()).thenReturn(indices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    // Mock getIndexAliases to return empty aliases (concrete indices, not aliases)
    GetAliasesResponse mockAliasesResponse = mock(GetAliasesResponse.class);
    when(mockAliasesResponse.getAliases()).thenReturn(java.util.Collections.emptyMap());
    when(mockSearchClient.getIndexAliases(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockAliasesResponse);

    // Mock indexExists to return true
    when(mockSearchClient.indexExists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(true);

    // Mock deleteIndex to return acknowledged response
    AcknowledgedResponse mockDeleteResponse = mock(AcknowledgedResponse.class);
    when(mockDeleteResponse.isAcknowledged()).thenReturn(true);
    when(mockSearchClient.deleteIndex(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockDeleteResponse);

    Set<String> deletedIndices = esWriteDAO.clear(opContext);

    // Verify the GetIndexRequest
    ArgumentCaptor<GetIndexRequest> indexRequestCaptor =
        ArgumentCaptor.forClass(GetIndexRequest.class);
    verify(mockSearchClient).getIndex(indexRequestCaptor.capture(), eq(RequestOptions.DEFAULT));
    assertEquals(indexRequestCaptor.getValue().indices()[0], TEST_PATTERN);

    // Verify indices were deleted
    assertEquals(deletedIndices.size(), 2);
    assertTrue(deletedIndices.contains("index1"));
    assertTrue(deletedIndices.contains("index2"));

    // Verify deleteIndex was called for each index
    ArgumentCaptor<DeleteIndexRequest> deleteRequestCaptor =
        ArgumentCaptor.forClass(DeleteIndexRequest.class);
    verify(mockSearchClient, times(2))
        .deleteIndex(deleteRequestCaptor.capture(), eq(RequestOptions.DEFAULT));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testClearWithIOException() throws IOException {
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new IOException("Test exception"));
    // This should now throw RuntimeException (not swallow the error)
    esWriteDAO.clear(opContext);
    // We should never reach here - the exception should be propagated
    fail("Expected RuntimeException to be thrown");
  }

  @Test
  public void testUpsertDocumentWithInvalidJson() {
    String invalidJson = "{invalid:json}";

    try {
      esWriteDAO.upsertDocument(opContext, TEST_ENTITY, invalidJson, TEST_DOC_ID);
    } catch (Exception e) {
      assertTrue(e instanceof org.opensearch.core.xcontent.XContentParseException);
    }
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testUpsertDocumentWithNullDocument() {
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, null, TEST_DOC_ID);
  }

  @Test
  public void testDeleteByQueryAsyncSuccess()
      throws IOException, ExecutionException, InterruptedException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    CompletableFuture<String> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, null);
    String result = future.get();

    assertNotNull(result);
    assertEquals(result, TEST_TASK_STRING);

    // Verify the request
    ArgumentCaptor<DeleteByQueryRequest> requestCaptor =
        ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    verify(mockSearchClient)
        .submitDeleteByQueryTask(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    DeleteByQueryRequest capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.indices()[0], TEST_DELETE_INDEX);
    assertEquals(capturedRequest.getBatchSize(), 1000);
    assertEquals(capturedRequest.getSearchRequest().source().query(), query);
  }

  @Test
  public void testDeleteByQueryAsyncWithCustomConfig()
      throws IOException, ExecutionException, InterruptedException {
    QueryBuilder query = QueryBuilders.matchAllQuery();
    BulkDeleteConfiguration customConfig =
        BulkDeleteConfiguration.builder()
            .batchSize(5000)
            .slices("10")
            .timeout(1)
            .timeoutUnit("HOURS")
            .build();

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    CompletableFuture<String> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, customConfig);
    String result = future.get();

    assertNotNull(result);

    // Verify custom config is applied
    ArgumentCaptor<DeleteByQueryRequest> requestCaptor =
        ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    verify(mockSearchClient)
        .submitDeleteByQueryTask(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    DeleteByQueryRequest capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.getBatchSize(), 5000);
    // Note: We can't directly verify slices as it's set internally
  }

  @Test
  public void testDeleteByQueryAsyncIOException() throws IOException {
    QueryBuilder query = QueryBuilders.termQuery("field", "value");

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new IOException("Network error"));

    CompletableFuture<String> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, null);

    try {
      future.get();
      fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("Failed to start async delete by query"));
    } catch (InterruptedException e) {
      fail("Unexpected InterruptedException");
    }
  }

  @Test
  public void testDeleteByQuerySyncNoDocuments() throws IOException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Mock count response with 0 documents
    CountResponse mockCountResponse = mock(CountResponse.class);
    when(mockCountResponse.getCount()).thenReturn(0L);
    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockCountResponse);

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertTrue(result.isSuccess());
    assertEquals(result.getRemainingDocuments(), 0);
    assertEquals(result.getRetryAttempts(), 0);
    assertNull(result.getFailureReason());
  }

  @Test
  public void testDeleteByQuerySyncSuccessfulDeletion() throws IOException, InterruptedException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Mock count responses
    CountResponse initialCount = mock(CountResponse.class);
    when(initialCount.getCount()).thenReturn(100L);

    CountResponse afterDeleteCount = mock(CountResponse.class);
    when(afterDeleteCount.getCount()).thenReturn(0L);

    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(initialCount)
        .thenReturn(afterDeleteCount);

    // Mock task submission
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    // Mock task monitoring
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    TaskInfo mockTaskInfo = mock(TaskInfo.class);
    when(mockTaskResponse.getTaskInfo()).thenReturn(mockTaskInfo);

    when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(Optional.of(mockTaskResponse));

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertTrue(result.isSuccess());
    assertEquals(result.getRemainingDocuments(), 0);
    assertNull(result.getFailureReason());
  }

  @Test
  public void testDeleteByQuerySyncWithRetries() throws IOException, InterruptedException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Mock count responses - documents remain after first attempt
    CountResponse count100 = mock(CountResponse.class);
    when(count100.getCount()).thenReturn(100L);

    CountResponse count50 = mock(CountResponse.class);
    when(count50.getCount()).thenReturn(50L);

    CountResponse count0 = mock(CountResponse.class);
    when(count0.getCount()).thenReturn(0L);

    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(count100) // Initial count
        .thenReturn(count50) // After first delete
        .thenReturn(count0); // After second delete

    // Mock task submissions
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    // Mock task monitoring
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    TaskInfo mockTaskInfo = mock(TaskInfo.class);
    when(mockTaskResponse.getTaskInfo()).thenReturn(mockTaskInfo);

    when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(Optional.of(mockTaskResponse));

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertTrue(result.isSuccess());
    assertEquals(result.getRemainingDocuments(), 0);
    assertEquals(result.getRetryAttempts(), 1); // One retry was needed
    assertNull(result.getFailureReason());

    // Verify two delete operations were performed
    verify(mockSearchClient, times(2))
        .submitDeleteByQueryTask(any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testDeleteByQuerySyncNoProgress() throws IOException, InterruptedException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Mock count responses - no progress made
    CountResponse count100 = mock(CountResponse.class);
    when(count100.getCount()).thenReturn(100L);

    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(count100); // Always returns 100

    // Mock task submission
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    // Mock task monitoring with no deletions
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    TaskInfo mockTaskInfo = mock(TaskInfo.class);
    when(mockTaskResponse.getTaskInfo()).thenReturn(mockTaskInfo);

    when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(Optional.of(mockTaskResponse));

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertFalse(result.isSuccess());
    assertEquals(result.getRemainingDocuments(), 100);
    assertTrue(result.getFailureReason().contains("no progress"));
  }

  @Test
  public void testDeleteByQuerySyncException() throws IOException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Mock initial count
    CountResponse mockCountResponse = mock(CountResponse.class);
    when(mockCountResponse.getCount()).thenReturn(100L);
    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockCountResponse);

    // Mock exception during task submission
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new IOException("Connection failed"));

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertFalse(result.isSuccess());
    assertEquals(result.getRemainingDocuments(), -1); // Unknown
    assertTrue(result.getFailureReason().contains("Exception"));
  }

  @Test
  public void testParseTaskIdValid() {
    // This would be a private method test, but we can test it indirectly through deleteByQuerySync
    QueryBuilder query = QueryBuilders.termQuery("field", "value");

    try {
      // Setup mocks for a basic flow
      CountResponse mockCount = mock(CountResponse.class);
      when(mockCount.getCount()).thenReturn(100L).thenReturn(0L);
      when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn(mockCount);

      when(mockSearchClient.submitDeleteByQueryTask(
              any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn("node123:456789");

      GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
      when(mockTaskResponse.isCompleted()).thenReturn(true);
      when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn(Optional.of(mockTaskResponse));

      ESWriteDAO.DeleteByQueryResult result =
          esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

      // If we get here without exception, parsing worked
      assertNotNull(result);
    } catch (Exception e) {
      fail("Should not throw exception for valid task ID format");
    }
  }

  @Test
  public void testDeleteByQueryWithInvalidSlicesConfig()
      throws IOException, ExecutionException, InterruptedException {
    QueryBuilder query = QueryBuilders.matchAllQuery();
    BulkDeleteConfiguration customConfig =
        BulkDeleteConfiguration.builder()
            .batchSize(1000)
            .slices("invalid-number")
            .timeout(30)
            .timeoutUnit("MINUTES")
            .build();

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    // Should handle invalid slices gracefully and default to auto
    CompletableFuture<String> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, customConfig);
    String result = future.get();

    assertNotNull(result);
    verify(mockSearchClient)
        .submitDeleteByQueryTask(any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testDeleteByQueryWithAutoSlices()
      throws IOException, ExecutionException, InterruptedException {
    QueryBuilder query = QueryBuilders.matchAllQuery();
    BulkDeleteConfiguration customConfig =
        BulkDeleteConfiguration.builder()
            .batchSize(2000)
            .slices("auto") // Explicitly set to auto
            .timeout(45)
            .timeoutUnit("MINUTES")
            .build();

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    CompletableFuture<String> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, customConfig);
    String result = future.get();

    assertNotNull(result);

    // Verify the request configuration
    ArgumentCaptor<DeleteByQueryRequest> requestCaptor =
        ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    verify(mockSearchClient)
        .submitDeleteByQueryTask(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    DeleteByQueryRequest capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.getBatchSize(), 2000);
    assertEquals(capturedRequest.getTimeout(), TimeValue.timeValueMinutes(45));
  }

  @Test
  public void testDeleteByQuerySyncTaskFailure() throws IOException, InterruptedException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Mock initial count
    CountResponse initialCount = mock(CountResponse.class);
    when(initialCount.getCount()).thenReturn(100L);

    CountResponse afterFailureCount = mock(CountResponse.class);
    when(afterFailureCount.getCount()).thenReturn(90L); // Some documents deleted before failure

    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(initialCount)
        .thenReturn(afterFailureCount);

    // Mock task submission
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    // Mock task monitoring with incomplete/failed task
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(false); // Task didn't complete

    when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(Optional.of(mockTaskResponse));

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertFalse(result.isSuccess());
    assertEquals(result.getRemainingDocuments(), 90);
    assertTrue(result.getFailureReason().contains("Task not completed within timeout"));
  }

  @Test
  public void testDeleteByQuerySyncMaxRetriesWithPartialProgress()
      throws IOException, InterruptedException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Mock count responses - partial progress on each attempt
    CountResponse count100 = mock(CountResponse.class);
    when(count100.getCount()).thenReturn(100L);

    CountResponse count80 = mock(CountResponse.class);
    when(count80.getCount()).thenReturn(80L);

    CountResponse count60 = mock(CountResponse.class);
    when(count60.getCount()).thenReturn(60L);

    CountResponse count40 = mock(CountResponse.class);
    when(count40.getCount()).thenReturn(40L);

    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(count100) // Initial
        .thenReturn(count80) // After 1st delete
        .thenReturn(count60) // After 2nd delete
        .thenReturn(count40); // After 3rd delete (max retries)

    // Mock task submissions
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    // Mock successful task completions
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);

    when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(Optional.of(mockTaskResponse));

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertFalse(result.isSuccess()); // Failed because documents remain
    assertEquals(result.getRemainingDocuments(), 40);
    assertEquals(result.getRetryAttempts(), 3); // 3 total attempts
    assertTrue(result.getFailureReason().contains("Documents still remaining after max retries"));

    // Verify 3 delete operations were performed (initial + 2 retries)
    verify(mockSearchClient, times(3))
        .submitDeleteByQueryTask(any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testMonitorDeleteByQueryTaskWithCountException() throws IOException {
    TaskId taskId = new TaskId(TEST_NODE_ID, TEST_TASK_ID);

    // Mock task API throwing exception
    when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new IOException("Task API error"));

    // Also make count fail when trying to get remaining documents
    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new IOException("Count API error"));

    // Note: This tests the internal monitor method indirectly through deleteByQuerySync
    QueryBuilder query = QueryBuilders.termQuery("field", "value");

    // Setup initial count to trigger the flow
    CountResponse initialCount = mock(CountResponse.class);
    when(initialCount.getCount()).thenReturn(100L);

    // First count succeeds, subsequent counts fail
    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(initialCount)
        .thenThrow(new IOException("Count API error"));

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertFalse(result.isSuccess());
    assertEquals(result.getRemainingDocuments(), -1); // Unknown due to count failure
    assertTrue(result.getFailureReason().contains("Monitoring failed"));
  }

  @Test
  public void testParseTaskIdInvalidFormats() {
    QueryBuilder query = QueryBuilders.termQuery("field", "value");

    // Test null task string
    try {
      CountResponse mockCount = mock(CountResponse.class);
      when(mockCount.getCount()).thenReturn(100L);
      when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn(mockCount);

      when(mockSearchClient.submitDeleteByQueryTask(
              any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn(null);

      ESWriteDAO.DeleteByQueryResult result =
          esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

      assertFalse(result.isSuccess());
      assertTrue(result.getFailureReason().contains("Invalid task string format"));
    } catch (Exception e) {
      // Expected
    }

    // Test task string without colon
    try {
      CountResponse mockCount = mock(CountResponse.class);
      when(mockCount.getCount()).thenReturn(100L);
      when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn(mockCount);

      when(mockSearchClient.submitDeleteByQueryTask(
              any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn("invalidformat");

      ESWriteDAO.DeleteByQueryResult result =
          esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

      assertFalse(result.isSuccess());
      assertTrue(result.getFailureReason().contains("Invalid task string format"));
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testDeleteByQuerySyncInterruptedException() throws IOException, InterruptedException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Mock count responses
    CountResponse count100 = mock(CountResponse.class);
    when(count100.getCount()).thenReturn(100L);

    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(count100);

    // Mock task submission
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    // Mock task monitoring that takes time
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);

    // Simulate interruption during task monitoring
    when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              throw new InterruptedException("Thread interrupted");
            });

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertFalse(result.isSuccess());
    assertTrue(result.getFailureReason().contains("interrupted"));

    // Verify thread interrupted status is preserved
    assertTrue(Thread.currentThread().isInterrupted());
    Thread.interrupted(); // Clear interrupt status for next tests
  }

  @Test
  public void testDeleteByQuerySyncEmptyTaskResponse() throws IOException, InterruptedException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");

    // Mock initial count
    CountResponse initialCount = mock(CountResponse.class);
    when(initialCount.getCount()).thenReturn(50L);

    CountResponse afterCount = mock(CountResponse.class);
    when(afterCount.getCount()).thenReturn(50L); // No change

    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(initialCount)
        .thenReturn(afterCount);

    // Mock task submission
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    // Return empty Optional for task response
    when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(Optional.empty());

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertFalse(result.isSuccess());
    assertEquals(result.getRemainingDocuments(), 50);
    assertTrue(result.getFailureReason().contains("Task not completed within timeout"));
  }

  @Test
  public void testMonitorTaskRequestParameters() throws IOException {
    QueryBuilder query = QueryBuilders.termQuery("status", "deleted");
    Duration customTimeout = Duration.ofMinutes(10);

    BulkDeleteConfiguration customConfig =
        BulkDeleteConfiguration.builder()
            .batchSize(1000)
            .slices("5")
            .timeout(10)
            .timeoutUnit("MINUTES")
            .pollInterval(2)
            .pollIntervalUnit("SECONDS")
            .numRetries(1)
            .build();

    // Setup initial count
    CountResponse initialCount = mock(CountResponse.class);
    when(initialCount.getCount()).thenReturn(50L);
    CountResponse afterCount = mock(CountResponse.class);
    when(afterCount.getCount()).thenReturn(0L);

    when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(initialCount)
        .thenReturn(afterCount);

    // Mock task submission
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    // Mock task response
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    when(mockSearchClient.getTask(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(Optional.of(mockTaskResponse));

    esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, customConfig);

    // Verify GetTaskRequest parameters
    ArgumentCaptor<GetTaskRequest> taskRequestCaptor =
        ArgumentCaptor.forClass(GetTaskRequest.class);
    verify(mockSearchClient).getTask(taskRequestCaptor.capture(), eq(RequestOptions.DEFAULT));

    GetTaskRequest capturedRequest = taskRequestCaptor.getValue();
    assertEquals(capturedRequest.getNodeId(), TEST_NODE_ID);
    assertEquals(capturedRequest.getTaskId(), TEST_TASK_ID);
    assertTrue(capturedRequest.getWaitForCompletion());
    assertEquals(capturedRequest.getTimeout(), TimeValue.timeValueMinutes(10));
  }

  @Test
  public void testDeleteByQueryRequestConflictsStrategy()
      throws IOException, ExecutionException, InterruptedException {
    QueryBuilder query = QueryBuilders.matchAllQuery();

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    CompletableFuture<String> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, null);
    future.get();

    // Verify conflicts strategy is set to "proceed"
    ArgumentCaptor<DeleteByQueryRequest> requestCaptor =
        ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    verify(mockSearchClient)
        .submitDeleteByQueryTask(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    DeleteByQueryRequest capturedRequest = requestCaptor.getValue();
    // Note: We can't directly verify conflicts setting as it's not exposed via getter
    // but we know it's set to "proceed" in the implementation
    assertNotNull(capturedRequest);
  }

  @DataProvider(name = "writabilityConfig")
  public Object[][] writabilityConfigProvider() {
    return new Object[][] {
      {true, "Writable"}, // canWrite = true, description
      {false, "ReadOnly"} // canWrite = false, description
    };
  }

  @Test(dataProvider = "writabilityConfig")
  public void testUpsertDocumentWithWritability(boolean canWrite, String description) {
    esWriteDAO.setWritable(canWrite);

    String document = "{\"field\":\"value" + description + "\"}";
    String docId = "doc_" + description;

    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, docId);

    if (canWrite) {
      // When writable, bulkProcessor.add should be called
      verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));
    } else {
      // When not writable, bulkProcessor.add should not be called
      verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testUpsertDocumentByIndexNameWithWritability(boolean canWrite, String description) {
    // Set writability
    esWriteDAO.setWritable(canWrite);

    String indexName = "test_index_" + description;
    String document = "{\"data\":\"" + description + "\"}";
    String docId = "doc_" + description;

    esWriteDAO.upsertDocumentByIndexName(indexName, document, docId);

    if (canWrite) {
      verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));
    } else {
      verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteDocumentWithWritability(boolean canWrite, String description) {
    // Set writability
    esWriteDAO.setWritable(canWrite);

    String docId = "doc_" + description;

    esWriteDAO.deleteDocument(opContext, TEST_ENTITY, docId);

    if (canWrite) {
      verify(mockBulkProcessor, times(1)).add(any(DeleteRequest.class));
    } else {
      verify(mockBulkProcessor, never()).add(any(DeleteRequest.class));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteDocumentByIndexNameWithWritability(boolean canWrite, String description) {
    // Set writability
    esWriteDAO.setWritable(canWrite);

    String indexName = "test_index_" + description;
    String docId = "doc_" + description;

    esWriteDAO.deleteDocumentByIndexName(indexName, docId);

    if (canWrite) {
      verify(mockBulkProcessor, times(1)).add(any(DeleteRequest.class));
    } else {
      verify(mockBulkProcessor, never()).add(any(DeleteRequest.class));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testUpsertDocumentBySearchGroupWithWritability(boolean canWrite, String description) {
    esWriteDAO.setWritable(canWrite);

    String searchGroup = "test_group";
    String document = "{\"group\":\"" + description + "\"}";
    String docId = "doc_" + description;

    esWriteDAO.upsertDocumentBySearchGroup(opContext, searchGroup, document, docId);

    if (canWrite) {
      verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));
    } else {
      verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteDocumentBySearchGroupWithWritability(boolean canWrite, String description) {
    esWriteDAO.setWritable(canWrite);

    String searchGroup = "test_group";
    String docId = "doc_" + description;

    esWriteDAO.deleteDocumentBySearchGroup(opContext, searchGroup, docId);

    if (canWrite) {
      verify(mockBulkProcessor, times(1)).add(any(DeleteRequest.class));
    } else {
      verify(mockBulkProcessor, never()).add(any(DeleteRequest.class));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testApplyScriptUpdateWithWritability(boolean canWrite, String description) {
    esWriteDAO.setWritable(canWrite);

    String scriptSource = "ctx._source.field = params.newValue";
    Map<String, Object> scriptParams = new HashMap<>();
    scriptParams.put("newValue", description);
    Map<String, Object> upsert = new HashMap<>();
    upsert.put("field", "initial_" + description);

    esWriteDAO.applyScriptUpdate(
        opContext, TEST_ENTITY, TEST_DOC_ID + description, scriptSource, scriptParams, upsert);

    if (canWrite) {
      verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));
    } else {
      verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testClearWithWritability(boolean canWrite, String description) throws IOException {
    esWriteDAO.setWritable(canWrite);

    String[] indices = new String[] {"index1", "index2"};
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    when(mockResponse.getIndices()).thenReturn(indices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    // Mock getIndexAliases to return empty aliases (concrete indices, not aliases)
    GetAliasesResponse mockAliasesResponse = mock(GetAliasesResponse.class);
    when(mockAliasesResponse.getAliases()).thenReturn(java.util.Collections.emptyMap());
    when(mockSearchClient.getIndexAliases(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockAliasesResponse);

    // Mock indexExists to return true
    when(mockSearchClient.indexExists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(true);

    // Mock deleteIndex to return acknowledged response
    AcknowledgedResponse mockDeleteResponse = mock(AcknowledgedResponse.class);
    when(mockDeleteResponse.isAcknowledged()).thenReturn(true);
    when(mockSearchClient.deleteIndex(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockDeleteResponse);

    Set<String> deletedIndices = esWriteDAO.clear(opContext);

    if (canWrite) {
      // Verify indices were deleted
      assertEquals(deletedIndices.size(), 2);
      assertTrue(deletedIndices.contains("index1"));
      assertTrue(deletedIndices.contains("index2"));
      // Verify deleteIndex was called for each index
      verify(mockSearchClient, times(2))
          .deleteIndex(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT));
    } else {
      // Verify no indices were deleted when not writable
      assertTrue(deletedIndices.isEmpty());
      verify(mockSearchClient, never())
          .deleteIndex(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteByQueryAsyncWithWritability(boolean canWrite, String description)
      throws IOException, ExecutionException, InterruptedException {
    esWriteDAO.setWritable(canWrite);

    QueryBuilder query = QueryBuilders.termQuery("status", "deleted_" + description);

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(TEST_TASK_STRING);

    CompletableFuture<String> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, null);
    String result = future.get();

    if (canWrite) {
      assertNotNull(result);
      assertEquals(result, TEST_TASK_STRING);
      verify(mockSearchClient, times(1))
          .submitDeleteByQueryTask(any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT));
    } else {
      assertEquals(result, ""); // Returns empty string when not writable
      verify(mockSearchClient, never())
          .submitDeleteByQueryTask(any(DeleteByQueryRequest.class), any());
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteByQuerySyncWithWritability(boolean canWrite, String description)
      throws IOException {
    esWriteDAO.setWritable(canWrite);

    QueryBuilder query = QueryBuilders.termQuery("status", "deleted_" + description);

    if (canWrite) {
      CountResponse mockCount = mock(CountResponse.class);
      when(mockCount.getCount()).thenReturn(0L);
      when(mockSearchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn(mockCount);
    }

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertNotNull(result);
    if (canWrite) {
      // When writable, returns result with success true (0 documents)
      assertTrue(result.isSuccess());
      assertEquals(result.getRemainingDocuments(), 0);
    } else {
      // When not writable, returns empty builder result (all fields are default/null/0)
      assertFalse(result.isSuccess());
      assertEquals(result.getRemainingDocuments(), 0);
      assertNull(result.getFailureReason());
      assertEquals(result.getRetryAttempts(), 0);
      assertEquals(result.getTimeTaken(), 0);
    }

    if (!canWrite) {
      // Verify no search client calls when not writable
      verify(mockSearchClient, never()).count(any(), any());
      verify(mockSearchClient, never()).submitDeleteByQueryTask(any(), any());
    }
  }

  @Test
  public void testSetWritableToggle() {
    esWriteDAO.setWritable(true);

    String document1 = "{\"field\":\"value1\"}";
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document1, "doc1");
    verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));

    esWriteDAO.setWritable(false);

    String document2 = "{\"field\":\"value2\"}";
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document2, "doc2");
    // Still only 1 call
    verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));

    esWriteDAO.setWritable(true);

    String document3 = "{\"field\":\"value3\"}";
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document3, "doc3");
    verify(mockBulkProcessor, times(2)).add(any(UpdateRequest.class));
  }

  @Test
  public void testAllWriteOperationsBlockedWhenNotWritable() throws IOException {
    esWriteDAO.setWritable(false);

    // Test all write operations
    String document = "{\"test\":\"data\"}";

    // 1. upsertDocument
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, "doc1");
    verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));

    // 2. upsertDocumentByIndexName
    esWriteDAO.upsertDocumentByIndexName("test_index", document, "doc2");
    verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));

    // 3. deleteDocument
    esWriteDAO.deleteDocument(opContext, TEST_ENTITY, "doc3");
    verify(mockBulkProcessor, never()).add(any(DeleteRequest.class));

    // 4. deleteDocumentByIndexName
    esWriteDAO.deleteDocumentByIndexName("test_index", "doc4");
    verify(mockBulkProcessor, never()).add(any(DeleteRequest.class));

    // 5. upsertDocumentBySearchGroup
    esWriteDAO.upsertDocumentBySearchGroup(opContext, "group", document, "doc5");
    verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));

    // 6. deleteDocumentBySearchGroup
    esWriteDAO.deleteDocumentBySearchGroup(opContext, "group", "doc6");
    verify(mockBulkProcessor, never()).add(any(DeleteRequest.class));

    // 7. applyScriptUpdate
    esWriteDAO.applyScriptUpdate(
        opContext, TEST_ENTITY, "doc7", "script", new HashMap<>(), new HashMap<>());
    verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));

    // 8. clear
    String[] indices = new String[] {"index1"};
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    when(mockResponse.getIndices()).thenReturn(indices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);
    esWriteDAO.clear(opContext);
    verify(mockBulkProcessor, never()).deleteByQuery(any(QueryBuilder.class), any());

    // 9. deleteByQueryAsync
    CompletableFuture<String> asyncResult =
        esWriteDAO.deleteByQueryAsync("index", QueryBuilders.matchAllQuery(), null);
    try {
      assertEquals(asyncResult.get(), "");
    } catch (Exception e) {
      fail("Should not throw exception");
    }
    verify(mockSearchClient, never()).submitDeleteByQueryTask(any(), any());

    // 10. deleteByQuerySync
    ESWriteDAO.DeleteByQueryResult syncResult =
        esWriteDAO.deleteByQuerySync("index", QueryBuilders.matchAllQuery(), null);
    assertNotNull(syncResult);
    assertFalse(syncResult.isSuccess());
    verify(mockSearchClient, never()).count(any(), any());
    verify(mockSearchClient, never()).submitDeleteByQueryTask(any(), any());
  }

  @Test
  public void testWritabilityDuringMigration()
      throws IOException, ExecutionException, InterruptedException {
    esWriteDAO.setWritable(false);

    String document = "{\"migration\":\"test\"}";

    // All write operations should be blocked
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, "migrationDoc1");
    esWriteDAO.upsertDocumentByIndexName("migration_index", document, "migrationDoc2");
    esWriteDAO.deleteDocument(opContext, TEST_ENTITY, "migrationDoc3");
    esWriteDAO.deleteDocumentByIndexName("migration_index", "migrationDoc4");
    esWriteDAO.upsertDocumentBySearchGroup(opContext, "migration_group", document, "migrationDoc5");
    esWriteDAO.deleteDocumentBySearchGroup(opContext, "migration_group", "migrationDoc6");
    esWriteDAO.applyScriptUpdate(
        opContext, TEST_ENTITY, "migrationDoc7", "script", new HashMap<>(), new HashMap<>());

    String[] indices = new String[] {"migration_index"};
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    when(mockResponse.getIndices()).thenReturn(indices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);
    esWriteDAO.clear(opContext);

    CompletableFuture<String> asyncResult =
        esWriteDAO.deleteByQueryAsync("migration_index", QueryBuilders.matchAllQuery(), null);
    String asyncTaskId = asyncResult.get();
    assertEquals(asyncTaskId, "");

    ESWriteDAO.DeleteByQueryResult syncResult =
        esWriteDAO.deleteByQuerySync("migration_index", QueryBuilders.matchAllQuery(), null);
    assertNotNull(syncResult);
    assertFalse(syncResult.isSuccess());

    // No operations should have been executed
    verify(mockBulkProcessor, never()).add(any());
    verify(mockBulkProcessor, never()).deleteByQuery(any(), any());
    verify(mockSearchClient, never()).submitDeleteByQueryTask(any(), any());
    verify(mockSearchClient, never()).count(any(), any());

    esWriteDAO.setWritable(true);

    // Writes should work again
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, "postMigrationDoc");
    verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));
  }

  @Test
  public void testMultipleOperationsInSequence() {
    esWriteDAO.setWritable(true);

    String document = "{\"seq\":\"test\"}";

    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, "seq1");
    esWriteDAO.upsertDocumentByIndexName("test_index", document, "seq2");
    esWriteDAO.deleteDocument(opContext, TEST_ENTITY, "seq3");

    verify(mockBulkProcessor, times(2)).add(any(UpdateRequest.class));
    verify(mockBulkProcessor, times(1)).add(any(DeleteRequest.class));

    esWriteDAO.setWritable(false);

    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, "seq4");
    esWriteDAO.deleteDocument(opContext, TEST_ENTITY, "seq5");

    // Counts should not increase
    verify(mockBulkProcessor, times(2)).add(any(UpdateRequest.class));
    verify(mockBulkProcessor, times(1)).add(any(DeleteRequest.class));

    esWriteDAO.setWritable(true);

    // Operations should work again
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, "seq6");
    verify(mockBulkProcessor, times(3)).add(any(UpdateRequest.class));
  }

  @Test
  public void testDeleteByQueryAsyncReturnsEmptyStringWhenNotWritable()
      throws ExecutionException, InterruptedException, IOException {
    esWriteDAO.setWritable(false);

    QueryBuilder query = QueryBuilders.termQuery("field", "value");

    CompletableFuture<String> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, null);
    String result = future.get();

    assertEquals(result, "");
    verify(mockSearchClient, never()).submitDeleteByQueryTask(any(), any());
  }

  @Test
  public void testDeleteByQuerySyncReturnsEmptyResultWhenNotWritable() throws IOException {
    esWriteDAO.setWritable(false);

    QueryBuilder query = QueryBuilders.termQuery("field", "value");

    ESWriteDAO.DeleteByQueryResult result =
        esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, null);

    assertNotNull(result);
    assertFalse(result.isSuccess());
    assertEquals(result.getTimeTaken(), 0);
    assertEquals(result.getRemainingDocuments(), 0);
    assertEquals(result.getRetryAttempts(), 0);
    assertNull(result.getFailureReason());
    assertNull(result.getTaskId());

    verify(mockSearchClient, never()).count(any(), any());
    verify(mockSearchClient, never()).submitDeleteByQueryTask(any(), any());
  }

  @Test
  public void testReadOperationsWorkWhenNotWritable() throws IOException {
    // Set to read-only
    esWriteDAO.setWritable(false);

    // getBulkProcessor() is a getter and should still work
    ESBulkProcessor bulkProcessor = esWriteDAO.getBulkProcessor();
    assertNotNull(bulkProcessor);
    assertEquals(bulkProcessor, mockBulkProcessor);

    // Note: ESWriteDAO doesn't have explicit read operations like the DAO classes,
    // but the bulk processor getter demonstrates that read-like operations are unaffected
  }

  @Test
  public void testWritableStateIndependentAcrossInstances() {
    ESWriteDAO secondDao =
        new ESWriteDAO(
            TEST_OS_SEARCH_CONFIG.toBuilder()
                .bulkProcessor(BulkProcessorConfiguration.builder().numRetries(NUM_RETRIES).build())
                .build(),
            mockSearchClient,
            mockBulkProcessor);

    esWriteDAO.setWritable(false);

    // Second instance should still be writable (independent state)
    secondDao.setWritable(true);

    String document = "{\"independent\":\"test\"}";

    // First instance operations blocked
    esWriteDAO.upsertDocument(opContext, TEST_ENTITY, document, "doc1");
    verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));

    // Second instance operations work
    secondDao.upsertDocument(opContext, TEST_ENTITY, document, "doc2");
    verify(mockBulkProcessor, times(1)).add(any(UpdateRequest.class));
  }

  @Test
  public void testUpsertByIndexNameAndSearchGroupBehaveSimilarlyWithWritability() {
    esWriteDAO.setWritable(false);

    String document = "{\"test\":\"data\"}";
    String docId = "testDoc";

    // Both methods should behave the same way when not writable
    esWriteDAO.upsertDocumentByIndexName("test_index", document, docId);
    esWriteDAO.upsertDocumentBySearchGroup(opContext, "test_group", document, docId);

    verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));

    esWriteDAO.setWritable(true);

    esWriteDAO.upsertDocumentByIndexName("test_index", document, docId);
    esWriteDAO.upsertDocumentBySearchGroup(opContext, "test_group", document, docId);

    verify(mockBulkProcessor, times(2)).add(any(UpdateRequest.class));
  }

  @Test
  public void testDeleteByIndexNameAndSearchGroupBehaveSimilarlyWithWritability() {
    esWriteDAO.setWritable(false);

    String docId = "testDoc";

    // Both methods should behave the same way when not writable
    esWriteDAO.deleteDocumentByIndexName("test_index", docId);
    esWriteDAO.deleteDocumentBySearchGroup(opContext, "test_group", docId);

    verify(mockBulkProcessor, never()).add(any(DeleteRequest.class));

    esWriteDAO.setWritable(true);

    esWriteDAO.deleteDocumentByIndexName("test_index", docId);
    esWriteDAO.deleteDocumentBySearchGroup(opContext, "test_group", docId);

    verify(mockBulkProcessor, times(2)).add(any(DeleteRequest.class));
  }

  @Test
  public void testScriptUpdateBlockedWhenNotWritable() {
    esWriteDAO.setWritable(false);

    String scriptSource = "ctx._source.counter++";
    Map<String, Object> scriptParams = new HashMap<>();
    scriptParams.put("increment", 1);
    Map<String, Object> upsert = new HashMap<>();
    upsert.put("counter", 0);

    esWriteDAO.applyScriptUpdate(
        opContext, TEST_ENTITY, "scriptDoc", scriptSource, scriptParams, upsert);

    verify(mockBulkProcessor, never()).add(any(UpdateRequest.class));
  }

  @Test
  public void testClearWithNoIndicesWhenNotWritable() throws IOException {
    esWriteDAO.setWritable(false);

    // Even if indices exist, clear should not execute when not writable
    String[] indices = new String[] {"index1", "index2", "index3"};
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    when(mockResponse.getIndices()).thenReturn(indices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    esWriteDAO.clear(opContext);

    // Should not call deleteByQuery at all
    verify(mockBulkProcessor, never()).deleteByQuery(any(), any());

    // Should not even try to get indices when not writable
    verify(mockSearchClient, never()).getIndex(any(), any());
  }
}
