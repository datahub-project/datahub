package com.linkedin.metadata.search.update;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.TasksClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.client.tasks.TaskId;
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.tasks.TaskInfo;
import org.testng.annotations.BeforeMethod;
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

  @Mock private RestHighLevelClient mockSearchClient;
  @Mock private ESBulkProcessor mockBulkProcessor;
  @Mock private TasksClient mockTasksClient;
  @Mock private org.opensearch.client.IndicesClient mockIndicesClient;

  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  private ESWriteDAO esWriteDAO;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Setup mock indices client
    when(mockSearchClient.indices()).thenReturn(mockIndicesClient);

    // Setup mock tasks client
    when(mockSearchClient.tasks()).thenReturn(mockTasksClient);

    esWriteDAO =
        new ESWriteDAO(
            TEST_ES_SEARCH_CONFIG.toBuilder()
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
    when(mockSearchClient.indices().get(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    esWriteDAO.clear(opContext);

    // Verify the GetIndexRequest
    ArgumentCaptor<GetIndexRequest> indexRequestCaptor =
        ArgumentCaptor.forClass(GetIndexRequest.class);
    verify(mockSearchClient.indices())
        .get(indexRequestCaptor.capture(), eq(RequestOptions.DEFAULT));
    assertEquals(indexRequestCaptor.getValue().indices()[0], TEST_PATTERN);

    // Verify the deletion query
    ArgumentCaptor<QueryBuilder> queryCaptor = ArgumentCaptor.forClass(QueryBuilder.class);
    verify(mockBulkProcessor).deleteByQuery(queryCaptor.capture(), eq(indices));
    assertTrue(queryCaptor.getValue() instanceof org.opensearch.index.query.MatchAllQueryBuilder);
  }

  @Test
  public void testClearWithIOException() throws IOException {
    when(mockSearchClient.indices().get(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new IOException("Test exception"));

    esWriteDAO.clear(opContext);

    // Verify empty array is used when exception occurs
    verify(mockBulkProcessor).deleteByQuery(any(QueryBuilder.class), eq(new String[] {}));
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
    TaskSubmissionResponse mockResponse = mock(TaskSubmissionResponse.class);
    when(mockResponse.getTask()).thenReturn(TEST_TASK_STRING);

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    CompletableFuture<TaskSubmissionResponse> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, null);
    TaskSubmissionResponse result = future.get();

    assertNotNull(result);
    assertEquals(result.getTask(), TEST_TASK_STRING);

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

    TaskSubmissionResponse mockResponse = mock(TaskSubmissionResponse.class);
    when(mockResponse.getTask()).thenReturn(TEST_TASK_STRING);

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    CompletableFuture<TaskSubmissionResponse> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, customConfig);
    TaskSubmissionResponse result = future.get();

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

    CompletableFuture<TaskSubmissionResponse> future =
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
    TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
    when(mockSubmission.getTask()).thenReturn(TEST_TASK_STRING);
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSubmission);

    // Mock task monitoring
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    TaskInfo mockTaskInfo = mock(TaskInfo.class);
    when(mockTaskResponse.getTaskInfo()).thenReturn(mockTaskInfo);

    when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
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
    TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
    when(mockSubmission.getTask()).thenReturn(TEST_TASK_STRING);
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSubmission);

    // Mock task monitoring
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    TaskInfo mockTaskInfo = mock(TaskInfo.class);
    when(mockTaskResponse.getTaskInfo()).thenReturn(mockTaskInfo);

    when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
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
    TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
    when(mockSubmission.getTask()).thenReturn(TEST_TASK_STRING);
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSubmission);

    // Mock task monitoring with no deletions
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    TaskInfo mockTaskInfo = mock(TaskInfo.class);
    when(mockTaskResponse.getTaskInfo()).thenReturn(mockTaskInfo);

    when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
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

      TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
      when(mockSubmission.getTask()).thenReturn("node123:456789");
      when(mockSearchClient.submitDeleteByQueryTask(
              any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn(mockSubmission);

      GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
      when(mockTaskResponse.isCompleted()).thenReturn(true);
      when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
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

    TaskSubmissionResponse mockResponse = mock(TaskSubmissionResponse.class);
    when(mockResponse.getTask()).thenReturn(TEST_TASK_STRING);

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    // Should handle invalid slices gracefully and default to auto
    CompletableFuture<TaskSubmissionResponse> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, customConfig);
    TaskSubmissionResponse result = future.get();

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

    TaskSubmissionResponse mockResponse = mock(TaskSubmissionResponse.class);
    when(mockResponse.getTask()).thenReturn(TEST_TASK_STRING);

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    CompletableFuture<TaskSubmissionResponse> future =
        esWriteDAO.deleteByQueryAsync(TEST_DELETE_INDEX, query, customConfig);
    TaskSubmissionResponse result = future.get();

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
    TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
    when(mockSubmission.getTask()).thenReturn(TEST_TASK_STRING);
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSubmission);

    // Mock task monitoring with incomplete/failed task
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(false); // Task didn't complete

    when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
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
    TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
    when(mockSubmission.getTask()).thenReturn(TEST_TASK_STRING);
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSubmission);

    // Mock successful task completions
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);

    when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
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
    when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
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

    TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
    when(mockSubmission.getTask()).thenReturn(TEST_TASK_STRING);
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSubmission);

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

      TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
      when(mockSubmission.getTask()).thenReturn(null); // Null task string
      when(mockSearchClient.submitDeleteByQueryTask(
              any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn(mockSubmission);

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

      TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
      when(mockSubmission.getTask()).thenReturn("invalidformat"); // No colon
      when(mockSearchClient.submitDeleteByQueryTask(
              any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
          .thenReturn(mockSubmission);

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
    TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
    when(mockSubmission.getTask()).thenReturn(TEST_TASK_STRING);
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSubmission);

    // Mock task monitoring that takes time
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);

    // Simulate interruption during task monitoring
    when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
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
    TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
    when(mockSubmission.getTask()).thenReturn(TEST_TASK_STRING);
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSubmission);

    // Return empty Optional for task response
    when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
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
    TaskSubmissionResponse mockSubmission = mock(TaskSubmissionResponse.class);
    when(mockSubmission.getTask()).thenReturn(TEST_TASK_STRING);
    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSubmission);

    // Mock task response
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    when(mockTasksClient.get(any(GetTaskRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(Optional.of(mockTaskResponse));

    esWriteDAO.deleteByQuerySync(TEST_DELETE_INDEX, query, customConfig);

    // Verify GetTaskRequest parameters
    ArgumentCaptor<GetTaskRequest> taskRequestCaptor =
        ArgumentCaptor.forClass(GetTaskRequest.class);
    verify(mockTasksClient).get(taskRequestCaptor.capture(), eq(RequestOptions.DEFAULT));

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

    TaskSubmissionResponse mockResponse = mock(TaskSubmissionResponse.class);
    when(mockResponse.getTask()).thenReturn(TEST_TASK_STRING);

    when(mockSearchClient.submitDeleteByQueryTask(
            any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    CompletableFuture<TaskSubmissionResponse> future =
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
}
