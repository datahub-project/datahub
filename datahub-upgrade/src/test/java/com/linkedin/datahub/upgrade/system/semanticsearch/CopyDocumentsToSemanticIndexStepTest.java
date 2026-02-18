package com.linkedin.datahub.upgrade.system.semanticsearch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.tasks.TaskInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CopyDocumentsToSemanticIndexStepTest {

  @Mock private SearchClientShim<?> searchClient;

  @Mock private EntityService<?> entityService;

  @Mock private IndexConvention indexConvention;

  @Mock private UpgradeContext upgradeContext;

  private OperationContext opContext;

  private CopyDocumentsToSemanticIndexStep step;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    opContext = TestOperationContexts.systemContextNoValidate();
    when(upgradeContext.opContext()).thenReturn(opContext);
  }

  @Test
  public void testExecutable_Success() throws Exception {
    String entityName = "dataset";
    String baseIndexName = "datasetindex_v2";
    String semanticIndexName = "datasetindex_v2_semantic_search";
    String taskId = "node123:456789";

    when(indexConvention.getEntityIndexName(entityName)).thenReturn(baseIndexName);
    when(indexConvention.getEntityIndexNameSemantic(entityName)).thenReturn(semanticIndexName);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class)))
        .thenReturn(taskId);

    // Mock task completion
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    TaskInfo mockTaskInfo = mock(TaskInfo.class);
    when(mockTaskInfo.isCancelled()).thenReturn(false);
    when(mockTaskResponse.getTaskInfo()).thenReturn(mockTaskInfo);
    when(searchClient.getTask(any(GetTaskRequest.class), any(RequestOptions.class)))
        .thenReturn(Optional.of(mockTaskResponse));

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    assertEquals(result.stepId(), "CopyDocumentsToSemanticIndex_dataset");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(searchClient).indexExists(any(GetIndexRequest.class), any(RequestOptions.class));
    verify(searchClient).submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class));
    verify(searchClient).getTask(any(GetTaskRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testExecutable_SemanticIndexDoesNotExist() throws Exception {
    String entityName = "dataset";
    String baseIndexName = "datasetindex_v2";
    String semanticIndexName = "datasetindex_v2_semantic_search";

    when(indexConvention.getEntityIndexName(entityName)).thenReturn(baseIndexName);
    when(indexConvention.getEntityIndexNameSemantic(entityName)).thenReturn(semanticIndexName);
    // Semantic index does not exist
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(false);

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    assertEquals(result.stepId(), "CopyDocumentsToSemanticIndex_dataset");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);

    // Verify reindex task was NOT submitted
    verify(searchClient, never())
        .submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testExecutable_TaskCancelled() throws Exception {
    String entityName = "chart";
    String baseIndexName = "chartindex_v2";
    String semanticIndexName = "chartindex_v2_semantic_search";
    String taskId = "node456:789012";

    when(indexConvention.getEntityIndexName(entityName)).thenReturn(baseIndexName);
    when(indexConvention.getEntityIndexNameSemantic(entityName)).thenReturn(semanticIndexName);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class)))
        .thenReturn(taskId);

    // Mock task completion with cancellation
    GetTaskResponse mockTaskResponse = mock(GetTaskResponse.class);
    when(mockTaskResponse.isCompleted()).thenReturn(true);
    TaskInfo mockTaskInfo = mock(TaskInfo.class);
    when(mockTaskInfo.isCancelled()).thenReturn(true); // Task was cancelled
    when(mockTaskResponse.getTaskInfo()).thenReturn(mockTaskInfo);
    when(searchClient.getTask(any(GetTaskRequest.class), any(RequestOptions.class)))
        .thenReturn(Optional.of(mockTaskResponse));

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    assertEquals(result.stepId(), "CopyDocumentsToSemanticIndex_chart");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_InvalidTaskIdFormat() throws Exception {
    String entityName = "dashboard";
    String baseIndexName = "dashboardindex_v2";
    String semanticIndexName = "dashboardindex_v2_semantic_search";
    String taskId = "invalid-task-id-without-colon"; // Invalid format

    when(indexConvention.getEntityIndexName(entityName)).thenReturn(baseIndexName);
    when(indexConvention.getEntityIndexNameSemantic(entityName)).thenReturn(semanticIndexName);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class)))
        .thenReturn(taskId);

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    assertEquals(result.stepId(), "CopyDocumentsToSemanticIndex_dashboard");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);

    // Task was never polled because ID format is invalid
    verify(searchClient, never()).getTask(any(GetTaskRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testExecutable_TaskNotFound() throws Exception {
    String entityName = "dataset";
    String baseIndexName = "datasetindex_v2";
    String semanticIndexName = "datasetindex_v2_semantic_search";
    String taskId = "node123:456789";

    when(indexConvention.getEntityIndexName(entityName)).thenReturn(baseIndexName);
    when(indexConvention.getEntityIndexNameSemantic(entityName)).thenReturn(semanticIndexName);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class)))
        .thenReturn(taskId);

    // Task not found (empty optional - may have completed and been cleaned up)
    when(searchClient.getTask(any(GetTaskRequest.class), any(RequestOptions.class)))
        .thenReturn(Optional.empty());

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    assertEquals(result.stepId(), "CopyDocumentsToSemanticIndex_dataset");
    // Task not found is treated as success (assumes completed and cleaned up)
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutable_ReindexThrowsException() throws Exception {
    String entityName = "dataset";
    String baseIndexName = "datasetindex_v2";
    String semanticIndexName = "datasetindex_v2_semantic_search";

    when(indexConvention.getEntityIndexName(entityName)).thenReturn(baseIndexName);
    when(indexConvention.getEntityIndexNameSemantic(entityName)).thenReturn(semanticIndexName);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Connection refused"));

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    assertEquals(result.stepId(), "CopyDocumentsToSemanticIndex_dataset");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_GetTaskThrowsException() throws Exception {
    String entityName = "dataset";
    String baseIndexName = "datasetindex_v2";
    String semanticIndexName = "datasetindex_v2_semantic_search";
    String taskId = "node123:456789";

    when(indexConvention.getEntityIndexName(entityName)).thenReturn(baseIndexName);
    when(indexConvention.getEntityIndexNameSemantic(entityName)).thenReturn(semanticIndexName);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class)))
        .thenReturn(taskId);

    // getTask throws exception
    when(searchClient.getTask(any(GetTaskRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Task service unavailable"));

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    assertEquals(result.stepId(), "CopyDocumentsToSemanticIndex_dataset");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testSkip_PreviousRunSucceeded() {
    String entityName = "dataset";
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(upgradeContext.upgrade()).thenReturn(mockUpgrade);

    // Previous run succeeded
    DataHubUpgradeResult previousResult =
        new DataHubUpgradeResult().setState(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(previousResult));

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testSkip_PreviousRunFailed() {
    String entityName = "dataset";
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(upgradeContext.upgrade()).thenReturn(mockUpgrade);

    // Previous run failed
    DataHubUpgradeResult previousResult =
        new DataHubUpgradeResult().setState(DataHubUpgradeState.FAILED);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(previousResult));

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    assertFalse(step.skip(upgradeContext));
  }

  @Test
  public void testSkip_NoPreviousRun() {
    String entityName = "dataset";
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(upgradeContext.upgrade()).thenReturn(mockUpgrade);

    // No previous run
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    assertFalse(step.skip(upgradeContext));
  }

  @Test
  public void testIsOptional() {
    String entityName = "dataset";

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    // Step should be optional
    assertTrue(step.isOptional());
  }

  @Test
  public void testId() {
    String entityName = "customEntity";

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    assertEquals(step.id(), "CopyDocumentsToSemanticIndex_customEntity");
  }

  @Test
  public void testExecutable_TaskStillRunningThenCompletes() throws Exception {
    String entityName = "dataset";
    String baseIndexName = "datasetindex_v2";
    String semanticIndexName = "datasetindex_v2_semantic_search";
    String taskId = "node123:456789";

    when(indexConvention.getEntityIndexName(entityName)).thenReturn(baseIndexName);
    when(indexConvention.getEntityIndexNameSemantic(entityName)).thenReturn(semanticIndexName);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class)))
        .thenReturn(taskId);

    // First call: task still running, second call: task completed
    GetTaskResponse runningResponse = mock(GetTaskResponse.class);
    when(runningResponse.isCompleted()).thenReturn(false);

    GetTaskResponse completedResponse = mock(GetTaskResponse.class);
    when(completedResponse.isCompleted()).thenReturn(true);
    TaskInfo mockTaskInfo = mock(TaskInfo.class);
    when(mockTaskInfo.isCancelled()).thenReturn(false);
    when(completedResponse.getTaskInfo()).thenReturn(mockTaskInfo);

    when(searchClient.getTask(any(GetTaskRequest.class), any(RequestOptions.class)))
        .thenReturn(Optional.of(runningResponse))
        .thenReturn(Optional.of(completedResponse));

    step =
        new CopyDocumentsToSemanticIndexStep(
            opContext, entityName, searchClient, entityService, indexConvention);

    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    assertEquals(result.stepId(), "CopyDocumentsToSemanticIndex_dataset");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }
}
