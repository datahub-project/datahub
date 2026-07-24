package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.elasticsearch.TaskFailureDetail;
import java.util.List;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.tasks.TaskInfo;
import org.testng.annotations.Test;

/**
 * Tests for {@link ESIndexBuilder#describeReindexTaskStatus} log-formatting helpers used to surface
 * reindex task progress and document failures in operator-facing logs.
 */
public class ESIndexBuilderStatusFormatTest {

  @Test
  public void testDescribeWithoutTaskInfoUsesCompletedOnly() {
    GetTaskResponse response = mock(GetTaskResponse.class);
    when(response.isCompleted()).thenReturn(false);
    when(response.getTaskInfo()).thenReturn(null);

    String description = ESIndexBuilder.describeReindexTaskStatus(response);

    assertEquals(description, "completed=false");
  }

  @Test
  public void testDescribeWithTaskInfoNoFailures() {
    GetTaskResponse response = mock(GetTaskResponse.class);
    TaskInfo taskInfo = mock(TaskInfo.class);
    when(response.isCompleted()).thenReturn(true);
    when(response.getTaskInfo()).thenReturn(taskInfo);
    when(taskInfo.getAction()).thenReturn("indices:data/write/reindex");
    when(taskInfo.getDescription()).thenReturn("reindex from [src] to [dest]");
    when(taskInfo.getRunningTimeNanos()).thenReturn(5_000_000_000L);
    when(taskInfo.isCancelled()).thenReturn(false);

    String description = ESIndexBuilder.describeReindexTaskStatus(response);

    assertEquals(
        description,
        "completed=true action=indices:data/write/reindex description=reindex from [src] to [dest] runningTimeNanos=5000000000 cancelled=false");

    // Overload with an empty failures list must match the no-failures base description exactly.
    assertEquals(ESIndexBuilder.describeReindexTaskStatus(response, List.of()), description);
  }

  @Test
  public void testDescribeWithFailuresAppendsFailureDetail() {
    GetTaskResponse response = mock(GetTaskResponse.class);
    TaskInfo taskInfo = mock(TaskInfo.class);
    when(response.isCompleted()).thenReturn(true);
    when(response.getTaskInfo()).thenReturn(taskInfo);
    when(taskInfo.getAction()).thenReturn("indices:data/write/reindex");
    when(taskInfo.getDescription()).thenReturn("reindex from [src] to [dest]");
    when(taskInfo.getRunningTimeNanos()).thenReturn(1_000L);
    when(taskInfo.isCancelled()).thenReturn(false);

    List<TaskFailureDetail> failures =
        List.of(
            new TaskFailureDetail("dest", "doc1", "mapper_parsing_exception", "bad field", 0),
            new TaskFailureDetail("dest", "doc2", "mapper_parsing_exception", "bad field", 1));

    String description = ESIndexBuilder.describeReindexTaskStatus(response, failures);

    assertTrue(description.startsWith(ESIndexBuilder.describeReindexTaskStatus(response)));
    assertTrue(description.contains("documentFailures=2"));
    assertTrue(
        description.contains(
            "FAILED doc=doc1 index=dest cause=mapper_parsing_exception: bad field"));
    assertTrue(
        description.contains(
            "FAILED doc=doc2 index=dest cause=mapper_parsing_exception: bad field"));
  }

  @Test
  public void testDescribeWithFailuresLimitsDetailToFive() {
    GetTaskResponse response = mock(GetTaskResponse.class);
    TaskInfo taskInfo = mock(TaskInfo.class);
    when(response.isCompleted()).thenReturn(true);
    when(response.getTaskInfo()).thenReturn(taskInfo);
    when(taskInfo.getAction()).thenReturn("indices:data/write/reindex");
    when(taskInfo.getDescription()).thenReturn("reindex from [src] to [dest]");
    when(taskInfo.getRunningTimeNanos()).thenReturn(1_000L);
    when(taskInfo.isCancelled()).thenReturn(false);

    List<TaskFailureDetail> failures =
        List.of(
            new TaskFailureDetail("dest", "doc0", "t", "r", 0),
            new TaskFailureDetail("dest", "doc1", "t", "r", 1),
            new TaskFailureDetail("dest", "doc2", "t", "r", 2),
            new TaskFailureDetail("dest", "doc3", "t", "r", 3),
            new TaskFailureDetail("dest", "doc4", "t", "r", 4),
            new TaskFailureDetail("dest", "doc5", "t", "r", 5),
            new TaskFailureDetail("dest", "doc6", "t", "r", 6));

    String description = ESIndexBuilder.describeReindexTaskStatus(response, failures);

    assertTrue(description.contains("documentFailures=7"));
    assertTrue(description.contains("doc=doc4"));
    assertTrue(description.contains("doc=doc0"));
    assertTrue(!description.contains("doc=doc5"));
    assertTrue(!description.contains("doc=doc6"));
  }

  @Test
  public void testDescribeWithExplicitTotalCount() {
    GetTaskResponse response = mock(GetTaskResponse.class);
    TaskInfo taskInfo = mock(TaskInfo.class);
    when(response.isCompleted()).thenReturn(true);
    when(response.getTaskInfo()).thenReturn(taskInfo);
    when(taskInfo.getAction()).thenReturn("indices:data/write/reindex");
    when(taskInfo.getDescription()).thenReturn("reindex from [src] to [dest]");
    when(taskInfo.getRunningTimeNanos()).thenReturn(1_000L);
    when(taskInfo.isCancelled()).thenReturn(false);

    List<TaskFailureDetail> capped =
        List.of(
            new TaskFailureDetail("dest", "doc0", "t", "r", 0),
            new TaskFailureDetail("dest", "doc1", "t", "r", 1));

    String description = ESIndexBuilder.describeReindexTaskStatus(response, capped, 15);

    assertTrue(description.contains("documentFailures=15"));
    assertTrue(description.contains("doc=doc0"));
    assertTrue(description.contains("doc=doc1"));
  }

  @Test
  public void testDescribeReindexTaskStatus_rendersBulkByScrollCountersUnderCorrectLabels() {
    BulkByScrollTask.Status status = mock(BulkByScrollTask.Status.class);
    when(status.getTotal()).thenReturn(100L);
    when(status.getCreated()).thenReturn(30L);
    when(status.getUpdated()).thenReturn(20L);
    when(status.getDeleted()).thenReturn(4L);
    when(status.getVersionConflicts()).thenReturn(5L);
    when(status.getNoops()).thenReturn(6L);
    when(status.getBatches()).thenReturn(7);
    when(status.getBulkRetries()).thenReturn(8L);
    when(status.getSearchRetries()).thenReturn(9L);

    TaskInfo taskInfo = mock(TaskInfo.class);
    when(taskInfo.getStatus()).thenReturn(status);

    GetTaskResponse response = mock(GetTaskResponse.class);
    when(response.isCompleted()).thenReturn(true);
    when(response.getTaskInfo()).thenReturn(taskInfo);

    String rendered = ESIndexBuilder.describeReindexTaskStatus(response);

    assertTrue(rendered.contains("completed=true"), rendered);
    assertTrue(rendered.contains("total=100"), rendered);
    assertTrue(rendered.contains("created=30"), rendered);
    assertTrue(rendered.contains("updated=20"), rendered);
    assertTrue(rendered.contains("deleted=4"), rendered);
    assertTrue(rendered.contains("versionConflicts=5"), rendered);
    assertTrue(rendered.contains("noops=6"), rendered);
    assertTrue(rendered.contains("batches=7"), rendered);
    assertTrue(rendered.contains("bulkRetries=8"), rendered);
    assertTrue(rendered.contains("searchRetries=9"), rendered);
  }
}
