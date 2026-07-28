package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.tasks.TaskInfo;
import org.testng.annotations.Test;

public class ESIndexBuilderStatusFormatTest {

  /**
   * The reindex-task counters (especially {@code versionConflicts} and {@code created} vs {@code
   * total}) are what make a dropped-document shortfall diagnosable, so verify each counter is
   * rendered under the right label. Distinct values per counter mean a getter mix-up (e.g. swapping
   * {@code created}/{@code updated}) fails the assertion.
   */
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
