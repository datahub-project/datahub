package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.utils.elasticsearch.TaskFailureDetail;
import com.linkedin.metadata.utils.elasticsearch.TaskResultWithFailures;
import java.io.IOException;
import java.util.Optional;
import org.testng.annotations.Test;

public class TaskWithFailuresRawResponseTest {

  private static final String SAMPLE_GET_TASK_JSON =
      "{"
          + "\"completed\":true,"
          + "\"task\":{"
          + "\"node\":\"node1\","
          + "\"id\":123,"
          + "\"type\":\"transport\","
          + "\"action\":\"indices:data/write/reindex\","
          + "\"start_time_in_millis\":0,"
          + "\"running_time_in_nanos\":0,"
          + "\"cancellable\":true"
          + "},"
          + "\"response\":{"
          + "\"took\":100,"
          + "\"timed_out\":false,"
          + "\"total\":5,"
          + "\"updated\":0,"
          + "\"created\":3,"
          + "\"deleted\":0,"
          + "\"batches\":1,"
          + "\"version_conflicts\":0,"
          + "\"noops\":0,"
          + "\"retries\":{\"bulk\":0,\"search\":0},"
          + "\"throttled_millis\":0,"
          + "\"requests_per_second\":-1.0,"
          + "\"throttled_until_millis\":0,"
          + "\"failures\":["
          + "{\"index\":\"my_index\",\"id\":\"doc1\","
          + "\"cause\":{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse field [foo]\"},"
          + "\"status\":400,\"shard\":2}"
          + "]"
          + "}"
          + "}";

  @Test
  public void fromRawJson_parsesFailuresIntoTaskResult() throws IOException {
    TaskResultWithFailures result = TaskWithFailuresRawResponse.fromRawJson(SAMPLE_GET_TASK_JSON);

    assertFalse(result.failureParse().isEmpty());
    assertEquals(result.failures().size(), 1);
    TaskFailureDetail failure = result.failures().get(0);
    assertEquals(failure.index(), "my_index");
    assertEquals(failure.documentId(), "doc1");
    assertEquals(failure.causeType(), "mapper_parsing_exception");
  }

  @Test
  public void fromEntity_nullReturnsEmpty() throws IOException {
    assertEquals(TaskWithFailuresRawResponse.fromEntity(null), Optional.empty());
  }

  @Test
  public void emptyIfNotFound_404ReturnsEmpty() throws IOException {
    Optional<TaskResultWithFailures> result =
        TaskWithFailuresRawResponse.emptyIfNotFound(404, new IOException("not found"));
    assertEquals(result, Optional.empty());
  }

  @Test
  public void emptyIfNotFound_non404Rethrows() {
    IOException boom = new IOException("server error");
    IOException thrown =
        expectThrows(
            IOException.class, () -> TaskWithFailuresRawResponse.emptyIfNotFound(500, boom));
    assertTrue(thrown == boom);
  }
}
