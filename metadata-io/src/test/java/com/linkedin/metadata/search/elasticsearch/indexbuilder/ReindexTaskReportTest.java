package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

/** Cases ported from #18570's TaskFailureParserTest, plus sliced and chained-cause responses. */
public class ReindexTaskReportTest {

  private static final String SAMPLE_TASK_JSON =
      "{"
          + "\"completed\":true,"
          + "\"task\":{\"node\":\"node1\",\"id\":123,\"type\":\"transport\","
          + "\"action\":\"indices:data/write/reindex\"},"
          + "\"response\":{"
          + "\"took\":100,"
          + "\"total\":5,"
          + "\"created\":3,"
          + "\"failures\":["
          + "{\"index\":\"my_index\",\"id\":\"doc1\",\"cause\":{\"type\":\"mapper_parsing_exception\","
          + "\"reason\":\"failed to parse field [foo]\"},\"status\":400,\"shard\":2}"
          + "]"
          + "}"
          + "}";

  @Test
  public void testParseSampleTaskJson() {
    ReindexTaskReport report = ReindexTaskReport.parse("node1:123", SAMPLE_TASK_JSON);

    assertNull(report.rawFallback());
    assertEquals(report.completed(), true);
    assertEquals(report.counts().get("created"), 3);
    assertEquals(report.totalFailures(), 1);
    ReindexTaskReport.Failure failure = report.failures().get(0);
    assertEquals(failure.taskId(), "node1:123");
    assertEquals(failure.docId(), "doc1");
    assertEquals(failure.status(), 400);
    assertEquals(failure.shard(), 2);
    assertEquals(failure.field(), "foo");
    assertEquals(failure.causes(), "mapper_parsing_exception: failed to parse field [foo]");
  }

  @Test
  public void testParseNullAndEmptyFallBackToRaw() {
    assertNotNull(ReindexTaskReport.parse("t:1", null).rawFallback());
    assertEquals(ReindexTaskReport.parse("t:1", "").rawFallback(), "");
  }

  @Test
  public void testParseMissingFailuresNode() {
    ReindexTaskReport report =
        ReindexTaskReport.parse(
            "t:1", "{\"completed\":true,\"response\":{\"took\":100,\"total\":5}}");

    assertNull(report.rawFallback());
    assertEquals(report.totalFailures(), 0);
    assertTrue(report.failures().isEmpty());
    assertEquals(report.counts().get("total"), 5);
  }

  @Test
  public void testRunningTaskUsesStatusCounters() {
    ReindexTaskReport report =
        ReindexTaskReport.parse(
            "t:1", "{\"completed\":false,\"task\":{\"status\":{\"total\":9,\"created\":4}}}");

    assertEquals(report.completed(), false);
    assertEquals(report.counts().get("created"), 4);
  }

  @Test
  public void testParseMalformedJsonRetainsRawFallback() {
    ReindexTaskReport report = ReindexTaskReport.parse("t:1", "{not valid json");

    assertEquals(report.rawFallback(), "{not valid json");
    assertTrue(report.failures().isEmpty());
    assertEquals(report.totalFailures(), 0);
  }

  @Test
  public void testUnexpectedShapeRetainsRawFallback() {
    // Valid JSON, but "response" is not an object.
    ReindexTaskReport report = ReindexTaskReport.parse("t:1", "{\"response\":[1,2]}");

    assertEquals(report.rawFallback(), "{\"response\":[1,2]}");
  }

  @Test
  public void testRawFallbackTruncated() {
    ReindexTaskReport report = ReindexTaskReport.parse("t:1", "y".repeat(5000));

    assertTrue(
        report.rawFallback().startsWith("y".repeat(ReindexTaskReport.MAX_RAW_FALLBACK_CHARS)));
    assertTrue(report.rawFallback().endsWith("... (truncated)"));
    assertTrue(report.rawFallback().length() < ReindexTaskReport.MAX_RAW_FALLBACK_CHARS + 20);
  }

  @Test
  public void testParseTruncatesLongReason() {
    String json =
        "{\"response\":{\"failures\":[{\"index\":\"i\",\"id\":\"d\",\"cause\":{\"type\":\"t\","
            + "\"reason\":\""
            + "x".repeat(600)
            + "\"},\"shard\":0}]}}";

    ReindexTaskReport report = ReindexTaskReport.parse("t:1", json);

    assertEquals(report.failures().size(), 1);
    // "t: " + 500 characters + "..."
    assertEquals(report.failures().get(0).causes().length(), 3 + 500 + 3);
  }

  @Test
  public void testParseCapsFailuresButKeepsTotalCount() {
    StringBuilder failures = new StringBuilder();
    for (int i = 0; i < 25; i++) {
      failures
          .append(i == 0 ? "" : ",")
          .append(
              String.format(
                  "{\"index\":\"i\",\"id\":\"d%d\",\"cause\":{\"type\":\"t\",\"reason\":\"r\"}}",
                  i));
    }

    ReindexTaskReport report =
        ReindexTaskReport.parse("t:1", "{\"response\":{\"failures\":[" + failures + "]}}");

    assertEquals(report.failures().size(), ReindexTaskReport.MAX_PARSED_FAILURES);
    assertEquals(report.totalFailures(), 25);
    assertEquals(report.failures().get(0).docId(), "d0");
    assertEquals(report.failures().get(19).docId(), "d19");
  }

  @Test
  public void testParseCollectsFailuresFromSlices() {
    String json =
        "{\"response\":{\"failures\":[{\"id\":\"top\",\"cause\":{\"type\":\"t\",\"reason\":\"r\"}}],"
            + "\"slices\":[null,"
            + "{\"failures\":[{\"id\":\"s1\",\"cause\":{\"type\":\"t\",\"reason\":\"r\"}}]},"
            + "{\"failures\":[{\"shard\":3,\"reason\":{\"type\":\"search\",\"reason\":\"down\"}}]}]}}";

    ReindexTaskReport report = ReindexTaskReport.parse("t:1", json);

    assertEquals(report.totalFailures(), 3);
    assertEquals(report.failures().get(1).docId(), "s1");
    assertNull(report.failures().get(2).docId());
    assertEquals(report.failures().get(2).shard(), 3);
    assertEquals(report.failures().get(2).causes(), "search: down");
  }

  @Test
  public void testParseFlattensCauseChainAndFindsQuotedField() {
    String json =
        "{\"response\":{\"failures\":[{\"id\":\"d\",\"cause\":{\"type\":\"document_parsing_exception\","
            + "\"reason\":\"[1:2] failed to parse\",\"caused_by\":{\"type\":\"illegal_argument_exception\","
            + "\"reason\":\"Document contains at least one immense term in field=\\\"name.keyword\\\"\"}}}]}}";

    ReindexTaskReport.Failure failure = ReindexTaskReport.parse("t:1", json).failures().get(0);

    assertTrue(failure.causes().contains(" <- illegal_argument_exception: "), failure.causes());
    assertEquals(failure.field(), "name.keyword");
  }

  @Test
  public void testTaskLevelError() {
    ReindexTaskReport report =
        ReindexTaskReport.parse(
            "t:1", "{\"completed\":true,\"error\":{\"type\":\"task_cancelled_exception\"}}");

    assertNotNull(report.error());
    assertEquals(report.totalFailures(), 0);
  }
}
