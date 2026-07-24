package com.linkedin.metadata.utils.elasticsearch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import org.testng.annotations.Test;

public class TaskFailureParserTest {

  private static final String SAMPLE_TASK_JSON =
      "{"
          + "\"completed\":true,"
          + "\"task\":{\"node\":\"node1\",\"id\":123,\"type\":\"transport\",\"action\":\"indices:data/write/reindex\"},"
          + "\"response\":{"
          + "\"took\":100,"
          + "\"total\":5,"
          + "\"created\":3,"
          + "\"failures\":["
          + "{\"index\":\"my_index\",\"id\":\"doc1\",\"cause\":{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse field [foo]\"},\"status\":400,\"shard\":2}"
          + "]"
          + "}"
          + "}";

  @Test
  public void testParseSampleTaskJson() {
    TaskFailureParseResult parsed = TaskFailureParser.parse(SAMPLE_TASK_JSON);

    assertEquals(parsed.details().size(), 1);
    assertEquals(parsed.totalCount(), 1);
    assertEquals(parsed.rawFallback(), null);
    TaskFailureDetail failure = parsed.details().get(0);
    assertEquals(failure.index(), "my_index");
    assertEquals(failure.documentId(), "doc1");
    assertEquals(failure.causeType(), "mapper_parsing_exception");
    assertEquals(failure.causeReason(), "failed to parse field [foo]");
    assertEquals(failure.shard(), 2);
  }

  @Test
  public void testParseNullReturnsEmpty() {
    assertEquals(TaskFailureParser.parse(null), TaskFailureParseResult.EMPTY);
  }

  @Test
  public void testParseEmptyStringReturnsEmpty() {
    assertEquals(TaskFailureParser.parse(""), TaskFailureParseResult.EMPTY);
  }

  @Test
  public void testParseMissingFailuresNodeReturnsEmpty() {
    String json = "{\"completed\":true,\"response\":{\"took\":100,\"total\":5}}";
    assertEquals(TaskFailureParser.parse(json), TaskFailureParseResult.EMPTY);
  }

  @Test
  public void testParseMalformedJsonRetainsRawFallback() {
    String malformed = "{not valid json";
    TaskFailureParseResult parsed = TaskFailureParser.parse(malformed);

    assertTrue(parsed.details().isEmpty());
    assertEquals(parsed.totalCount(), 0);
    assertEquals(parsed.rawFallback(), malformed);
    assertTrue(!parsed.isEmpty());

    String formatted = TaskFailureParser.formatForLog(parsed, 5);
    assertTrue(formatted.contains("RAW task response (parse incomplete):"));
    assertTrue(formatted.contains(malformed));
  }

  @Test
  public void testParseTruncatesLongReason() {
    String longReason = "x".repeat(600);
    String json =
        "{\"response\":{\"failures\":[{\"index\":\"i\",\"id\":\"d\",\"cause\":{\"type\":\"t\",\"reason\":\""
            + longReason
            + "\"},\"shard\":0}]}}";

    TaskFailureParseResult parsed = TaskFailureParser.parse(json);

    assertEquals(parsed.details().size(), 1);
    assertEquals(parsed.details().get(0).causeReason().length(), 500);
    assertEquals(parsed.rawFallback(), null);
  }

  @Test
  public void testParseCapsAtMaxTenFailuresButKeepsTotalCount() {
    StringBuilder failuresArray = new StringBuilder("[");
    for (int i = 0; i < 15; i++) {
      if (i > 0) {
        failuresArray.append(",");
      }
      failuresArray.append(
          String.format(
              "{\"index\":\"i%d\",\"id\":\"d%d\",\"cause\":{\"type\":\"t\",\"reason\":\"r\"},\"shard\":0}",
              i, i));
    }
    failuresArray.append("]");
    String json = "{\"response\":{\"failures\":" + failuresArray + "}}";

    TaskFailureParseResult parsed = TaskFailureParser.parse(json);

    assertEquals(parsed.details().size(), 10);
    assertEquals(parsed.totalCount(), 15);
    assertEquals(parsed.rawFallback(), null);
    assertTrue(parsed.details().get(0).documentId().equals("d0"));
    assertTrue(parsed.details().get(9).documentId().equals("d9"));
  }

  @Test
  public void testFormatForLogEmpty() {
    assertEquals(TaskFailureParser.formatForLog((List<TaskFailureDetail>) null, 5), "");
    assertEquals(TaskFailureParser.formatForLog(List.of(), 5), "");
    assertEquals(TaskFailureParser.formatForLog((TaskFailureParseResult) null, 5), "");
    assertEquals(TaskFailureParser.formatForLog(TaskFailureParseResult.EMPTY, 5), "");
  }

  @Test
  public void testFormatForLogCapsDetailAtLimit() {
    List<TaskFailureDetail> failures =
        List.of(
            new TaskFailureDetail("i", "d0", "t", "r", 0),
            new TaskFailureDetail("i", "d1", "t", "r", 1),
            new TaskFailureDetail("i", "d2", "t", "r", 2));
    String formatted = TaskFailureParser.formatForLog(failures, 2);
    assertTrue(formatted.contains("documentFailures=3"));
    assertTrue(formatted.contains("doc=d0"));
    assertTrue(formatted.contains("doc=d1"));
    assertTrue(!formatted.contains("doc=d2"));
  }

  @Test
  public void testFormatForLogUsesExplicitTotalWhenCapped() {
    List<TaskFailureDetail> capped =
        List.of(
            new TaskFailureDetail("i", "d0", "t", "r", 0),
            new TaskFailureDetail("i", "d1", "t", "r", 1));
    String formatted = TaskFailureParser.formatForLog(capped, 2, 15);
    assertTrue(formatted.contains("documentFailures=15"));
    assertTrue(formatted.contains("doc=d0"));
    assertTrue(formatted.contains("doc=d1"));
  }

  @Test
  public void testFormatForLogParseResultUsesTotalCount() {
    TaskFailureParseResult parsed =
        new TaskFailureParseResult(List.of(new TaskFailureDetail("i", "d0", "t", "r", 0)), 12);
    String formatted = TaskFailureParser.formatForLog(parsed, 5);
    assertTrue(formatted.contains("documentFailures=12"));
    assertTrue(formatted.contains("doc=d0"));
    assertTrue(!formatted.contains("RAW"));
  }

  @Test
  public void testFormatForLogPrefersStructuredOverRawFallback() {
    TaskFailureParseResult parsed =
        new TaskFailureParseResult(
            List.of(new TaskFailureDetail("i", "d0", "t", "r", 0)), 1, "{\"raw\":true}");
    String formatted = TaskFailureParser.formatForLog(parsed, 5);
    assertTrue(formatted.contains("documentFailures=1"));
    assertTrue(!formatted.contains("RAW"));
  }

  @Test
  public void testRawFallbackTruncatedAtMaxLength() {
    String huge = "y".repeat(5000);
    TaskFailureParseResult parsed = TaskFailureParser.parse(huge);
    assertTrue(parsed.details().isEmpty());
    assertEquals(parsed.rawFallback().length(), 4000);
  }
}
