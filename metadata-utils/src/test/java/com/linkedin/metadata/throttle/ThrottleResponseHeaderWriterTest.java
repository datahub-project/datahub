package com.linkedin.metadata.throttle;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class ThrottleResponseHeaderWriterTest {

  @Test
  public void testCreateDenialHeaders() {
    Map<String, String> headers =
        ThrottleResponseHeaderWriter.createDenialHeaders(
            "MCL_VERSIONED_LAG",
            ThrottleMechanismType.INGEST,
            ThrottleResponseSource.METADATA_WRITE,
            5000L);

    assertEquals(headers.get(ThrottleResponseHeaders.RULE), "MCL_VERSIONED_LAG");
    assertEquals(headers.get(ThrottleResponseHeaders.TYPE), "ingest");
    assertEquals(headers.get(ThrottleResponseHeaders.SOURCE), "metadata-write");
    assertEquals(headers.get(ThrottleResponseHeaders.RETRY_AFTER), "5");
  }

  @Test
  public void testApplyDenialSkipsBlankRuleAndNegativeRetryAfter() {
    Map<String, String> headers = new LinkedHashMap<>();
    ThrottleResponseHeaderWriter.applyDenial(
        headers, " ", ThrottleMechanismType.SEARCH, ThrottleResponseSource.OPENSEARCH, -1L);

    assertFalse(headers.containsKey(ThrottleResponseHeaders.RULE));
    assertFalse(headers.containsKey(ThrottleResponseHeaders.RETRY_AFTER));
    assertEquals(headers.get(ThrottleResponseHeaders.TYPE), "search");
    assertEquals(headers.get(ThrottleResponseHeaders.SOURCE), "opensearch");
  }
}
