package com.linkedin.metadata.datahubusage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.datahubusage.event.EventSource;
import com.linkedin.metadata.datahubusage.event.UsageEventResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link DataHubUsageEventResultMapper#fromSourceMap}: source-map field copying,
 * type coercion, and rawUsageEvent preservation.
 */
public class DataHubUsageEventResultMapperTest {

  private OperationContext opContext;

  @BeforeClass
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testFromSourceMap_populatesAllSupportedFields() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put(DataHubUsageEventConstants.TYPE, "EntityEvent");
    source.put(DataHubUsageEventConstants.TIMESTAMP, 1_700_000_000_000L);
    source.put(DataHubUsageEventConstants.ACTOR_URN, "urn:li:corpuser:alice");
    source.put(DataHubUsageEventConstants.SOURCE_IP, "10.0.0.1");
    source.put(DataHubUsageEventConstants.EVENT_SOURCE, "GRAPHQL");
    source.put(DataHubUsageEventConstants.ENTITY_TYPE, "dataset");
    source.put(
        DataHubUsageEventConstants.ENTITY_URN, "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
    source.put(DataHubUsageEventConstants.ASPECT_NAME, "ownership");
    source.put(DataHubUsageEventConstants.TRACE_ID, "trace-1");
    source.put(DataHubUsageEventConstants.USER_AGENT, "Mozilla/5.0");

    UsageEventResult result = DataHubUsageEventResultMapper.fromSourceMap(opContext, source);

    assertNotNull(result);
    assertEquals(result.getEventType(), "EntityEvent");
    assertEquals(result.getTimestamp(), 1_700_000_000_000L);
    assertEquals(result.getActorUrn(), "urn:li:corpuser:alice");
    assertEquals(result.getSourceIP(), "10.0.0.1");
    assertEquals(result.getEventSource(), EventSource.GRAPHQL);
    assertEquals(result.getUserAgent(), "Mozilla/5.0");
    assertEquals(result.getTelemetryTraceId(), "trace-1");
    assertEquals(result.getRawUsageEvent().get(DataHubUsageEventConstants.TYPE), "EntityEvent");
  }

  @Test
  public void testFromSourceMap_handlesIntegerTimestamp() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put(DataHubUsageEventConstants.TYPE, "PageViewEvent");
    // Numbers can arrive as Integer, Long, or Double from JSONB roundtrip; mapper coerces via
    // Number#longValue.
    source.put(DataHubUsageEventConstants.TIMESTAMP, 1_700_000);

    UsageEventResult result = DataHubUsageEventResultMapper.fromSourceMap(opContext, source);
    assertEquals(result.getTimestamp(), 1_700_000L);
  }

  @Test
  public void testFromSourceMap_skipsMissingOptionalFields() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put(DataHubUsageEventConstants.TYPE, "SearchEvent");
    source.put(DataHubUsageEventConstants.TIMESTAMP, 1L);

    UsageEventResult result = DataHubUsageEventResultMapper.fromSourceMap(opContext, source);

    assertEquals(result.getEventType(), "SearchEvent");
    assertNull(result.getActorUrn());
    assertNull(result.getSourceIP());
    assertNull(result.getEventSource());
    assertNull(result.getUserAgent());
  }

  @Test
  public void testFromSourceMap_unknownEventSourceMapsToNull() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put(DataHubUsageEventConstants.TYPE, "Event");
    source.put(DataHubUsageEventConstants.TIMESTAMP, 1L);
    source.put(DataHubUsageEventConstants.EVENT_SOURCE, "DOES_NOT_EXIST");

    UsageEventResult result = DataHubUsageEventResultMapper.fromSourceMap(opContext, source);

    assertNull(
        result.getEventSource(),
        "EventSource.getSource returns null for unknown values; mapper must not throw");
  }

  @Test
  public void testFromSourceMap_rawUsageEventIsCopy() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put(DataHubUsageEventConstants.TYPE, "Event");
    source.put(DataHubUsageEventConstants.TIMESTAMP, 1L);
    source.put("custom", "value");

    UsageEventResult result = DataHubUsageEventResultMapper.fromSourceMap(opContext, source);

    assertNotNull(result.getRawUsageEvent());
    assertTrue(result.getRawUsageEvent().containsKey("custom"));
    assertEquals(result.getRawUsageEvent().get("custom"), "value");

    // Mutating the source after mapping must not affect the produced result.
    source.put("late", "added");
    assertTrue(
        !result.getRawUsageEvent().containsKey("late"),
        "rawUsageEvent should snapshot the source map");
  }
}
