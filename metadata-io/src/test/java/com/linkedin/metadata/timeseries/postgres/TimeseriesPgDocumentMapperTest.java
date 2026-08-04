package com.linkedin.metadata.timeseries.postgres;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Map;
import org.testng.annotations.Test;

public class TimeseriesPgDocumentMapperTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final ObjectMapper mapper = opContext.getObjectMapper();

  @Test
  public void envelopedAspectFromRow_usesEventAndSystemMetadataColumns() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("event")).thenReturn("{\"stat\":42}");
    when(rs.getString("system_metadata")).thenReturn("{\"runId\":\"run-1\"}");

    EnvelopedAspect aspect =
        TimeseriesPgDocumentMapper.envelopedAspectFromRow(opContext, rs, false);

    assertNotNull(aspect.getAspect());
    String payload = aspect.getAspect().getValue().asString(StandardCharsets.UTF_8);
    assertTrue(payload.contains("\"stat\":42"));
    assertEquals(aspect.getSystemMetadata().getRunId(), "run-1");
  }

  @Test
  public void envelopedAspectFromRow_preferDocumentColumn_withEventField() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("event")).thenReturn(null);
    when(rs.getString("system_metadata")).thenReturn(null);
    when(rs.getString("document"))
        .thenReturn(
            "{\"urn\":\"urn:li:dataset:x\",\"event\":{\"stat\":7},"
                + "\"systemMetadata\":{\"runId\":\"doc-run\"}}");

    EnvelopedAspect aspect = TimeseriesPgDocumentMapper.envelopedAspectFromRow(opContext, rs, true);

    String payload = aspect.getAspect().getValue().asString(StandardCharsets.UTF_8);
    assertTrue(payload.contains("\"stat\":7"));
    assertEquals(aspect.getSystemMetadata().getRunId(), "doc-run");
  }

  @Test
  public void envelopedAspectFromRow_preferDocumentColumn_syntheticEventFromExploded()
      throws Exception {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("event")).thenReturn(null);
    when(rs.getString("system_metadata")).thenReturn(null);
    when(rs.getString("document"))
        .thenReturn(
            "{\""
                + MappingsBuilder.URN_FIELD
                + "\":\"urn:li:dataset:x\",\""
                + MappingsBuilder.IS_EXPLODED_FIELD
                + "\":true,\"customField\":\"value\"}");

    EnvelopedAspect aspect = TimeseriesPgDocumentMapper.envelopedAspectFromRow(opContext, rs, true);

    String payload = aspect.getAspect().getValue().asString(StandardCharsets.UTF_8);
    assertTrue(payload.contains("customField"));
    assertFalse(payload.contains(MappingsBuilder.IS_EXPLODED_FIELD));
  }

  @Test
  public void envelopedAspectFromRow_badEventJson_throws() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("event")).thenReturn("{not-json");
    when(rs.getString("system_metadata")).thenReturn(null);

    expectThrows(
        IllegalStateException.class,
        () -> TimeseriesPgDocumentMapper.envelopedAspectFromRow(opContext, rs, false));
  }

  @Test
  public void rawDocumentMap_nullBlankAndBadJson() {
    assertNull(TimeseriesPgDocumentMapper.rawDocumentMap(mapper, null));
    assertNull(TimeseriesPgDocumentMapper.rawDocumentMap(mapper, "  "));
    assertNull(TimeseriesPgDocumentMapper.rawDocumentMap(mapper, "{bad"));

    Map<String, Object> map =
        TimeseriesPgDocumentMapper.rawDocumentMap(mapper, "{\"urn\":\"urn:li:dataset:x\"}");
    assertNotNull(map);
    assertEquals(map.get("urn"), "urn:li:dataset:x");
  }
}
