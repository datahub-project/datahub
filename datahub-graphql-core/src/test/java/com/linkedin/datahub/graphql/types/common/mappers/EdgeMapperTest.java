package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.generated.EntityEdge;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class EdgeMapperTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)";
  private static final String ACTOR_URN = "urn:li:corpuser:test";
  private static final long TIMESTAMP = 1_000_000L;

  @Test
  public void testMapResolvableDestination() throws URISyntaxException {
    Edge edge = new Edge().setDestinationUrn(Urn.createFromString(DATASET_URN));

    EntityEdge result = EdgeMapper.map(null, edge);

    assertNotNull(result);
    assertNotNull(result.getDestination());
    assertEquals(result.getDestination().getUrn(), DATASET_URN);
  }

  @Test
  public void testMapUnresolvableDestinationReturnsNull() throws URISyntaxException {
    // "unknownEntity" is not handled by UrnToEntityMapper, so destination resolves to null.
    Edge edge = new Edge().setDestinationUrn(Urn.createFromString("urn:li:unknownEntity:someId"));

    EntityEdge result = EdgeMapper.map(null, edge);

    assertNull(result);
  }

  @Test
  public void testMapListFiltersUnresolvableEdges() throws URISyntaxException {
    Edge resolvable = new Edge().setDestinationUrn(Urn.createFromString(DATASET_URN));
    Edge unresolvable =
        new Edge().setDestinationUrn(Urn.createFromString("urn:li:unknownEntity:someId"));

    List<EntityEdge> results = EdgeMapper.mapList(null, Arrays.asList(resolvable, unresolvable));

    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getDestination().getUrn(), DATASET_URN);
  }

  @Test
  public void testMapListNullReturnsEmpty() {
    List<EntityEdge> results = EdgeMapper.mapList(null, null);
    assertNotNull(results);
    assertTrue(results.isEmpty());
  }

  @Test
  public void testMapListEmptyReturnsEmpty() {
    List<EntityEdge> results = EdgeMapper.mapList(null, Collections.emptyList());
    assertNotNull(results);
    assertTrue(results.isEmpty());
  }

  @Test
  public void testMapAuditStamps() throws URISyntaxException {
    AuditStamp created =
        new AuditStamp().setActor(Urn.createFromString(ACTOR_URN)).setTime(TIMESTAMP);
    AuditStamp lastModified =
        new AuditStamp().setActor(Urn.createFromString(ACTOR_URN)).setTime(TIMESTAMP + 1_000L);

    Edge edge =
        new Edge()
            .setDestinationUrn(Urn.createFromString(DATASET_URN))
            .setCreated(created)
            .setLastModified(lastModified);

    EntityEdge result = EdgeMapper.map(null, edge);

    assertNotNull(result);
    assertNotNull(result.getCreated());
    assertEquals(result.getCreated().getActor(), ACTOR_URN);
    assertEquals((long) result.getCreated().getTime(), TIMESTAMP);
    assertNotNull(result.getLastModified());
    assertEquals(result.getLastModified().getActor(), ACTOR_URN);
    assertEquals((long) result.getLastModified().getTime(), TIMESTAMP + 1_000L);
  }

  @Test
  public void testMapProperties() throws URISyntaxException {
    StringMap props = new StringMap();
    props.put("key1", "value1");
    Edge edge =
        new Edge().setDestinationUrn(Urn.createFromString(DATASET_URN)).setProperties(props);

    EntityEdge result = EdgeMapper.map(null, edge);

    assertNotNull(result);
    List<StringMapEntry> entries = result.getProperties();
    assertNotNull(entries);
    assertEquals(entries.size(), 1);
    assertEquals(entries.get(0).getKey(), "key1");
    assertEquals(entries.get(0).getValue(), "value1");
  }
}
