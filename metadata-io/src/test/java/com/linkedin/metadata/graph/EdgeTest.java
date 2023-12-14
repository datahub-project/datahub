package com.linkedin.metadata.graph;

import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import java.util.Collections;
import org.testng.annotations.Test;

public class EdgeTest {
  private static final String SOURCE_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:foo,source1,PROD)";
  private static final String SOURCE_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:foo,source2,PROD)";
  private static final String DESTINATION_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:foo,destination1,PROD)";
  private static final String DESTINATION_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:foo,destination2,PROD)";
  private static final String DOWNSTREAM_RELATIONSHIP_TYPE = "DownstreamOf";
  private static final Long TIMESTAMP_1 = 1L;
  private static final Long TIMESTAMP_2 = 2L;
  private static final String ACTOR_URN_1 = "urn:li:corpuser:actor1";
  private static final String ACTOR_URN_2 = "urn:li:corpuser:actor2";

  @Test
  public void testEdgeEquals() {
    // First edge
    final Edge edge1 =
        new Edge(
            UrnUtils.getUrn(SOURCE_URN_1),
            UrnUtils.getUrn(DESTINATION_URN_1),
            DOWNSTREAM_RELATIONSHIP_TYPE,
            TIMESTAMP_1,
            UrnUtils.getUrn(ACTOR_URN_1),
            TIMESTAMP_1,
            UrnUtils.getUrn(ACTOR_URN_2),
            Collections.emptyMap());

    // Second edge has same source, destination, and relationship type as edge1, and should be
    // considered the same edge.
    // All other fields are different.
    final Edge edge2 =
        new Edge(
            UrnUtils.getUrn(SOURCE_URN_1),
            UrnUtils.getUrn(DESTINATION_URN_1),
            DOWNSTREAM_RELATIONSHIP_TYPE,
            TIMESTAMP_2,
            UrnUtils.getUrn(ACTOR_URN_2),
            TIMESTAMP_2,
            UrnUtils.getUrn(ACTOR_URN_2),
            Collections.emptyMap());
    assertEquals(edge1, edge2);

    // Third edge has different source and destination as edge1, and thus is not the same edge.
    final Edge edge3 =
        new Edge(
            UrnUtils.getUrn(SOURCE_URN_2),
            UrnUtils.getUrn(DESTINATION_URN_2),
            DOWNSTREAM_RELATIONSHIP_TYPE,
            TIMESTAMP_1,
            UrnUtils.getUrn(ACTOR_URN_1),
            TIMESTAMP_1,
            UrnUtils.getUrn(ACTOR_URN_1),
            Collections.emptyMap());
    assertNotEquals(edge1, edge3);
  }
}
