package com.linkedin.metadata.graph.elastic.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.models.registry.LineageRegistry.EdgeInfo;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.testng.annotations.Test;

public class GraphFilterUtilsTest {

  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset,PROD)");

  @Test
  public void testGetAggregationFilterOutgoing() {
    String entityType = "dataset";
    EdgeInfo edgeInfo = createMockEdgeInfo("DownstreamOf", "table");
    Pair<String, EdgeInfo> pair = Pair.of(entityType, edgeInfo);

    BoolQueryBuilder result =
        GraphFilterUtils.getAggregationFilter(pair, RelationshipDirection.OUTGOING);

    assertNotNull(result);
    assertEquals(result.filter().size(), 3);

    // Check relationship type filter
    TermQueryBuilder relationshipTypeFilter = (TermQueryBuilder) result.filter().get(0);
    assertEquals(relationshipTypeFilter.fieldName(), "relationshipType");
    assertEquals(relationshipTypeFilter.value(), "DownstreamOf");

    // Check source type filter (should be entityType for OUTGOING)
    TermQueryBuilder sourceTypeFilter = (TermQueryBuilder) result.filter().get(1);
    assertEquals(sourceTypeFilter.fieldName(), "source.entityType");
    assertEquals(sourceTypeFilter.value(), "dataset");

    // Check destination type filter (should be opposingEntityType for OUTGOING)
    TermQueryBuilder destinationTypeFilter = (TermQueryBuilder) result.filter().get(2);
    assertEquals(destinationTypeFilter.fieldName(), "destination.entityType");
    assertEquals(destinationTypeFilter.value(), "table");
  }

  @Test
  public void testGetAggregationFilterIncoming() {
    String entityType = "table";
    EdgeInfo edgeInfo = createMockEdgeInfo("DownstreamOf", "dataset");
    Pair<String, EdgeInfo> pair = Pair.of(entityType, edgeInfo);

    BoolQueryBuilder result =
        GraphFilterUtils.getAggregationFilter(pair, RelationshipDirection.INCOMING);

    assertNotNull(result);
    assertEquals(result.filter().size(), 3);

    // Check relationship type filter
    TermQueryBuilder relationshipTypeFilter = (TermQueryBuilder) result.filter().get(0);
    assertEquals(relationshipTypeFilter.fieldName(), "relationshipType");
    assertEquals(relationshipTypeFilter.value(), "DownstreamOf");

    // Check source type filter (should be opposingEntityType for INCOMING)
    TermQueryBuilder sourceTypeFilter = (TermQueryBuilder) result.filter().get(1);
    assertEquals(sourceTypeFilter.fieldName(), "source.entityType");
    assertEquals(sourceTypeFilter.value(), "dataset");

    // Check destination type filter (should be entityType for INCOMING)
    TermQueryBuilder destinationTypeFilter = (TermQueryBuilder) result.filter().get(2);
    assertEquals(destinationTypeFilter.fieldName(), "destination.entityType");
    assertEquals(destinationTypeFilter.value(), "table");
  }

  @Test
  public void testGetUrnStatusQuerySource() {
    QueryBuilder result = GraphFilterUtils.getUrnStatusQuery(EdgeUrnType.SOURCE, TEST_URN, true);

    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;

    assertEquals(boolQuery.filter().size(), 2);

    // Check URN filter
    TermQueryBuilder urnFilter = (TermQueryBuilder) boolQuery.filter().get(0);
    assertEquals(urnFilter.fieldName(), "source.urn");
    assertEquals(urnFilter.value(), TEST_URN.toString());

    // Check status filter
    TermQueryBuilder statusFilter = (TermQueryBuilder) boolQuery.filter().get(1);
    assertEquals(statusFilter.fieldName(), "source.removed");
    assertEquals(statusFilter.value(), "true");
  }

  @Test
  public void testGetUrnStatusQueryDestination() {
    QueryBuilder result =
        GraphFilterUtils.getUrnStatusQuery(EdgeUrnType.DESTINATION, TEST_URN, false);

    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;

    assertEquals(boolQuery.filter().size(), 1);
    assertEquals(boolQuery.should().size(), 2);
    assertEquals(boolQuery.minimumShouldMatch(), "1");

    // Check URN filter
    TermQueryBuilder urnFilter = (TermQueryBuilder) boolQuery.filter().get(0);
    assertEquals(urnFilter.fieldName(), "destination.urn");
    assertEquals(urnFilter.value(), TEST_URN.toString());

    // Check status filter (should clause)
    TermQueryBuilder statusFilter = (TermQueryBuilder) boolQuery.should().get(0);
    assertEquals(statusFilter.fieldName(), "destination.removed");
    assertEquals(statusFilter.value(), "false");
  }

  @Test
  public void testGetUrnStatusQueryVia() {
    QueryBuilder result = GraphFilterUtils.getUrnStatusQuery(EdgeUrnType.VIA, TEST_URN, true);

    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;

    assertEquals(boolQuery.filter().size(), 2);

    // Check URN filter
    TermQueryBuilder urnFilter = (TermQueryBuilder) boolQuery.filter().get(0);
    assertEquals(urnFilter.fieldName(), "via");
    assertEquals(urnFilter.value(), TEST_URN.toString());

    // Check status filter
    TermQueryBuilder statusFilter = (TermQueryBuilder) boolQuery.filter().get(1);
    assertEquals(statusFilter.fieldName(), "viaRemoved");
    assertEquals(statusFilter.value(), "true");
  }

  @Test
  public void testGetUrnStatusQueryLifecycleOwner() {
    QueryBuilder result =
        GraphFilterUtils.getUrnStatusQuery(EdgeUrnType.LIFECYCLE_OWNER, TEST_URN, false);

    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;

    assertEquals(boolQuery.filter().size(), 1);
    assertEquals(boolQuery.should().size(), 2);
    assertEquals(boolQuery.minimumShouldMatch(), "1");

    // Check URN filter
    TermQueryBuilder urnFilter = (TermQueryBuilder) boolQuery.filter().get(0);
    assertEquals(urnFilter.fieldName(), "lifecycleOwner");
    assertEquals(urnFilter.value(), TEST_URN.toString());

    // Check status filter (should clause)
    TermQueryBuilder statusFilter = (TermQueryBuilder) boolQuery.should().get(0);
    assertEquals(statusFilter.fieldName(), "lifecycleOwnerRemoved");
    assertEquals(statusFilter.value(), "false");
  }

  @Test
  public void testGetUrnStatusFieldName() {
    assertEquals(GraphFilterUtils.getUrnStatusFieldName(EdgeUrnType.SOURCE), "source.removed");
    assertEquals(
        GraphFilterUtils.getUrnStatusFieldName(EdgeUrnType.DESTINATION), "destination.removed");
    assertEquals(GraphFilterUtils.getUrnStatusFieldName(EdgeUrnType.VIA), "viaRemoved");
    assertEquals(
        GraphFilterUtils.getUrnStatusFieldName(EdgeUrnType.LIFECYCLE_OWNER),
        "lifecycleOwnerRemoved");
  }

  @Test
  public void testGetUrnFieldName() {
    assertEquals(GraphFilterUtils.getUrnFieldName(EdgeUrnType.SOURCE), "source.urn");
    assertEquals(GraphFilterUtils.getUrnFieldName(EdgeUrnType.DESTINATION), "destination.urn");
    assertEquals(GraphFilterUtils.getUrnFieldName(EdgeUrnType.VIA), "via");
    assertEquals(GraphFilterUtils.getUrnFieldName(EdgeUrnType.LIFECYCLE_OWNER), "lifecycleOwner");
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testGetUrnStatusFieldNameInvalidType() {
    // Test with a null EdgeUrnType to trigger the default case
    GraphFilterUtils.getUrnStatusFieldName(null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testGetUrnFieldNameInvalidType() {
    // Test with a null EdgeUrnType to trigger the default case
    GraphFilterUtils.getUrnFieldName(null);
  }

  @Test
  public void testGetEdgeTimeFilterQuery() {
    long startTime = 1000L;
    long endTime = 2000L;

    QueryBuilder result = GraphFilterUtils.getEdgeTimeFilterQuery(startTime, endTime);

    assertTrue(result instanceof BoolQueryBuilder);
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;
    assertEquals(boolQuery.minimumShouldMatch(), "1");
    assertEquals(boolQuery.should().size(), 3);
  }

  private EdgeInfo createMockEdgeInfo(String type, String opposingEntityType) {
    return new EdgeInfo(type, RelationshipDirection.OUTGOING, opposingEntityType);
  }
}
