package com.linkedin.metadata.graph.search;

import static com.linkedin.metadata.graph.elastic.GraphFilterUtils.getUrnFieldName;
import static com.linkedin.metadata.graph.elastic.GraphFilterUtils.getUrnStatusFieldName;
import static com.linkedin.metadata.graph.elastic.GraphFilterUtils.getUrnStatusQuery;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.io.Resources;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.graph.elastic.GraphFilterUtils;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.testng.annotations.Test;

public class GraphFilterUtilsTest {
  private static final Urn TEST_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)");

  private static final String TEST_QUERY_FILE =
      "elasticsearch/sample_filters/lineage_time_query_filters_1.json";

  @Test
  public void testGetEdgeTimeFilterQuery() throws Exception {
    URL url = Resources.getResource(TEST_QUERY_FILE);
    String expectedQuery = Resources.toString(url, StandardCharsets.UTF_8);
    long startTime = 1L;
    long endTime = 2L;
    QueryBuilder result = GraphFilterUtils.getEdgeTimeFilterQuery(startTime, endTime);
    assertEquals(result.toString(), expectedQuery);
  }

  @Test
  public void testGetUrnStatusQuery() {
    for (EdgeUrnType edgeUrnType : EdgeUrnType.values()) {
      for (Boolean removed : List.of(true, false)) {
        QueryBuilder test = getUrnStatusQuery(edgeUrnType, TEST_URN, removed);

        assertTrue(test instanceof BoolQueryBuilder);
        BoolQueryBuilder testBoolQuery = (BoolQueryBuilder) test;

        final TermQueryBuilder urnQueryBuilder;
        final TermQueryBuilder statusQueryBuilder;

        if (removed) {
          assertEquals(
              testBoolQuery.should().size(),
              0,
              String.format("type:%s removed:%s", edgeUrnType, true));
          assertNull(
              testBoolQuery.minimumShouldMatch(),
              "Minimum should match not expected for empty should queries");
          assertEquals(
              testBoolQuery.must().size(),
              0,
              String.format("type:%s removed:%s", edgeUrnType, true));
          assertEquals(
              testBoolQuery.filter().size(),
              2,
              String.format("type:%s removed:%s", edgeUrnType, true));
          assertEquals(
              testBoolQuery.mustNot().size(),
              0,
              String.format("type:%s removed:%s", edgeUrnType, true));

          urnQueryBuilder = (TermQueryBuilder) testBoolQuery.filter().get(0);
          statusQueryBuilder = (TermQueryBuilder) testBoolQuery.filter().get(1);
        } else {
          assertEquals(
              testBoolQuery.should().size(),
              2,
              String.format("type:%s removed:%s", edgeUrnType, false));
          assertEquals(
              testBoolQuery.minimumShouldMatch(),
              "1",
              "should queries should require at least 1 match");
          assertEquals(
              testBoolQuery.must().size(),
              0,
              String.format("type:%s removed:%s", edgeUrnType, false));
          assertEquals(
              testBoolQuery.filter().size(),
              1,
              String.format("type:%s removed:%s", edgeUrnType, false));
          assertEquals(
              testBoolQuery.mustNot().size(),
              0,
              String.format("type:%s removed:%s", edgeUrnType, false));

          urnQueryBuilder = (TermQueryBuilder) testBoolQuery.filter().get(0);
          statusQueryBuilder = (TermQueryBuilder) testBoolQuery.should().get(0);

          // non-existent status field
          BoolQueryBuilder existsBoolQuery = (BoolQueryBuilder) testBoolQuery.should().get(1);
          assertEquals(
              existsBoolQuery.should().size(),
              0,
              String.format("type:%s removed:%s", edgeUrnType, false));
          assertEquals(
              existsBoolQuery.must().size(),
              0,
              String.format("type:%s removed:%s", edgeUrnType, false));
          assertEquals(
              existsBoolQuery.filter().size(),
              0,
              String.format("type:%s removed:%s", edgeUrnType, false));
          assertEquals(
              existsBoolQuery.mustNot().size(),
              1,
              String.format("type:%s removed:%s", edgeUrnType, false));

          ExistsQueryBuilder existsQueryBuilder =
              (ExistsQueryBuilder) existsBoolQuery.mustNot().get(0);
          assertEquals(
              existsQueryBuilder.fieldName(),
              getUrnStatusFieldName(edgeUrnType),
              String.format("type:%s removed:%s", edgeUrnType, false));
        }

        assertEquals(
            urnQueryBuilder.fieldName(),
            getUrnFieldName(edgeUrnType),
            String.format("type:%s removed:%s", edgeUrnType, removed));
        assertEquals(
            urnQueryBuilder.value(),
            TEST_URN.toString(),
            String.format("type:%s removed:%s", edgeUrnType, removed));

        assertEquals(
            statusQueryBuilder.fieldName(),
            getUrnStatusFieldName(edgeUrnType),
            String.format("type:%s removed:%s", edgeUrnType, removed));
        assertEquals(
            statusQueryBuilder.value(),
            removed.toString(),
            String.format("type:%s removed:%s", edgeUrnType, removed));
      }
    }
  }
}
