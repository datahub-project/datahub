package com.linkedin.metadata.graph.elastic;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Set;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** The count request fetches no documents and excludes the group's own members as neighbours. */
public class GraphQueryBaseDAOLineageCountTest {

  private static final Urn DATASET_ONE =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,count_one,PROD)");
  private static final Urn DATASET_TWO =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,count_two,PROD)");

  private OperationContext opContext;
  private GraphQueryPITDAO dao;

  @BeforeClass
  public void setup() {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    dao = new GraphQueryPITDAO(client, TEST_GRAPH_SERVICE_CONFIG, TEST_ES_SEARCH_CONFIG, null);
  }

  @AfterClass
  public void teardown() {
    dao.shutdown();
  }

  @Test
  public void testCountRequestAggregatesInsteadOfFetching() {
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            opContext.getLineageRegistry(), "dataset", LineageDirection.DOWNSTREAM);

    SearchSourceBuilder source =
        dao.buildLineageCountSource(opContext, Set.of(DATASET_ONE, DATASET_TWO), filters, 7);

    String rendered = source.toString();
    assertTrue(rendered.contains("\"size\":0"), rendered);
    assertTrue(rendered.contains("\"cardinality\""), rendered);
    assertTrue(rendered.contains("\"terms\""), rendered);
    assertTrue(rendered.contains("lineageCount_"), rendered);
    // Via entities are sampled too, so ghost and sibling filtering reach them.
    assertTrue(rendered.contains("\"viaSample\""), rendered);
    // Group members are left out of the vias as well as the neighbours.
    assertTrue(rendered.contains("\"viaOutsideGroup\""), rendered);
    // Members of the group are never counted as each other's neighbours.
    assertTrue(rendered.contains("must_not"), rendered);
    assertTrue(rendered.contains(DATASET_ONE.toString()), rendered);
    assertTrue(rendered.contains(DATASET_TWO.toString()), rendered);
  }

  @Test
  public void testNoLineageEdgesMeansNoRequest() {
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            opContext.getLineageRegistry(), "tag", LineageDirection.DOWNSTREAM);
    assertNull(
        dao.buildLineageCountSource(
            opContext, Set.of(UrnUtils.getUrn("urn:li:tag:count")), filters, 7));
  }

  @Test
  public void testSampleSizeIsAtLeastOne() {
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            opContext.getLineageRegistry(), "dataset", LineageDirection.UPSTREAM);
    SearchSourceBuilder source =
        dao.buildLineageCountSource(opContext, Set.of(DATASET_ONE), filters, 0);
    assertFalse(source.toString().contains("\"size\":-"), source.toString());
  }

  /**
   * When a sample is truncated the total falls back to the cardinalities, which count a URN that is
   * both a neighbour and a via once per role. URNs seen in both samples are counted once.
   */
  @Test
  public void testTruncatedTotalCountsNeighbourAndViaOnce() {
    Urn a = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,count_a,PROD)");
    Urn job = UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD),job)");
    // Three distinct neighbours (a, job and one beyond the sample) and one via (job, which is also
    // a neighbour). The neighbour sample is truncated.
    Filter direction = mock(Filter.class);
    when(direction.getName()).thenReturn("lineageCount_OUTGOING");
    Aggregations sub =
        new Aggregations(
            List.of(
                cardinality("distinct", 3),
                terms("sample", 1, a, job),
                vias(cardinality("via", 1), terms("viaSample", 0, job))));
    when(direction.getAggregations()).thenReturn(sub);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(new Aggregations(List.of(direction)));

    assertEquals(dao.parseLineageCount(response, Set.of(DATASET_ONE), 10).getTotal(), 3);
  }

  /** The filter that wraps the via aggregations, excluding group members. */
  private static Filter vias(Cardinality via, Terms viaSample) {
    Filter filter = mock(Filter.class);
    when(filter.getName()).thenReturn("viaOutsideGroup");
    when(filter.getAggregations()).thenReturn(new Aggregations(List.of(via, viaSample)));
    return filter;
  }

  private static Cardinality cardinality(String name, long value) {
    Cardinality cardinality = mock(Cardinality.class);
    when(cardinality.getName()).thenReturn(name);
    when(cardinality.getValue()).thenReturn(value);
    return cardinality;
  }

  private static Terms terms(String name, long otherDocs, Urn... urns) {
    Terms terms = mock(Terms.class);
    when(terms.getName()).thenReturn(name);
    when(terms.getSumOfOtherDocCounts()).thenReturn(otherDocs);
    List<Terms.Bucket> buckets =
        java.util.Arrays.stream(urns)
            .map(
                urn -> {
                  Terms.Bucket bucket = mock(Terms.Bucket.class);
                  when(bucket.getKeyAsString()).thenReturn(urn.toString());
                  return bucket;
                })
            .toList();
    doReturn(buckets).when(terms).getBuckets();
    return terms;
  }
}
