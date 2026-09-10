package io.datahubproject.openapi.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Filter;
import io.datahubproject.openapi.v2.models.GenericRelationship;
import io.datahubproject.openapi.v3.models.LineageRelationship;
import java.util.List;
import org.testng.annotations.Test;

public class ScrollUtilsTest {

  private static final String DATASET_A = "urn:li:dataset:(urn:li:dataPlatform:kafka,a,PROD)";
  private static final String DATASET_B = "urn:li:dataset:(urn:li:dataPlatform:kafka,b,PROD)";
  private static final String DATASET_C = "urn:li:dataset:(urn:li:dataPlatform:kafka,c,PROD)";

  // Edge with A as the upstream endpoint and B as the downstream endpoint.
  private static LineageRelationship edgeAUpstreamOfB() {
    return LineageRelationship.builder()
        .relationshipType("DownstreamOf")
        .source(GenericRelationship.GenericNode.fromUrn(UrnUtils.getUrn(DATASET_B)))
        .destination(GenericRelationship.GenericNode.fromUrn(UrnUtils.getUrn(DATASET_A)))
        .upstream(DATASET_A)
        .downstream(DATASET_B)
        .build();
  }

  @Test
  public void testBuildUrnEndpointFilterEmpty() {
    assertEquals(ScrollUtils.buildUrnEndpointFilter(null).getOr().size(), 0);
    assertEquals(ScrollUtils.buildUrnEndpointFilter(List.of()).getOr().size(), 0);
  }

  @Test
  public void testBuildUrnEndpointFilterMatchesEitherEndpoint() {
    Filter filter = ScrollUtils.buildUrnEndpointFilter(List.of(DATASET_A));

    // Disjunction of two single-criterion conjunctions: source.urn == A, destination.urn == A.
    assertEquals(filter.getOr().size(), 2);
    List<String> fields =
        filter.getOr().stream()
            .flatMap(c -> c.getAnd().stream())
            .map(c -> c.getField())
            .sorted()
            .collect(java.util.stream.Collectors.toList());
    assertEquals(fields, List.of("destination.urn", "source.urn"));
    for (ConjunctiveCriterion conjunction : filter.getOr()) {
      assertEquals(conjunction.getAnd().get(0).getValues().get(0), DATASET_A);
    }
  }

  @Test
  public void testKeepByLineageDirectionAnchorIsDownstream() {
    // Anchor B is the edge's downstream endpoint, so A is an upstream of B.
    LineageRelationship edge = edgeAUpstreamOfB();
    assertTrue(
        ScrollUtils.keepByLineageDirection(edge, List.of(DATASET_B), LineageDirection.UPSTREAM));
    assertFalse(
        ScrollUtils.keepByLineageDirection(edge, List.of(DATASET_B), LineageDirection.DOWNSTREAM));
  }

  @Test
  public void testKeepByLineageDirectionAnchorIsUpstream() {
    // Anchor A is the edge's upstream endpoint, so B is a downstream of A.
    LineageRelationship edge = edgeAUpstreamOfB();
    assertTrue(
        ScrollUtils.keepByLineageDirection(edge, List.of(DATASET_A), LineageDirection.DOWNSTREAM));
    assertFalse(
        ScrollUtils.keepByLineageDirection(edge, List.of(DATASET_A), LineageDirection.UPSTREAM));
  }

  @Test
  public void testKeepByLineageDirectionNeitherEndpointIsAnchor() {
    LineageRelationship edge = edgeAUpstreamOfB();
    assertFalse(
        ScrollUtils.keepByLineageDirection(edge, List.of(DATASET_C), LineageDirection.UPSTREAM));
  }

  @Test
  public void testKeepByLineageDirectionNoOpWithoutDirectionOrUrns() {
    LineageRelationship edge = edgeAUpstreamOfB();
    assertTrue(ScrollUtils.keepByLineageDirection(edge, List.of(DATASET_A), null));
    assertTrue(ScrollUtils.keepByLineageDirection(edge, null, LineageDirection.UPSTREAM));
    assertTrue(ScrollUtils.keepByLineageDirection(edge, List.of(), LineageDirection.UPSTREAM));
  }
}
