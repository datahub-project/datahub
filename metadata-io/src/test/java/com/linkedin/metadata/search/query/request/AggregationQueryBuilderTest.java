package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.utils.SearchUtil.*;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AggregationQueryBuilderTest {

  private static AspectRetriever aspectRetriever;

  @BeforeClass
  public static void setup() throws RemoteInvocationException, URISyntaxException {
    aspectRetriever = TestOperationContexts.emptyAspectRetriever(null);
  }

  @Test
  public void testGetDefaultAggregationsHasFields() {
    SearchableAnnotation annotation =
        new SearchableAnnotation(
            "test",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            true,
            Optional.empty(),
            Optional.of("Has Test"),
            1.0,
            Optional.of("hasTest"),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false);

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation)));

    List<AggregationBuilder> aggs =
        builder.getAggregations(TestOperationContexts.systemContextNoSearchAuthorization());

    Assert.assertTrue(aggs.stream().anyMatch(agg -> agg.getName().equals("hasTest")));
  }

  @Test
  public void testGetDefaultAggregationsFields() {

    SearchableAnnotation annotation =
        new SearchableAnnotation(
            "test",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            true,
            false,
            Optional.of("Test Filter"),
            Optional.empty(),
            1.0,
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false);

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation)));

    List<AggregationBuilder> aggs =
        builder.getAggregations(TestOperationContexts.systemContextNoSearchAuthorization());

    Assert.assertTrue(aggs.stream().anyMatch(agg -> agg.getName().equals("test")));
  }

  @Test
  public void testGetSpecificAggregationsHasFields() {
    SearchableAnnotation annotation1 =
        new SearchableAnnotation(
            "test1",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.empty(),
            Optional.of("Has Test"),
            1.0,
            Optional.of("hasTest1"),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false);

    SearchableAnnotation annotation2 =
        new SearchableAnnotation(
            "test2",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.of("Test Filter"),
            Optional.empty(),
            1.0,
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false);

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config,
            ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation1, annotation2)));

    // Case 1: Ask for fields that should exist.
    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            ImmutableList.of("test1", "test2", "hasTest1"));
    Assert.assertEquals(aggs.size(), 3);
    Set<String> facets = aggs.stream().map(AggregationBuilder::getName).collect(Collectors.toSet());
    Assert.assertEquals(ImmutableSet.of("test1", "test2", "hasTest1"), facets);

    // Case 2: Ask for fields that should NOT exist.
    aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            ImmutableList.of("hasTest2"));
    Assert.assertEquals(aggs.size(), 0);
  }

  @Test
  public void testAggregateOverStructuredProperty() {
    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of()));

    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            List.of("structuredProperties.ab.fgh.ten"));
    Assert.assertEquals(aggs.size(), 1);
    AggregationBuilder aggBuilder = aggs.get(0);
    Assert.assertTrue(aggBuilder instanceof TermsAggregationBuilder);
    TermsAggregationBuilder agg = (TermsAggregationBuilder) aggBuilder;
    // Check that field name is sanitized to correct field name
    Assert.assertEquals(agg.field(), "structuredProperties.ab_fgh_ten");

    // Two structured properties
    aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            List.of("structuredProperties.ab.fgh.ten", "structuredProperties.hello"));
    Assert.assertEquals(aggs.size(), 2);
    Assert.assertEquals(
        aggs.stream()
            .map(aggr -> ((TermsAggregationBuilder) aggr).field())
            .collect(Collectors.toSet()),
        Set.of("structuredProperties.ab_fgh_ten", "structuredProperties.hello"));
  }

  @Test
  public void testAggregateOverFieldsAndStructProp() {
    SearchableAnnotation annotation1 =
        new SearchableAnnotation(
            "test1",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.empty(),
            Optional.of("Has Test"),
            1.0,
            Optional.of("hasTest1"),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false);

    SearchableAnnotation annotation2 =
        new SearchableAnnotation(
            "test2",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.of("Test Filter"),
            Optional.empty(),
            1.0,
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false);

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config,
            ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation1, annotation2)));

    // Aggregate over fields and structured properties
    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            ImmutableList.of(
                "test1",
                "test2",
                "hasTest1",
                "structuredProperties.ab.fgh.ten",
                "structuredProperties.hello"));
    Assert.assertEquals(aggs.size(), 5);
    Set<String> facets =
        aggs.stream()
            .map(aggB -> ((TermsAggregationBuilder) aggB).field())
            .collect(Collectors.toSet());
    Assert.assertEquals(
        facets,
        ImmutableSet.of(
            "test1.keyword",
            "test2.keyword",
            "hasTest1",
            "structuredProperties.ab_fgh_ten",
            "structuredProperties.hello"));
  }

  @Test
  public void testMissingAggregation() {

    SearchableAnnotation annotation =
        new SearchableAnnotation(
            "test",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            true,
            Optional.empty(),
            Optional.of("Has Test"),
            1.0,
            Optional.of("hasTest"),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            true);

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation)));

    List<AggregationBuilder> aggs =
        builder.getAggregations(TestOperationContexts.systemContextNoSearchAuthorization());

    Assert.assertTrue(aggs.stream().anyMatch(agg -> agg.getName().equals("hasTest")));
    Assert.assertTrue(
        aggs.stream()
            .anyMatch(
                agg ->
                    agg.getName()
                        .equals(
                            MISSING_SPECIAL_TYPE + AGGREGATION_SPECIAL_TYPE_DELIMITER + "test")));
  }
}
