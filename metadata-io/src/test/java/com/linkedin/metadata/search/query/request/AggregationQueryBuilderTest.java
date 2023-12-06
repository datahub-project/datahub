package com.linkedin.metadata.search.query.request;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AggregationQueryBuilderTest {

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
            Collections.emptyList());

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(config, ImmutableList.of(annotation));

    List<AggregationBuilder> aggs = builder.getAggregations();

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
            Collections.emptyList());

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(config, ImmutableList.of(annotation));

    List<AggregationBuilder> aggs = builder.getAggregations();

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
            Collections.emptyList());

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
            Collections.emptyList());

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(config, ImmutableList.of(annotation1, annotation2));

    // Case 1: Ask for fields that should exist.
    List<AggregationBuilder> aggs =
        builder.getAggregations(ImmutableList.of("test1", "test2", "hasTest1"));
    Assert.assertEquals(aggs.size(), 3);
    Set<String> facets = aggs.stream().map(AggregationBuilder::getName).collect(Collectors.toSet());
    Assert.assertEquals(ImmutableSet.of("test1", "test2", "hasTest1"), facets);

    // Case 2: Ask for fields that should NOT exist.
    aggs = builder.getAggregations(ImmutableList.of("hasTest2"));
    Assert.assertEquals(aggs.size(), 0);
  }
}
