package com.linkedin.metadata.search.elasticsearch.query.request;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggregationQueryBuilderTest {

  @Test
  public void testGetAggregationsHasFields() {

    SearchableAnnotation annotation = new SearchableAnnotation(
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
        Collections.emptyMap()
    );

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder = new AggregationQueryBuilder(
        config, ImmutableList.of(annotation));

    List<AggregationBuilder> aggs = builder.getAggregations();

    Assert.assertTrue(aggs.stream().anyMatch(agg -> agg.getName().equals("hasTest")));
  }

  @Test
  public void testGetAggregationsFields() {

    SearchableAnnotation annotation = new SearchableAnnotation(
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
        Collections.emptyMap()
    );

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder = new AggregationQueryBuilder(
        config, ImmutableList.of(annotation));

    List<AggregationBuilder> aggs = builder.getAggregations();

    Assert.assertTrue(aggs.stream().anyMatch(agg -> agg.getName().equals("test")));
  }
}
