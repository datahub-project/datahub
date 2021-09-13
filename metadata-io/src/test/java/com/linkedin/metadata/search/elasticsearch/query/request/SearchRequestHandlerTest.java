package com.linkedin.metadata.search.elasticsearch.query.request;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.TestEntitySpecBuilder;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SearchRequestHandlerTest {
  @Test
  public void testSearchRequestHandler() {
    SearchRequestHandler requestHandler = SearchRequestHandler.getBuilder(TestEntitySpecBuilder.getSpec());
    SearchRequest searchRequest = requestHandler.getSearchRequest("testQuery", null, null, 0, 10);
    SearchSourceBuilder sourceBuilder = searchRequest.source();
    assertEquals(sourceBuilder.from(), 0);
    assertEquals(sourceBuilder.size(), 10);
    // Filters
    Optional<AggregationBuilder> aggregationBuilder =
        sourceBuilder.aggregations().getAggregatorFactories().stream().findFirst();
    assertTrue(aggregationBuilder.isPresent());
    TermsAggregationBuilder filterPanelBuilder = (TermsAggregationBuilder) aggregationBuilder.get();
    assertEquals(filterPanelBuilder.field(), "textFieldOverride.keyword");
    // Highlights
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<String> fields =
        highlightBuilder.fields().stream().map(HighlightBuilder.Field::name).collect(Collectors.toList());
    assertEquals(fields.size(), 16);
    List<String> highlightableFields =
        ImmutableList.of("keyPart1", "textArrayField", "textFieldOverride", "foreignKey", "nestedForeignKey",
            "nestedArrayStringField", "nestedArrayArrayField", "customProperties");
    highlightableFields.forEach(field -> {
      assertTrue(fields.contains(field));
      assertTrue(fields.contains(field + ".*"));
    });
  }
}
