package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.TestEntitySpecBuilder;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class AutocompleteRequestHandlerTest {
  private AutocompleteRequestHandler handler = AutocompleteRequestHandler.getBuilder(TestEntitySpecBuilder.getSpec());

  @Test
  public void testDefaultAutocompleteRequest() {
    // When field is null
    SearchRequest autocompleteRequest = handler.getSearchRequest("input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    assertEquals(sourceBuilder.size(), 10);
    BoolQueryBuilder query = (BoolQueryBuilder) sourceBuilder.query();
    assertEquals(query.must().size(), 1);
    QueryStringQueryBuilder autocompleteQuery = (QueryStringQueryBuilder) query.must().get(0);
    Map<String, Float> queryFields = autocompleteQuery.fields();
    assertTrue(queryFields.containsKey("keyPart1"));
    assertTrue(queryFields.containsKey("keyPart1.ngram"));
    assertEquals(autocompleteQuery.analyzer(), "word_delimited");
    assertEquals(autocompleteQuery.defaultOperator(), Operator.AND);
    assertEquals(query.mustNot().size(), 1);
    MatchQueryBuilder removedFilter = (MatchQueryBuilder) query.mustNot().get(0);
    assertEquals(removedFilter.fieldName(), "removed");
    assertEquals(removedFilter.value(), true);
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<HighlightBuilder.Field> highlightedFields = highlightBuilder.fields();
    assertEquals(highlightedFields.size(), 2);
    assertEquals(highlightedFields.get(0).name(), "keyPart1");
    assertEquals(highlightedFields.get(1).name(), "keyPart1.*");
  }

  @Test
  public void testAutocompleteRequestWithField() {
    // When field is null
    SearchRequest autocompleteRequest = handler.getSearchRequest("input", "field", null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    assertEquals(sourceBuilder.size(), 10);
    BoolQueryBuilder query = (BoolQueryBuilder) sourceBuilder.query();
    assertEquals(query.must().size(), 1);
    QueryStringQueryBuilder autocompleteQuery = (QueryStringQueryBuilder) query.must().get(0);
    Map<String, Float> queryFields = autocompleteQuery.fields();
    assertTrue(queryFields.containsKey("field"));
    assertTrue(queryFields.containsKey("field.ngram"));
    assertEquals(autocompleteQuery.analyzer(), "word_delimited");
    assertEquals(autocompleteQuery.defaultOperator(), Operator.AND);
    MatchQueryBuilder removedFilter = (MatchQueryBuilder) query.mustNot().get(0);
    assertEquals(removedFilter.fieldName(), "removed");
    assertEquals(removedFilter.value(), true);
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<HighlightBuilder.Field> highlightedFields = highlightBuilder.fields();
    assertEquals(highlightedFields.size(), 2);
    assertEquals(highlightedFields.get(0).name(), "field");
    assertEquals(highlightedFields.get(1).name(), "field.*");
  }
}
