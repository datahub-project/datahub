package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.TestEntitySpecBuilder;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
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
    MultiMatchQueryBuilder queryBuilder = (MultiMatchQueryBuilder) sourceBuilder.query();
    Map<String, Float> queryFields = queryBuilder.fields();
    assertTrue(queryFields.containsKey("keyPart1"));
    assertTrue(queryFields.containsKey("keyPart1.ngram"));
    assertEquals(queryBuilder.analyzer(), "word_delimited");
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
    MultiMatchQueryBuilder queryBuilder = (MultiMatchQueryBuilder) sourceBuilder.query();
    Map<String, Float> queryFields = queryBuilder.fields();
    assertTrue(queryFields.containsKey("field"));
    assertTrue(queryFields.containsKey("field.ngram"));
    assertEquals(queryBuilder.analyzer(), "word_delimited");
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<HighlightBuilder.Field> highlightedFields = highlightBuilder.fields();
    assertEquals(highlightedFields.size(), 2);
    assertEquals(highlightedFields.get(0).name(), "field");
    assertEquals(highlightedFields.get(1).name(), "field.*");
  }
}
