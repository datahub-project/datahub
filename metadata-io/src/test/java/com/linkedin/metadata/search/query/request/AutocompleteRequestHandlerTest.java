package com.linkedin.metadata.search.query.request;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.testng.annotations.Test;

public class AutocompleteRequestHandlerTest {
  private AutocompleteRequestHandler handler =
      AutocompleteRequestHandler.getBuilder(
          TestEntitySpecBuilder.getSpec(), mock(AspectRetriever.class));
  private OperationContext mockOpContext =
      TestOperationContexts.systemContextNoSearchAuthorization(mock(EntityRegistry.class));

  @Test
  public void testDefaultAutocompleteRequest() {
    // When field is null
    SearchRequest autocompleteRequest =
        handler.getSearchRequest(mockOpContext, "input", null, null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    assertEquals(sourceBuilder.size(), 10);
    BoolQueryBuilder query = (BoolQueryBuilder) sourceBuilder.query();
    assertEquals(query.should().size(), 3);

    MultiMatchQueryBuilder autocompleteQuery = (MultiMatchQueryBuilder) query.should().get(2);
    Map<String, Float> queryFields = autocompleteQuery.fields();
    assertTrue(queryFields.containsKey("keyPart1.ngram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._2gram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._3gram"));
    assertTrue(queryFields.containsKey("keyPart1.ngram._4gram"));
    assertEquals(autocompleteQuery.type(), MultiMatchQueryBuilder.Type.BOOL_PREFIX);

    MatchPhrasePrefixQueryBuilder prefixQuery =
        (MatchPhrasePrefixQueryBuilder) query.should().get(0);
    assertEquals("keyPart1.delimited", prefixQuery.fieldName());

    assertEquals(query.mustNot().size(), 1);
    MatchQueryBuilder removedFilter = (MatchQueryBuilder) query.mustNot().get(0);
    assertEquals(removedFilter.fieldName(), "removed");
    assertEquals(removedFilter.value(), true);
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<HighlightBuilder.Field> highlightedFields = highlightBuilder.fields();
    assertEquals(highlightedFields.size(), 8);
    assertEquals(highlightedFields.get(0).name(), "keyPart1");
    assertEquals(highlightedFields.get(1).name(), "keyPart1.*");
    assertEquals(highlightedFields.get(2).name(), "keyPart1.ngram");
    assertEquals(highlightedFields.get(3).name(), "keyPart1.delimited");
    assertEquals(highlightedFields.get(4).name(), "urn");
    assertEquals(highlightedFields.get(5).name(), "urn.*");
    assertEquals(highlightedFields.get(6).name(), "urn.ngram");
    assertEquals(highlightedFields.get(7).name(), "urn.delimited");
  }

  @Test
  public void testAutocompleteRequestWithField() {
    // When field is null
    SearchRequest autocompleteRequest =
        handler.getSearchRequest(mockOpContext, "input", "field", null, 10);
    SearchSourceBuilder sourceBuilder = autocompleteRequest.source();
    assertEquals(sourceBuilder.size(), 10);
    BoolQueryBuilder query = (BoolQueryBuilder) sourceBuilder.query();
    assertEquals(query.should().size(), 2);

    MultiMatchQueryBuilder autocompleteQuery = (MultiMatchQueryBuilder) query.should().get(1);
    Map<String, Float> queryFields = autocompleteQuery.fields();
    assertTrue(queryFields.containsKey("field.ngram"));
    assertTrue(queryFields.containsKey("field.ngram._2gram"));
    assertTrue(queryFields.containsKey("field.ngram._3gram"));
    assertTrue(queryFields.containsKey("field.ngram._4gram"));
    assertEquals(autocompleteQuery.type(), MultiMatchQueryBuilder.Type.BOOL_PREFIX);

    MatchPhrasePrefixQueryBuilder prefixQuery =
        (MatchPhrasePrefixQueryBuilder) query.should().get(0);
    assertEquals("field.delimited", prefixQuery.fieldName());

    MatchQueryBuilder removedFilter = (MatchQueryBuilder) query.mustNot().get(0);
    assertEquals(removedFilter.fieldName(), "removed");
    assertEquals(removedFilter.value(), true);
    HighlightBuilder highlightBuilder = sourceBuilder.highlighter();
    List<HighlightBuilder.Field> highlightedFields = highlightBuilder.fields();
    assertEquals(highlightedFields.size(), 4);
    assertEquals(highlightedFields.get(0).name(), "field");
    assertEquals(highlightedFields.get(1).name(), "field.*");
    assertEquals(highlightedFields.get(2).name(), "field.ngram");
    assertEquals(highlightedFields.get(3).name(), "field.delimited");
  }
}
