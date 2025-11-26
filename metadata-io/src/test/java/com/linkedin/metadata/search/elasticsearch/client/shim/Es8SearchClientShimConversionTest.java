package com.linkedin.metadata.search.elasticsearch.client.shim;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import co.elastic.clients.elasticsearch.core.search.FieldSuggester;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.lang.reflect.Method;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.search.suggest.SuggestBuilders;
import org.opensearch.search.suggest.SuggestionBuilder;
import org.opensearch.search.suggest.term.TermSuggestionBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for Es8SearchClientShim conversion methods */
public class Es8SearchClientShimConversionTest {

  private Es8SearchClientShim shim;

  @BeforeMethod
  public void setUp() throws Exception {
    SearchClientShim.ShimConfiguration mockConfig = mock(SearchClientShim.ShimConfiguration.class);
    when(mockConfig.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(mockConfig.getHost()).thenReturn("localhost");
    when(mockConfig.getPort()).thenReturn(9200);
    when(mockConfig.getUsername()).thenReturn(null);
    when(mockConfig.getPassword()).thenReturn(null);
    when(mockConfig.isUseSSL()).thenReturn(false);
    when(mockConfig.getPathPrefix()).thenReturn(null);
    when(mockConfig.isUseAwsIamAuth()).thenReturn(false);
    when(mockConfig.getThreadCount()).thenReturn(1);
    when(mockConfig.getConnectionRequestTimeout()).thenReturn(5000);

    ObjectMapper objectMapper = new ObjectMapper();

    shim = new Es8SearchClientShim(mockConfig, objectMapper);
  }

  /** Test that convertSuggestion properly serializes a TermSuggestionBuilder using XContent. */
  @Test
  public void testConvertSuggestionWithTermSuggestion() throws Exception {
    TermSuggestionBuilder termSuggestion =
        SuggestBuilders.termSuggestion("name").text("test_input").size(5);

    FieldSuggester result = invokeConvertSuggestion(termSuggestion);

    assertNotNull(result, "Converted FieldSuggester should not be null");
  }

  /**
   * Test that convertSuggestion handles different suggestion types. This ensures the XContent
   * serialization works for various SuggestionBuilder types.
   */
  @Test
  public void testConvertSuggestionWithPhraseSuggestion() throws Exception {
    SuggestionBuilder<?> phraseSuggestion =
        SuggestBuilders.phraseSuggestion("name").text("test phrase").size(3);

    FieldSuggester result = invokeConvertSuggestion(phraseSuggestion);

    assertNotNull(result, "Converted FieldSuggester should not be null");
  }

  /**
   * Test that convertSuggestion handles completion suggestions. This verifies XContent
   * serialization for completion-based suggestions.
   */
  @Test
  public void testConvertSuggestionWithCompletionSuggestion() throws Exception {
    SuggestionBuilder<?> completionSuggestion =
        SuggestBuilders.completionSuggestion("name").text("test").size(10);

    FieldSuggester result = invokeConvertSuggestion(completionSuggestion);

    assertNotNull(result, "Converted FieldSuggester should not be null");
  }

  /**
   * Test end-to-end search request conversion with suggestions enabled. This simulates the actual
   * usage pattern where suggestions are built via ESUtils.buildNameSuggestions.
   */
  @Test
  public void testSearchRequestWithSuggestions() throws Exception {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    SuggestionBuilder<TermSuggestionBuilder> builder =
        SuggestBuilders.termSuggestion("name").text("dataset");
    SuggestBuilder suggestBuilder = new SuggestBuilder();
    suggestBuilder.addSuggestion("name_suggestion", builder);
    searchSourceBuilder.suggest(suggestBuilder);

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);

    assertNotNull(
        searchRequest.source().suggest(),
        "SearchRequest should have suggestions attached before conversion");
    assertEquals(
        1,
        searchRequest.source().suggest().getSuggestions().size(),
        "Should have exactly one suggestion");
  }

  /**
   * Helper method to invoke the private convertSuggestion method via reflection. This is necessary
   * because convertSuggestion is a private method in Es8SearchClientShim.
   */
  private FieldSuggester invokeConvertSuggestion(SuggestionBuilder<?> suggestionBuilder)
      throws Exception {
    Method convertSuggestionMethod =
        Es8SearchClientShim.class.getDeclaredMethod("convertSuggestion", SuggestionBuilder.class);
    convertSuggestionMethod.setAccessible(true);

    return (FieldSuggester) convertSuggestionMethod.invoke(shim, suggestionBuilder);
  }
}
