package com.linkedin.metadata.search.elasticsearch.client.shim;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import co.elastic.clients.elasticsearch.core.search.FieldSuggester;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.lang.reflect.Method;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.search.suggest.SuggestBuilders;
import org.opensearch.search.suggest.SuggestionBuilder;
import org.opensearch.search.suggest.completion.CompletionSuggestion;
import org.opensearch.search.suggest.phrase.PhraseSuggestion;
import org.opensearch.search.suggest.term.TermSuggestion;
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
    when(mockConfig.getSocketTimeout()).thenReturn(30000);

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

  /** Test that convertSuggestion handles different suggestion types */
  @Test
  public void testConvertSuggestionWithPhraseSuggestion() throws Exception {
    SuggestionBuilder<?> phraseSuggestion =
        SuggestBuilders.phraseSuggestion("name").text("test phrase").size(3);

    FieldSuggester result = invokeConvertSuggestion(phraseSuggestion);

    assertNotNull(result, "Converted FieldSuggester should not be null");
  }

  /** Test that convertSuggestion handles completion suggestions. */
  @Test
  public void testConvertSuggestionWithCompletionSuggestion() throws Exception {
    SuggestionBuilder<?> completionSuggestion =
        SuggestBuilders.completionSuggestion("name").text("test").size(10);

    FieldSuggester result = invokeConvertSuggestion(completionSuggestion);

    assertNotNull(result, "Converted FieldSuggester should not be null");
  }

  /** Test end-to-end search request conversion with suggestions enabled. */
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

  /** Test that convertAliasAction properly serializes an add alias action using XContent */
  @Test
  public void testConvertAliasActionWithAddAlias() throws Exception {
    IndicesAliasesRequest.AliasActions addAction =
        IndicesAliasesRequest.AliasActions.add().index("test-index").alias("test-alias");

    Action result = invokeConvertAliasAction(addAction);

    assertNotNull(result, "Converted Action should not be null");
  }

  /** Test that convertAliasAction properly serializes a remove alias action. */
  @Test
  public void testConvertAliasActionWithRemoveAlias() throws Exception {
    IndicesAliasesRequest.AliasActions removeAction =
        IndicesAliasesRequest.AliasActions.remove().index("test-index").alias("test-alias");

    Action result = invokeConvertAliasAction(removeAction);

    assertNotNull(result, "Converted Action should not be null");
  }

  /** Test that convertAliasAction handles alias actions with filters. */
  @Test
  public void testConvertAliasActionWithFilter() throws Exception {
    IndicesAliasesRequest.AliasActions addActionWithFilter =
        IndicesAliasesRequest.AliasActions.add()
            .index("test-index")
            .alias("filtered-alias")
            .filter(QueryBuilders.termQuery("status", "active"));

    Action result = invokeConvertAliasAction(addActionWithFilter);

    assertNotNull(result, "Converted Action with filter should not be null");
  }

  /** Test that convertAliasAction handles alias actions with routing. */
  @Test
  public void testConvertAliasActionWithRouting() throws Exception {
    IndicesAliasesRequest.AliasActions addActionWithRouting =
        IndicesAliasesRequest.AliasActions.add()
            .index("test-index")
            .alias("routed-alias")
            .routing("shard-1");

    Action result = invokeConvertAliasAction(addActionWithRouting);

    assertNotNull(result, "Converted Action with routing should not be null");
  }

  /**
   * Test that X_CONTENT_REGISTRY can parse a TermSuggestion from ES8 JSON response. The JSON uses
   * typed keys format (e.g., "term#suggestion_name") which is how ES8 returns suggestions when
   * typed_keys=true is set.
   */
  @Test
  public void testXContentRegistryParsesTermSuggestion() throws Exception {
    String termSuggestionJson =
        "{"
            + "\"took\": 5,"
            + "\"timed_out\": false,"
            + "\"_shards\": {\"total\": 1, \"successful\": 1, \"skipped\": 0, \"failed\": 0},"
            + "\"hits\": {\"total\": {\"value\": 0, \"relation\": \"eq\"}, \"max_score\": null, \"hits\": []},"
            + "\"suggest\": {"
            + "  \"term#term_suggestion\": [{"
            + "    \"text\": \"tset\","
            + "    \"offset\": 0,"
            + "    \"length\": 4,"
            + "    \"options\": [{\"text\": \"test\", \"score\": 0.75, \"freq\": 10}]"
            + "  }]"
            + "}"
            + "}";

    SearchResponse response = parseSearchResponse(termSuggestionJson);

    assertNotNull(response.getSuggest(), "Suggest should not be null");
    Suggest.Suggestion<
            ? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>
        suggestion = response.getSuggest().getSuggestion("term_suggestion");
    assertNotNull(suggestion, "term_suggestion should be present");
    assertEquals(suggestion.getClass(), TermSuggestion.class, "Should be a TermSuggestion");

    TermSuggestion termSuggestion = (TermSuggestion) suggestion;
    assertEquals(termSuggestion.getEntries().size(), 1, "Should have one entry");
    assertEquals(
        termSuggestion.getEntries().get(0).getText().string(), "tset", "Entry text should match");
  }

  /**
   * Test that X_CONTENT_REGISTRY can parse a PhraseSuggestion from ES8 JSON response. The JSON uses
   * typed keys format (e.g., "phrase#suggestion_name").
   */
  @Test
  public void testXContentRegistryParsesPhraseSuggestion() throws Exception {
    String phraseSuggestionJson =
        "{"
            + "\"took\": 5,"
            + "\"timed_out\": false,"
            + "\"_shards\": {\"total\": 1, \"successful\": 1, \"skipped\": 0, \"failed\": 0},"
            + "\"hits\": {\"total\": {\"value\": 0, \"relation\": \"eq\"}, \"max_score\": null, \"hits\": []},"
            + "\"suggest\": {"
            + "  \"phrase#phrase_suggestion\": [{"
            + "    \"text\": \"nobel prize\","
            + "    \"offset\": 0,"
            + "    \"length\": 11,"
            + "    \"options\": [{\"text\": \"noble prize\", \"score\": 0.85}]"
            + "  }]"
            + "}"
            + "}";

    SearchResponse response = parseSearchResponse(phraseSuggestionJson);

    assertNotNull(response.getSuggest(), "Suggest should not be null");
    Suggest.Suggestion<
            ? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>
        suggestion = response.getSuggest().getSuggestion("phrase_suggestion");
    assertNotNull(suggestion, "phrase_suggestion should be present");
    assertEquals(suggestion.getClass(), PhraseSuggestion.class, "Should be a PhraseSuggestion");

    PhraseSuggestion phraseSuggestion = (PhraseSuggestion) suggestion;
    assertEquals(phraseSuggestion.getEntries().size(), 1, "Should have one entry");
    assertEquals(
        phraseSuggestion.getEntries().get(0).getText().string(),
        "nobel prize",
        "Entry text should match");
  }

  /**
   * Test that X_CONTENT_REGISTRY can parse a CompletionSuggestion from ES8 JSON response. The JSON
   * uses typed keys format (e.g., "completion#suggestion_name").
   */
  @Test
  public void testXContentRegistryParsesCompletionSuggestion() throws Exception {
    String completionSuggestionJson =
        "{"
            + "\"took\": 5,"
            + "\"timed_out\": false,"
            + "\"_shards\": {\"total\": 1, \"successful\": 1, \"skipped\": 0, \"failed\": 0},"
            + "\"hits\": {\"total\": {\"value\": 0, \"relation\": \"eq\"}, \"max_score\": null, \"hits\": []},"
            + "\"suggest\": {"
            + "  \"completion#completion_suggestion\": [{"
            + "    \"text\": \"dat\","
            + "    \"offset\": 0,"
            + "    \"length\": 3,"
            + "    \"options\": [{\"text\": \"dataset\", \"_index\": \"test\", \"_id\": \"1\", \"_score\": 1.0, \"_source\": {}}]"
            + "  }]"
            + "}"
            + "}";

    SearchResponse response = parseSearchResponse(completionSuggestionJson);

    assertNotNull(response.getSuggest(), "Suggest should not be null");
    Suggest.Suggestion<
            ? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>
        suggestion = response.getSuggest().getSuggestion("completion_suggestion");
    assertNotNull(suggestion, "completion_suggestion should be present");
    assertEquals(
        suggestion.getClass(), CompletionSuggestion.class, "Should be a CompletionSuggestion");

    CompletionSuggestion completionSuggestion = (CompletionSuggestion) suggestion;
    assertEquals(completionSuggestion.getEntries().size(), 1, "Should have one entry");
    assertEquals(
        completionSuggestion.getEntries().get(0).getText().string(),
        "dat",
        "Entry text should match");
  }

  /** Helper method to parse a JSON string into a SearchResponse using X_CONTENT_REGISTRY. */
  private SearchResponse parseSearchResponse(String json) throws Exception {
    try (XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(
                SearchClientShimUtil.X_CONTENT_REGISTRY,
                LoggingDeprecationHandler.INSTANCE,
                json)) {
      return SearchResponse.fromXContent(parser);
    }
  }

  /** Helper method to invoke the private convertSuggestion method via reflection. */
  private FieldSuggester invokeConvertSuggestion(SuggestionBuilder<?> suggestionBuilder)
      throws Exception {
    Method convertSuggestionMethod =
        Es8SearchClientShim.class.getDeclaredMethod("convertSuggestion", SuggestionBuilder.class);
    convertSuggestionMethod.setAccessible(true);

    return (FieldSuggester) convertSuggestionMethod.invoke(shim, suggestionBuilder);
  }

  /** Helper method to invoke the private convertAliasAction method via reflection. */
  private Action invokeConvertAliasAction(IndicesAliasesRequest.AliasActions aliasAction)
      throws Exception {
    Method convertAliasActionMethod =
        Es8SearchClientShim.class.getDeclaredMethod(
            "convertAliasAction", IndicesAliasesRequest.AliasActions.class);
    convertAliasActionMethod.setAccessible(true);

    return (Action) convertAliasActionMethod.invoke(shim, aliasAction);
  }
}
