package com.linkedin.metadata.search.opensearch;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.index.NoOpMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.client.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Semantic kNN on a Search V3 document index, run against the suite's OpenSearch 2 and 3
 * containers. The index carries the same root {@code embeddings} mapping the V3 writer adds for
 * semantic-enabled entities, and no V2 semantic index exists.
 */
@Import({
  OpenSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class SemanticSearchV3OpenSearchTest extends AbstractTestNGSpringContextTests {

  private static final String MODEL_KEY = "test_model";
  private static final String NEAR_URN = "urn:li:document:near";
  private static final String FAR_URN = "urn:li:document:far";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired private SearchClientShim<?> searchClientShim;

  @Test
  public void testSemanticSearchReadsDocumentVectorsFromV3Index() throws IOException {
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of("document"));
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(4);
    semanticSearch.setModels(Map.of(MODEL_KEY, model));

    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("semanticv3"),
            TEST_OS_SEARCH_CONFIG.getEntityIndex());
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            SearchContext.builder()
                .indexConvention(indexConvention)
                .searchClusterAccess(SearchClusterAccess.fixed(searchClientShim))
                .build());
    String index = indexConvention.getEntityIndexNameV3(opContext, "document");

    createIndex(index, semanticSearch);
    try {
      indexDocument(index, NEAR_URN, new float[] {1f, 0f, 0f, 0f});
      indexDocument(index, FAR_URN, new float[] {0f, 1f, 0f, 0f});
      lowLevel("POST", "/" + index + "/_refresh", null);

      EmbeddingProvider embeddingProvider = mock(EmbeddingProvider.class);
      when(embeddingProvider.embed(anyString(), any(), any(EmbeddingTaskType.class)))
          .thenReturn(new float[] {1f, 0f, 0f, 0f});
      SemanticEntitySearchService service =
          new SemanticEntitySearchService(
              searchClientShim,
              embeddingProvider,
              new NoOpMappingsBuilder(),
              MODEL_KEY,
              EntityIndexConfiguration.builder()
                  .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
                  .v3(
                      EntityIndexVersionConfiguration.builder()
                          .enabled(true)
                          .semanticReadEnabled(true)
                          .build())
                  .semanticSearch(semanticSearch)
                  .build());

      SearchResult ranked =
          service.search(opContext, List.of("document"), "query", null, null, 0, 10);
      assertEquals(urns(ranked), List.of(NEAR_URN, FAR_URN));

      // Root-field filters still apply inside the kNN query on V3 documents
      SearchResult filtered =
          service.search(
              opContext, List.of("document"), "query", filter("urn", FAR_URN), null, 0, 10);
      assertEquals(urns(filtered), List.of(FAR_URN));

      // Entity-type filters in the GraphQL enum form keep matching on V3
      SearchResult byType =
          service.search(
              opContext,
              List.of("document"),
              "query",
              filter("_entityType", "DOCUMENT"),
              null,
              0,
              10);
      assertEquals(urns(byType), List.of(NEAR_URN, FAR_URN));
    } finally {
      lowLevel("DELETE", "/" + index, null);
    }
  }

  private void createIndex(String index, SemanticSearchConfiguration semanticSearch)
      throws IOException {
    Map<String, Object> body =
        Map.of(
            "settings",
            Map.of("index", Map.of("knn", true, "number_of_shards", 1, "number_of_replicas", 0)),
            "mappings",
            Map.of(
                "properties",
                Map.of(
                    "urn",
                    Map.of("type", "keyword"),
                    "_entityType",
                    Map.of("type", "keyword"),
                    SemanticEmbeddingMappings.EMBEDDINGS_FIELD,
                    SemanticEmbeddingMappings.buildEmbeddingFieldConfig(
                        semanticSearch, searchClientShim))));
    lowLevel("PUT", "/" + index, body);
  }

  private void indexDocument(String index, String urn, float[] vector) throws IOException {
    Map<String, Object> chunk = Map.of("vector", vector, "text", urn, "position", 0);
    Map<String, Object> document =
        Map.of(
            "urn",
            urn,
            "_entityType",
            "document",
            SemanticEmbeddingMappings.EMBEDDINGS_FIELD,
            Map.of(MODEL_KEY, Map.of("chunks", List.of(chunk))));
    lowLevel("PUT", "/" + index + "/_doc/" + urn.hashCode(), document);
  }

  private void lowLevel(String method, String endpoint, Map<String, Object> body)
      throws IOException {
    Request request = new Request(method, endpoint);
    if (body != null) {
      request.setJsonEntity(MAPPER.writeValueAsString(body));
    }
    int status =
        searchClientShim
            .performLowLevelRequest(OperationFingerprint.EMPTY, request)
            .getStatusLine()
            .getStatusCode();
    assertTrue(status >= 200 && status < 300, method + " " + endpoint + " returned " + status);
  }

  private static Filter filter(String field, String value) {
    Criterion criterion =
        new Criterion()
            .setField(field)
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(List.of(value)));
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(criterion))));
  }

  private static List<String> urns(SearchResult result) {
    return result.getEntities().stream()
        .map(entity -> entity.getEntity().toString())
        .collect(Collectors.toList());
  }
}
