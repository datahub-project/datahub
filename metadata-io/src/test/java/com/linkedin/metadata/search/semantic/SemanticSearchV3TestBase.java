package com.linkedin.metadata.search.semantic;

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
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.DocumentV3EmbeddingMappingContributor;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.opensearch.client.Request;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Semantic kNN on a Search V3 document index, run against each engine's test container. The index
 * carries the same root {@code embeddings} mapping the V3 writer adds for semantic-enabled entities
 * and sits behind an alias, as it does after a reindex, so entity-type filters are checked against
 * the alias name. No V2 semantic index exists.
 */
public abstract class SemanticSearchV3TestBase extends AbstractTestNGSpringContextTests {

  private static final String MODEL_KEY = "test_model";
  private static final String NEAR_URN = "urn:li:document:near";
  private static final String FAR_URN = "urn:li:document:far";
  private static final String DOMAIN_URN = "urn:li:domain:engineering";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SemanticEntitySearchService service;
  private OperationContext opContext;
  private String backingIndex;

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @BeforeClass
  public void setUpV3Index() throws IOException {
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of("document"));
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(4);
    semanticSearch.setModels(Map.of(MODEL_KEY, model));
    EntityIndexConfiguration entityIndex =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
            .v3(
                EntityIndexVersionConfiguration.builder()
                    .enabled(true)
                    .semanticReadEnabled(true)
                    .build())
            .semanticSearch(semanticSearch)
            .build();

    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("semanticv3"),
            entityIndex);
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            SearchContext.builder()
                .indexConvention(indexConvention)
                .searchClusterAccess(SearchClusterAccess.fixed(getSearchClient()))
                .build());
    String alias = indexConvention.getEntityIndexNameV3(opContext, "document");
    backingIndex = alias + "_1";

    createIndex(backingIndex, alias, semanticSearch);
    indexDocument(alias, NEAR_URN, new float[] {1f, 0f, 0f, 0f}, DOMAIN_URN);
    indexDocument(alias, FAR_URN, new float[] {0f, 1f, 0f, 0f}, null);
    lowLevel("POST", "/" + alias + "/_refresh", null);

    EmbeddingProvider embeddingProvider = mock(EmbeddingProvider.class);
    when(embeddingProvider.embed(anyString(), any(), any(EmbeddingTaskType.class)))
        .thenReturn(new float[] {1f, 0f, 0f, 0f});
    service =
        new SemanticEntitySearchService(
            getSearchClient(),
            embeddingProvider,
            new NoOpMappingsBuilder(),
            MODEL_KEY,
            entityIndex);
  }

  @AfterClass(alwaysRun = true)
  public void deleteV3Index() throws IOException {
    if (backingIndex != null) {
      lowLevel("DELETE", "/" + backingIndex, null);
    }
  }

  @Test
  public void testSemanticSearchReadsDocumentVectorsFromV3Index() {
    SearchResult ranked =
        service.search(opContext, List.of("document"), "query", null, null, 0, 10);
    assertEquals(urns(ranked), List.of(NEAR_URN, FAR_URN));

    // Root-field filters still apply to kNN on V3 documents
    SearchResult filtered =
        service.search(
            opContext, List.of("document"), "query", filter("urn", FAR_URN), null, 0, 10);
    assertEquals(urns(filtered), List.of(FAR_URN));

    // Entity-type filters in the GraphQL enum form match through the alias
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
    SearchResult otherType =
        service.search(
            opContext, List.of("document"), "query", filter("_entityType", "DATASET"), null, 0, 10);
    assertTrue(otherType.getEntities().isEmpty(), urns(otherType).toString());
  }

  @Test
  public void testAspectFieldFilterMatchesV3Documents() {
    if (getSearchClient().getEngineType() == SearchEngineType.OPENSEARCH_2) {
      throw new SkipException(
          "GMS refuses V3 semantic reads on OpenSearch 2, whose k-NN plugin does not apply nested"
              + " pre-filters to fields under an underscore-prefixed object such as V3's _aspects;"
              + " this test builds the service directly");
    }
    // URN and keyword fields have no .keyword subfield on V3, unlike the V2 indices
    SearchResult byDomain =
        service.search(
            opContext, List.of("document"), "query", filter("domains", DOMAIN_URN), null, 0, 10);
    assertEquals(urns(byDomain), List.of(NEAR_URN));
  }

  private void createIndex(
      String backingIndex, String alias, SemanticSearchConfiguration semanticSearch)
      throws IOException {
    // The root fields the V3 mapping contributor adds for documents, plus the fields the reader
    // uses
    Map<String, Object> properties =
        new HashMap<>(
            new DocumentV3EmbeddingMappingContributor(semanticSearch, getSearchClient())
                .extraRootProperties("document"));
    properties.put("urn", Map.of("type", "keyword"));
    properties.put("_entityType", Map.of("type", "keyword"));
    // A URN field the way the V3 mappings builder lays it out: keyword under _aspects, reached
    // through a root alias
    properties.put(
        "_aspects",
        Map.of(
            "properties",
            Map.of(
                "domains",
                Map.of(
                    "properties",
                    Map.of("domains", Map.of("type", "keyword", "ignore_above", 255))))));
    properties.put("domains", Map.of("type", "alias", "path", "_aspects.domains.domains"));
    Map<String, Object> indexSettings = new HashMap<>();
    indexSettings.put("number_of_shards", 1);
    indexSettings.put("number_of_replicas", 0);
    // OpenSearch needs index-level kNN; Elasticsearch rejects the setting
    if (SemanticEmbeddingMappings.shouldEnableIndexLevelKnn(getSearchClient())) {
      indexSettings.put("knn", true);
    }
    Map<String, Object> body =
        Map.of(
            "settings",
            Map.of("index", indexSettings),
            "mappings",
            Map.of("properties", properties),
            "aliases",
            Map.of(alias, Map.of()));
    lowLevel("PUT", "/" + backingIndex, body);
  }

  private void indexDocument(String index, String urn, float[] vector, String domain)
      throws IOException {
    Map<String, Object> chunk = Map.of("vector", vector, "text", urn, "position", 0);
    Map<String, Object> document = new HashMap<>();
    document.put("urn", urn);
    document.put("_entityType", "document");
    document.put(
        SemanticEmbeddingMappings.EMBEDDINGS_FIELD,
        Map.of(MODEL_KEY, Map.of("chunks", List.of(chunk))));
    if (domain != null) {
      document.put("_aspects", Map.of("domains", Map.of("domains", List.of(domain))));
    }
    lowLevel("PUT", "/" + index + "/_doc/" + urn.hashCode(), document);
  }

  private void lowLevel(String method, String endpoint, Map<String, Object> body)
      throws IOException {
    Request request = new Request(method, endpoint);
    if (body != null) {
      request.setJsonEntity(MAPPER.writeValueAsString(body));
    }
    int status =
        getSearchClient()
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
