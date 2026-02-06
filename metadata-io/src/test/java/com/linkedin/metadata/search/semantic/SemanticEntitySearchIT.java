package com.linkedin.metadata.search.semantic;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.NoOpMappingsBuilder;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.opensearch.client.Request;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(enabled = false) // Disabled: Requires OpenSearch with semantic indices
public class SemanticEntitySearchIT {

  private SearchClientShim<?> client;

  @BeforeClass
  public void setUp() throws IOException {
    SearchClientShimUtil.ShimConfigurationBuilder configBuilder =
        new SearchClientShimUtil.ShimConfigurationBuilder()
            .withHost("localhost")
            .withPort(9200)
            .withSSL(false);
    client =
        SearchClientShimUtil.createShimWithAutoDetection(configBuilder.build(), new ObjectMapper());

    // Skip if OpenSearch is not available
    try {
      RawResponse resp = client.performLowLevelRequest(new Request("GET", "/_cluster/health"));
      int status = resp.getStatusLine().getStatusCode();
      if (status < 200 || status >= 300) {
        throw new SkipException("OpenSearch not healthy: status=" + status);
      }

      // Ensure semantic dataset index exists
      RawResponse headIndex =
          client.performLowLevelRequest(new Request("HEAD", "/datasetindex_v2_semantic"));
      int idxStatus = headIndex.getStatusLine().getStatusCode();
      if (idxStatus == 404) {
        throw new SkipException("Index datasetindex_v2_semantic not found; skipping semantic IT");
      }

      // Ensure vector field mapping exists
      RawResponse mapping =
          client.performLowLevelRequest(
              new Request(
                  "GET",
                  "/datasetindex_v2_semantic/_mapping/field/embeddings.cohere_embed_v3.chunks.vector"));
      int mapStatus = mapping.getStatusLine().getStatusCode();
      if (mapStatus < 200 || mapStatus >= 300) {
        throw new SkipException("Vector field mapping not found; skipping semantic IT");
      }
    } catch (IOException e) {
      throw new RuntimeException("OpenSearch not reachable on localhost:9200", e);
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  public void testSemanticSearchAgainstLocalOpenSearch() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    EmbeddingProvider embeddingProvider = new ZerosEmbeddingProvider();
    MappingsBuilder mappingsBuilder = new NoOpMappingsBuilder();

    SemanticEntitySearchService service =
        new SemanticEntitySearchService(client, embeddingProvider, mappingsBuilder);

    // Query "anything"; embedding is zeros; expect request to succeed and return 0..N hits
    SearchResult result =
        service.search(opContext, List.of("dataset"), "customers", null, null, 0, 5);

    assertNotNull(result, "SearchResult should not be null");
    assertNotNull(result.getEntities(), "Entities should not be null");
    assertTrue(result.getFrom() == 0, "From should be 0");
    assertTrue(result.getPageSize() == 5, "PageSize should be 5");
    // Not asserting >0 hits; depends on local data
  }

  public void testSemanticSearchWithFilters() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    EmbeddingProvider embeddingProvider = new ZerosEmbeddingProvider();
    MappingsBuilder mappingsBuilder = new NoOpMappingsBuilder();

    SemanticEntitySearchService service =
        new SemanticEntitySearchService(client, embeddingProvider, mappingsBuilder);

    // Build a simple doc-level filter: platform == hive (adjust to a value present in your data)
    Criterion platformEq = new Criterion();
    platformEq.setField("platform");
    platformEq.setCondition(Condition.EQUAL);
    com.linkedin.data.template.StringArray values = new com.linkedin.data.template.StringArray();
    values.add("urn:li:dataPlatform:hive");
    platformEq.setValues(values);

    ConjunctiveCriterion and = new ConjunctiveCriterion();
    and.setAnd(new CriterionArray(platformEq));
    Filter filter = new Filter().setOr(new ConjunctiveCriterionArray(and));

    SearchResult result =
        service.search(opContext, java.util.List.of("dataset"), "customers", filter, null, 0, 5);

    assertNotNull(result, "SearchResult should not be null");
    assertEquals(result.getFrom().intValue(), 0);
    assertEquals(result.getPageSize().intValue(), 5);
    // We cannot assert hits>0 in a generic local cluster; just ensure request succeeded
  }

  private static final class ZerosEmbeddingProvider implements EmbeddingProvider {
    @Override
    public @Nonnull float[] embed(@Nonnull String text, @javax.annotation.Nullable String model) {
      // For tests, always return 1024-dimensional vector
      int expectedDimensions = 1024;
      float[] vec = new float[expectedDimensions];
      // Use a small non-zero constant to avoid cosine-similarity edge cases with zero vector
      for (int i = 0; i < expectedDimensions; i++) {
        vec[i] = 0.01f;
      }
      return vec;
    }
  }
}
