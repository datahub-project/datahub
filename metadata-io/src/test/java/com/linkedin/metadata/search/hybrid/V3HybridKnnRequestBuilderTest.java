package com.linkedin.metadata.search.hybrid;

import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class V3HybridKnnRequestBuilderTest {

  private static final Duration TIMEOUT = Duration.ofMillis(1_500);

  private static final String MODEL_KEY = "text_embedding_3_small";
  private static final Urn DOC_A = UrnUtils.getUrn("urn:li:document:a");
  private static final Urn DOC_B = UrnUtils.getUrn("urn:li:document:b");

  private OperationContext opContext;
  private V3HybridKnnRequestBuilder builder;

  @BeforeMethod
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoValidate();
    builder = new V3HybridKnnRequestBuilder(semanticSearch(true));
  }

  @Test
  public void testMixedQuerySearchesOnlyTheDocumentIndex() {
    List<String> entityNames = List.of("dataset", "document", "chart");

    KnnSearchRequest request = build(builder, entityNames, List.of(DOC_A, DOC_B));

    assertEquals(
        request.indexName(),
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(opContext, "document"));
    assertEquals(request.vectorField(), "embeddings.text_embedding_3_small.chunks.vector");
    // The filter bounds the hits to the given rows; k and num_candidates exceed them
    assertEquals(request.k(), V3HybridKnnRequestBuilder.MAX_K);
    assertEquals(request.numCandidates(), 10_000);
    assertEquals(request.timeout().get(), TIMEOUT);
    assertEquals(request.fieldsToFetch(), List.of("urn"));
    assertEquals(request.filter().get(), filters(documentTypeFilter(), urnFilter(DOC_A, DOC_B)));
    assertEquals(builder.vectorEntityNames(opContext, entityNames), Set.of("document"));
  }

  @Test
  public void testNoRequestWithoutEntityTypesThatHaveVectors() {
    assertFalse(
        builder
            .build(
                opContext,
                List.of("dataset", "chart"),
                MODEL_KEY,
                new float[] {0.1f},
                List.of(DOC_A),
                List.of("urn"),
                TIMEOUT)
            .isPresent());
    // With semantic search off no V3 index has vectors
    assertFalse(
        new V3HybridKnnRequestBuilder(semanticSearch(false))
            .build(
                opContext,
                List.of("document"),
                MODEL_KEY,
                new float[] {0.1f},
                List.of(DOC_A),
                List.of("urn"),
                TIMEOUT)
            .isPresent());
  }

  @Test
  public void testNoRequestWithoutRows() {
    assertFalse(
        builder
            .build(
                opContext,
                List.of("document"),
                MODEL_KEY,
                new float[] {0.1f},
                List.of(),
                List.of("urn"),
                TIMEOUT)
            .isPresent());
  }

  @Test
  public void testVectorFieldRejectsBlankModelKey() {
    assertThrows(IllegalArgumentException.class, () -> V3HybridKnnRequestBuilder.vectorField(" "));
  }

  private KnnSearchRequest build(
      V3HybridKnnRequestBuilder builder, List<String> entityNames, List<Urn> urns) {
    return builder
        .build(
            opContext,
            entityNames,
            MODEL_KEY,
            new float[] {0.1f, 0.2f},
            urns,
            List.of("urn"),
            TIMEOUT)
        .orElseThrow();
  }

  @SafeVarargs
  private static Map<String, Object> filters(Map<String, Object>... filters) {
    return Map.of("bool", Map.of("filter", List.of(filters)));
  }

  private static Map<String, Object> urnFilter(Urn... urns) {
    return Map.of(
        "terms",
        Map.of(
            "urn",
            java.util.Arrays.stream(urns)
                .map(Urn::toString)
                .collect(java.util.stream.Collectors.toList())));
  }

  private static Map<String, Object> documentTypeFilter() {
    return Map.of("terms", Map.of(INDEX_VIRTUAL_FIELD, List.of("document")));
  }

  static SemanticSearchConfiguration semanticSearch(boolean enabled) {
    return new SemanticSearchConfiguration(
        enabled, Set.of("document"), Map.of(MODEL_KEY, new ModelEmbeddingConfig()), null);
  }
}
