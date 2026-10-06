package com.linkedin.metadata.search.hybrid;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HybridSearchResultRerankerTest {

  private static final Urn DOC_A = UrnUtils.getUrn("urn:li:document:a");
  private static final Urn DOC_B = UrnUtils.getUrn("urn:li:document:b");
  private static final Urn DOC_C = UrnUtils.getUrn("urn:li:document:c");
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)");
  private static final Urn CHART = UrnUtils.getUrn("urn:li:chart:(looker,orders)");
  private static final List<String> ENTITY_NAMES = List.of("dataset", "document", "chart");

  private SearchClientShim<?> primaryClient;
  private SearchClientShim<?> v3Client;
  private OperationContext opContext;
  private EmbeddingProvider embeddingProvider;
  private HybridSearchResultReranker reranker;

  @BeforeMethod
  public void setUp() {
    primaryClient = mock(SearchClientShim.class);
    v3Client = mock(SearchClientShim.class);
    opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoValidate(),
            component -> component == SearchComponent.SEARCH_V3 ? v3Client : primaryClient);
    embeddingProvider = mock(EmbeddingProvider.class);
    when(embeddingProvider.embed("revenue", "text-embedding-3-small", EmbeddingTaskType.QUERY))
        .thenReturn(new float[] {0.1f, 0.2f});
    reranker =
        new HybridSearchResultReranker(
            new HybridQueryEmbeddingService(
                embeddingProvider, "text-embedding-3-small", "text_embedding_3_small"),
            new V3HybridKnnRequestBuilder(V3HybridKnnRequestBuilderTest.semanticSearch(true)),
            new HybridScoreMapBuilder(
                new HybridVectorScoreNormalizer(SearchEngineType.ELASTICSEARCH_8, "cosine")),
            new HybridCandidateMerger(
                new HybridLexicalScoreNormalizer(0d, 100d, 4d), new HybridScoreCombiner(0.5d)));
  }

  @Test
  public void testDocumentsMoveOnlyAmongDocumentPositions() throws Exception {
    when(v3Client.searchKnn(any(OperationContext.class), any(KnnSearchRequest.class)))
        .thenReturn(knnHits(Map.of(DOC_A, 0.1d, DOC_B, 0.9d, DOC_C, 0.95d)));

    List<SearchEntity> reranked =
        reranker.rerank(
            opContext,
            ENTITY_NAMES,
            "revenue",
            List.of(
                row(DOC_A, 80), row(DATASET, 70), row(DOC_B, 60), row(CHART, 50), row(DOC_C, 40)),
            200,
            List.of("urn"),
            null);

    // The vector scores reorder the documents; the dataset and the chart keep their positions
    assertEquals(urns(reranked), List.of(DOC_B, DATASET, DOC_C, CHART, DOC_A));
    ArgumentCaptor<KnnSearchRequest> request = ArgumentCaptor.forClass(KnnSearchRequest.class);
    verify(v3Client).searchKnn(any(OperationContext.class), request.capture());
    assertEquals(
        request.getValue().indexName(),
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(opContext, "document"));
    assertEquals(request.getValue().k(), 200);
    verify(primaryClient, never())
        .searchKnn(any(OperationContext.class), any(KnnSearchRequest.class));
  }

  @Test
  public void testDocumentWithoutVectorHitRanksBelowCloseOnes() throws Exception {
    when(v3Client.searchKnn(any(OperationContext.class), any(KnnSearchRequest.class)))
        .thenReturn(knnHits(Map.of(DOC_B, 0.4d)));

    List<SearchEntity> reranked =
        reranker.rerank(
            opContext,
            ENTITY_NAMES,
            "revenue",
            List.of(row(DOC_A, 50), row(DOC_B, 50)),
            200,
            List.of("urn"),
            null);

    assertEquals(urns(reranked), List.of(DOC_B, DOC_A));
  }

  @Test
  public void testRowsWithoutVectorTypesSkipEmbeddingAndKnn() throws Exception {
    List<SearchEntity> rows = List.of(row(DATASET, 70), row(CHART, 50));

    assertEquals(
        reranker.rerank(opContext, ENTITY_NAMES, "revenue", rows, 200, List.of("urn"), null), rows);
    verifyNoInteractions(embeddingProvider, v3Client);
  }

  @Test
  public void testWildcardQuerySkipsEmbeddingAndKnn() throws Exception {
    List<SearchEntity> rows = List.of(row(DOC_A, 1));

    assertEquals(
        reranker.rerank(opContext, ENTITY_NAMES, "*", rows, 200, List.of("urn"), null), rows);
    verifyNoInteractions(embeddingProvider, v3Client);
  }

  @Test
  public void testReorderKeepsRepeatedRowsInPlace() {
    List<SearchEntity> rows = List.of(row(DOC_A, 3), row(DOC_B, 2), row(DOC_A, 1));
    List<HybridCandidate> candidates =
        List.of(
            new HybridCandidate(DOC_B, 0d, 0d, 0d, 0d, 0.9d),
            new HybridCandidate(DOC_A, 0d, 0d, 0d, 0d, 0.1d));

    assertEquals(
        urns(HybridSearchResultReranker.reorder(rows, candidates)), List.of(DOC_B, DOC_A, DOC_A));
  }

  private static KnnSearchResponse knnHits(Map<Urn, Double> scores) {
    return new KnnSearchResponse(
        scores.entrySet().stream()
            .map(
                entry ->
                    new KnnSearchResponse.Hit(
                        "hashed-id", entry.getValue(), Map.of("urn", entry.getKey().toString())))
            .collect(Collectors.toList()));
  }

  private static SearchEntity row(Urn urn, double score) {
    return new SearchEntity().setEntity(urn).setScore(score);
  }

  private static List<Urn> urns(List<SearchEntity> rows) {
    return rows.stream().map(SearchEntity::getEntity).collect(Collectors.toList());
  }
}
