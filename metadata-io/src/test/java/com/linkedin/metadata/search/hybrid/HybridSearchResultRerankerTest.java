package com.linkedin.metadata.search.hybrid;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.util.concurrent.UncheckedTimeoutException;
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
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
    when(embeddingProvider.embed(
            eq("revenue"),
            eq("text-embedding-3-small"),
            eq(EmbeddingTaskType.QUERY),
            any(Duration.class)))
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
        reranker
            .rerank(
                opContext,
                ENTITY_NAMES,
                "revenue",
                List.of(
                    row(DOC_A, 80),
                    row(DATASET, 70),
                    row(DOC_B, 60),
                    row(CHART, 50),
                    row(DOC_C, 40)),
                List.of("urn"),
                inSeconds(60))
            .orElseThrow();

    // The vector scores reorder the documents; the dataset and the chart keep their positions
    assertEquals(urns(reranked), List.of(DOC_B, DATASET, DOC_C, CHART, DOC_A));
    // Moved rows take the scores of their new positions, so callers sorting by score keep the order
    assertEquals(
        reranked.stream().map(SearchEntity::getScore).collect(Collectors.toList()),
        List.of(80d, 70d, 60d, 50d, 40d));
    ArgumentCaptor<KnnSearchRequest> request = ArgumentCaptor.forClass(KnnSearchRequest.class);
    verify(v3Client).searchKnn(any(OperationContext.class), request.capture());
    assertEquals(
        request.getValue().indexName(),
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(opContext, "document"));
    // The kNN query scores exactly the three document rows
    Map<?, ?> bool = (Map<?, ?>) request.getValue().filter().get().get("bool");
    assertEquals(
        ((Map<?, ?>) ((List<?>) bool.get("filter")).get(1)).get("terms"),
        Map.of("urn", List.of(DOC_A.toString(), DOC_B.toString(), DOC_C.toString())));
    // The kNN call gets the time left before the deadline, less than the whole budget
    Duration timeout = request.getValue().timeout().get();
    assertTrue(
        timeout.compareTo(Duration.ZERO) > 0 && timeout.compareTo(Duration.ofSeconds(60)) < 0);
    verify(primaryClient, never())
        .searchKnn(any(OperationContext.class), any(KnnSearchRequest.class));
  }

  @Test
  public void testRowWithoutVectorKeepsItsPosition() throws Exception {
    when(v3Client.searchKnn(any(OperationContext.class), any(KnnSearchRequest.class)))
        .thenReturn(knnHits(Map.of(DOC_B, 0.1d, DOC_C, 0.9d)));

    List<SearchEntity> reranked =
        reranker
            .rerank(
                opContext,
                ENTITY_NAMES,
                "revenue",
                List.of(row(DOC_A, 50), row(DOC_B, 50), row(DOC_C, 50)),
                List.of("urn"),
                inSeconds(60))
            .orElseThrow();

    // DOC_A has no vector, e.g. not embedded yet, so it stays first while the others trade places
    assertEquals(urns(reranked), List.of(DOC_A, DOC_C, DOC_B));
  }

  @Test
  public void testSingleRowWithVectorHitIsNotReranked() throws Exception {
    // DOC_A is not embedded yet, so DOC_B has no other position to move into
    when(v3Client.searchKnn(any(OperationContext.class), any(KnnSearchRequest.class)))
        .thenReturn(knnHits(Map.of(DOC_B, 0.4d)));
    MetricUtils metrics = mock(MetricUtils.class);
    List<SearchEntity> rows = List.of(row(DOC_A, 50), row(DOC_B, 50));

    assertEquals(
        reranker.rerank(
            metered(metrics), ENTITY_NAMES, "revenue", rows, List.of("urn"), inSeconds(60)),
        Optional.empty());
    verify(metrics).increment(HybridSearchResultReranker.class, "hybridReadNoVectors", 1);
  }

  @Test
  public void testRowsWithoutVectorTypesSkipEmbeddingAndKnn() throws Exception {
    List<SearchEntity> rows = List.of(row(DATASET, 70), row(CHART, 50));

    assertEquals(
        reranker.rerank(opContext, ENTITY_NAMES, "revenue", rows, List.of("urn"), inSeconds(60)),
        Optional.empty());
    verifyNoInteractions(embeddingProvider, v3Client);
  }

  @Test
  public void testSingleRowWithVectorsSkipsEmbeddingAndKnn() throws Exception {
    // The one document has no other document position to move into
    List<SearchEntity> rows = List.of(row(DOC_A, 70), row(DATASET, 50));

    assertEquals(
        reranker.rerank(opContext, ENTITY_NAMES, "revenue", rows, List.of("urn"), inSeconds(60)),
        Optional.empty());
    verifyNoInteractions(embeddingProvider, v3Client);
  }

  @Test
  public void testWildcardQuerySkipsEmbeddingAndKnn() throws Exception {
    List<SearchEntity> rows = List.of(row(DOC_A, 1));

    assertEquals(
        reranker.rerank(opContext, ENTITY_NAMES, "*", rows, List.of("urn"), inSeconds(60)),
        Optional.empty());
    verifyNoInteractions(embeddingProvider, v3Client);
  }

  @Test
  public void testRowsWithoutVectorHitsAreNotReranked() throws Exception {
    // e.g. none of the documents is embedded yet
    when(v3Client.searchKnn(any(OperationContext.class), any(KnnSearchRequest.class)))
        .thenReturn(knnHits(Map.of()));
    MetricUtils metrics = mock(MetricUtils.class);
    List<SearchEntity> rows = List.of(row(DOC_A, 50), row(DOC_B, 40));

    assertEquals(
        reranker.rerank(
            metered(metrics), ENTITY_NAMES, "revenue", rows, List.of("urn"), inSeconds(60)),
        Optional.empty());
    verify(metrics).increment(HybridSearchResultReranker.class, "hybridReadNoVectors", 1);
  }

  @Test
  public void testPartialKnnResponseFailsTheRerank() throws Exception {
    // A timed-out or failed shard may have dropped DOC_A's hit, so the rerank fails and the search
    // counts it toward the pause
    when(v3Client.searchKnn(any(OperationContext.class), any(KnnSearchRequest.class)))
        .thenReturn(new KnnSearchResponse(knnHits(Map.of(DOC_B, 0.9d)).hits(), true));
    MetricUtils metrics = mock(MetricUtils.class);
    List<SearchEntity> rows = List.of(row(DOC_A, 50), row(DOC_B, 40));

    assertThrows(
        IOException.class,
        () ->
            reranker.rerank(
                metered(metrics), ENTITY_NAMES, "revenue", rows, List.of("urn"), inSeconds(60)));
    verify(metrics).increment(HybridSearchResultReranker.class, "hybridReadPartial", 1);
  }

  @Test
  public void testDeadlinePassedDuringEmbeddingSkipsKnn() throws Exception {
    // The provider answers only after the time it was given
    when(embeddingProvider.embed(
            eq("revenue"),
            eq("text-embedding-3-small"),
            eq(EmbeddingTaskType.QUERY),
            any(Duration.class)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(((Duration) invocation.getArgument(3)).toMillis() + 50);
              return new float[] {0.1f, 0.2f};
            });
    List<SearchEntity> rows = List.of(row(DOC_A, 50), row(DOC_B, 40));

    // Reported as a timeout, so the caller counts it as one and serves the keyword ranking
    assertThrows(
        UncheckedTimeoutException.class,
        () ->
            reranker.rerank(
                opContext,
                ENTITY_NAMES,
                "revenue",
                rows,
                List.of("urn"),
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100)));
    verifyNoInteractions(v3Client);
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

  private OperationContext metered(MetricUtils metrics) {
    OperationContext metered = spy(opContext);
    doReturn(Optional.of(metrics)).when(metered).getMetricUtils();
    return metered;
  }

  private static long inSeconds(long seconds) {
    return System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
  }

  private static SearchEntity row(Urn urn, double score) {
    return new SearchEntity().setEntity(urn).setScore(score);
  }

  private static List<Urn> urns(List<SearchEntity> rows) {
    return rows.stream().map(SearchEntity::getEntity).collect(Collectors.toList());
  }
}
