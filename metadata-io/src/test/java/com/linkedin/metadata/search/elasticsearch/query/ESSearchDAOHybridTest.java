package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESSearchDAOHybridTest {

  private static final List<String> ENTITY_NAMES = List.of("dataset", "document");
  private static final long TOTAL_HITS = 250;

  private SearchClientShim<?> client;
  private HybridSearchResultReranker reranker;
  private OperationContext opContext;
  private MetricUtils metrics;
  private ESSearchDAO dao;

  @BeforeMethod
  public void setUp() throws IOException {
    client = mock(SearchClientShim.class);
    opContext =
        spy(
            TestOperationContexts.withFixedSearchClient(
                    TestOperationContexts.systemContextNoValidate(), client)
                .withSearchFlags(flags -> flags.setFulltext(true)));
    metrics = mock(MetricUtils.class);
    doReturn(Optional.of(metrics)).when(opContext).getMetricUtils();
    reranker = mock(HybridSearchResultReranker.class);
    // Reverses the rows it is given, so the test can tell reranked rows from keyword ones
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              List<SearchEntity> rows = new ArrayList<>(invocation.getArgument(3));
              Collections.reverse(rows);
              return Optional.of(rows);
            });
    when(reranker.vectorEntityNames(any(OperationContext.class), any()))
        .thenReturn(Set.of("document"));
    dao =
        new ESSearchDAO(
            false,
            TEST_OS_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            false,
            TEST_SEARCH_SERVICE_CONFIG,
            new Sha256UrnEntityDocumentIdHasher(),
            reranker);
  }

  @Test
  public void testPageInsideTheWindowIsSlicedFromTheRerankedWindow() throws IOException {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);

    SearchResult result =
        dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 10, 10, List.of());

    // The window is reversed, so rows 10-19 are the keyword rows 89 down to 80
    assertEquals(rowIds(result), range(89, 79));
    assertEquals(result.getFrom().intValue(), 10);
    assertEquals(result.getPageSize().intValue(), 10);
    assertEquals(result.getNumEntities().intValue(), TOTAL_HITS);
    SearchSourceBuilder source = searchedSource();
    assertEquals(source.from(), 0);
    assertEquals(source.size(), 100);
    ArgumentCaptor<List<SearchEntity>> rows = ArgumentCaptor.forClass(List.class);
    verify(reranker)
        .rerank(
            any(OperationContext.class),
            eq(ENTITY_NAMES),
            eq("revenue"),
            rows.capture(),
            eq(List.of("urn")),
            anyLong());
    assertEquals(rows.getValue().size(), 100);
    verify(metrics).increment(ESSearchDAO.class, "hybridReadApplied", 1);
  }

  @Test
  public void testRowsPastTheWindowKeepTheirKeywordOrder() throws IOException {
    SearchResponse keywordResponse = response(105);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);

    SearchResult result =
        dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 95, 10, List.of());

    // The last five reranked rows, then keyword rows 100-104
    List<Integer> expected = new ArrayList<>(range(4, -1));
    expected.addAll(range(100, 105));
    assertEquals(rowIds(result), expected);
    assertEquals(searchedSource().size(), 105);
  }

  @Test
  public void testPagesPastTheWindowStayKeywordOnly() throws IOException {
    SearchResponse keywordResponse = response(10);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);

    dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 100, 10, List.of());

    assertEquals(searchedSource().from(), 100);
    assertEquals(searchedSource().size(), 10);
    verifyNoInteractions(reranker);
  }

  @Test
  public void testOnlyRelevanceRankedFullTextSearchesRerank() throws IOException {
    SearchResponse keywordResponse = response(10);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    List<SortCriterion> byName =
        List.of(new SortCriterion().setField("name").setOrder(SortOrder.ASCENDING));

    dao.search(opContext, ENTITY_NAMES, "revenue", null, byName, 0, 10, List.of());

    dao.search(
        opContext,
        ENTITY_NAMES,
        "revenue",
        null,
        List.of(new SortCriterion().setField("_score").setOrder(SortOrder.ASCENDING)),
        0,
        10,
        List.of());

    // The fetch from the top would pass the result limit of 1000

    dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 50, 1000, List.of());
    dao.search(opContext, ENTITY_NAMES, "*", null, null, 0, 10, List.of());
    dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 0, List.of());
    dao.search(
        opContext.withSearchFlags(flags -> flags.setFulltext(false)),
        ENTITY_NAMES,
        "revenue",
        null,
        null,
        0,
        10,
        List.of());

    verifyNoInteractions(reranker);
  }

  @Test
  public void testExactLookupsStayKeywordOnly() throws IOException {
    SearchResponse keywordResponse = response(10);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);

    for (String lookup :
        List.of(
            "\"revenue report\"",
            "'revenue report'",
            "urn:li:dataset:(urn:li:dataPlatform:hive,revenue,PROD)",
            "s3://my-bucket/revenue/2024")) {
      dao.search(opContext, ENTITY_NAMES, lookup, null, null, 0, 10, List.of());
    }

    // Each lookup fetched only its page, not the rerank window, and nothing was reranked
    ArgumentCaptor<SearchRequest> requests = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client, times(4))
        .search(any(OperationContext.class), requests.capture(), eq(RequestOptions.DEFAULT));
    requests.getAllValues().forEach(request -> assertEquals(request.source().size(), 10));
    verify(reranker, never())
        .rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong());
  }

  @Test
  public void testSearchWithoutVectorTypesStaysKeywordOnly() throws IOException {
    SearchResponse keywordResponse = response(10);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    when(reranker.vectorEntityNames(any(OperationContext.class), any())).thenReturn(Set.of());

    dao.search(opContext, List.of("corpuser"), "zelda", null, null, 0, 10, List.of());

    assertEquals(searchedSource().size(), 10);
  }

  @Test
  public void testWindowWithoutVectorRowsSkipsTheRerank() throws IOException {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    // Charts would have vectors, but the window holds only documents
    when(reranker.vectorEntityNames(any(OperationContext.class), any()))
        .thenReturn(Set.of("chart"));

    SearchResult result =
        dao.search(
            opContext, List.of("chart", "document"), "revenue", null, null, 0, 10, List.of());

    assertEquals(rowIds(result), range(0, 10));
    verify(reranker, org.mockito.Mockito.never())
        .rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong());
  }

  @Test
  public void testSlowRerankServesTheKeywordRanking() throws IOException {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              // Changes made by a rerank that runs past the timeout stay off the served rows
              List<SearchEntity> rows = invocation.getArgument(3);
              rows.forEach(row -> row.setScore(-1d));
              Thread.sleep(5_000);
              return Optional.of(rows);
            });

    SearchResult result =
        dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of());

    assertEquals(rowIds(result), range(0, 10));
    assertEquals(result.getEntities().get(0).getScore(), 100d);
  }

  @Test
  public void testHungProviderFreesItsWorkerAtTheDeadline() throws Exception {
    // Without the pause after repeated failures, so the search right after them uses the workers
    dao.setHybridPauseMillis(0);
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    AtomicBoolean hang = new AtomicBoolean(true);
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              List<SearchEntity> rows = new ArrayList<>(invocation.getArgument(3));
              if (hang.get()) {
                // A hung provider whose call the deadline ends; it ignores interrupts
                long deadlineNanos = invocation.getArgument(5);
                Uninterruptibles.sleepUninterruptibly(
                    deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
                throw new IOException("embedding call timed out");
              }
              Collections.reverse(rows);
              return Optional.of(rows);
            });

    // Enough searches at once to occupy every worker and fill the queue
    ExecutorService searches = Executors.newFixedThreadPool(24);
    try {
      List<Future<SearchResult>> hung = new ArrayList<>();
      for (int i = 0; i < 24; i++) {
        hung.add(
            searches.submit(
                () ->
                    dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of())));
      }
      for (Future<SearchResult> search : hung) {
        assertEquals(rowIds(search.get()), range(0, 10));
      }
    } finally {
      searches.shutdownNow();
    }
    hang.set(false);

    // The calls ended at their deadlines, so the workers come free right after them; a search is
    // reranked rather than rejected well before a provider's own 30-second timeout
    List<Integer> served = List.of();
    long giveUp = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!served.equals(range(99, 89)) && System.nanoTime() < giveUp) {
      served = rowIds(dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of()));
      if (!served.equals(range(99, 89))) {
        Thread.sleep(50);
      }
    }
    assertEquals(served, range(99, 89));
  }

  @Test
  public void testRepeatedFailuresPauseHybridRead() throws Exception {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenThrow(new IOException("provider rate limited"));
    dao.setHybridPauseMillis(1_000);

    for (int i = 0; i < 4; i++) {
      // Keyword order every time; the mocked client returns every hit whatever the page size
      assertEquals(
          rowIds(dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of()))
              .subList(0, 10),
          range(0, 10));
    }

    // Three failures in a row pause hybrid read: the fourth search calls neither the reranker nor
    // fetches the rerank window
    verify(reranker, times(3))
        .rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong());
    verify(metrics).increment(ESSearchDAO.class, "hybridReadSkipped", 1);
    ArgumentCaptor<SearchRequest> requests = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client, times(4))
        .search(any(OperationContext.class), requests.capture(), eq(RequestOptions.DEFAULT));
    assertEquals(requests.getAllValues().get(3).source().size(), 10);

    // After the pause, hybrid read tries again
    Thread.sleep(1_100);
    dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of());
    verify(reranker, times(4))
        .rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong());
  }

  @Test
  public void testTimedOutSearchesPauseHybridRead() throws Exception {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    // A slow provider: every rerank runs past the timeout, whose interrupt ends it
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              Thread.sleep(10_000);
              return Optional.empty();
            });

    // Three at once, so the test waits out one timeout rather than three
    ExecutorService searches = Executors.newFixedThreadPool(3);
    try {
      List<Future<SearchResult>> slow = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        slow.add(
            searches.submit(
                () ->
                    dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of())));
      }
      for (Future<SearchResult> search : slow) {
        assertEquals(rowIds(search.get()), range(0, 10));
      }
    } finally {
      searches.shutdownNow();
    }
    dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of());

    // Three timeouts in a row pause hybrid read, so the fourth search makes no rerank call
    verify(metrics, times(3)).increment(ESSearchDAO.class, "hybridReadTimeout", 1);
    verify(metrics).increment(ESSearchDAO.class, "hybridReadSkipped", 1);
    verify(reranker, times(3))
        .rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong());
  }

  @Test
  public void testRejectedSearchesPauseHybridRead() throws Exception {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    CountDownLatch release = new CountDownLatch(1);
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              release.await();
              return Optional.empty();
            });

    // 8 running and 16 queued reranks fill the hybrid executor, so the searches past them are
    // rejected
    ExecutorService searches = Executors.newFixedThreadPool(27);
    List<Future<SearchResult>> held = new ArrayList<>();
    try {
      for (int i = 0; i < 27; i++) {
        held.add(
            searches.submit(
                () ->
                    dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of())));
      }
      verify(metrics, timeout(1_000).atLeast(3))
          .increment(ESSearchDAO.class, "hybridReadRejected", 1);
      // The executor stays full, so each of these is rejected until three rejections in a row
      // pause hybrid read; the last one is skipped at the latest
      for (int i = 0; i < 4; i++) {
        dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of());
      }
      verify(metrics, atLeastOnce()).increment(ESSearchDAO.class, "hybridReadSkipped", 1);
    } finally {
      release.countDown();
      for (Future<SearchResult> search : held) {
        search.get();
      }
      searches.shutdown();
    }
    // Released well before their timeout: only the rejections can have paused hybrid read
    verify(metrics, never()).increment(ESSearchDAO.class, "hybridReadTimeout", 1);
  }

  @Test
  public void testInterruptedSearchesDoNotPauseHybridRead() throws Exception {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    Semaphore started = new Semaphore(0);
    AtomicBoolean hold = new AtomicBoolean(true);
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              if (hold.get()) {
                started.release();
                Thread.sleep(10_000);
              }
              return Optional.empty();
            });

    for (int i = 0; i < 3; i++) {
      Thread search =
          new Thread(
              () -> dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of()));
      search.start();
      // Interrupts the search while it waits for its rerank
      started.acquire();
      search.interrupt();
      search.join();
    }
    hold.set(false);
    dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of());

    // Interrupted searches do not count toward the pause, so the fourth search reranks
    verify(metrics, times(3)).increment(ESSearchDAO.class, "hybridReadFailed", 1);
    verify(metrics, never()).increment(ESSearchDAO.class, "hybridReadSkipped", 1);
    verify(reranker, times(4))
        .rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong());
  }

  @Test
  public void testRerankWithoutVectorsEndsTheFailureStreakButIsNotApplied() throws Exception {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    // Two failures, a rerank that completes but finds no vectors, then two more failures
    IOException rateLimited = new IOException("provider rate limited");
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenThrow(rateLimited, rateLimited)
        .thenReturn(Optional.empty())
        .thenThrow(rateLimited, rateLimited);

    for (int i = 0; i < 5; i++) {
      assertEquals(
          rowIds(dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of())),
          range(0, 10));
    }

    // No three failures in a row, so no search was skipped, and no search used vector scores
    verify(reranker, times(5))
        .rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong());
    verify(metrics, never()).increment(ESSearchDAO.class, "hybridReadSkipped", 1);
    verify(metrics, never()).increment(ESSearchDAO.class, "hybridReadApplied", 1);
  }

  @Test
  public void testWindowWithOneDocumentMakesNoRerankCall() throws IOException {
    SearchResponse keywordResponse = response(1);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);

    SearchResult result =
        dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of());

    // A single document has no other position to move into
    assertEquals(rowIds(result), List.of(0));
    verify(reranker, never())
        .rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong());
  }

  @Test
  public void testCallCutOffAtTheDeadlineCountsAsTimeout() throws IOException {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    // A provider that gives up at the deadline, as the bounded embedding call does
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              long deadlineNanos = invocation.getArgument(5);
              Uninterruptibles.sleepUninterruptibly(
                  deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
              throw new IOException("embedding call timed out");
            });

    SearchResult result =
        dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 0, 10, List.of());

    assertEquals(rowIds(result), range(0, 10));
    verify(metrics).increment(ESSearchDAO.class, "hybridReadTimeout", 1);
    verify(metrics, never()).increment(ESSearchDAO.class, "hybridReadFailed", 1);
  }

  @Test
  public void testFailedRerankServesTheKeywordRanking() throws IOException {
    SearchResponse keywordResponse = response(100);
    when(client.search(any(OperationContext.class), any(), eq(RequestOptions.DEFAULT)))
        .thenReturn(keywordResponse);
    when(reranker.rerank(any(OperationContext.class), any(), any(), anyList(), any(), anyLong()))
        .thenThrow(new IOException("kNN unavailable"));

    SearchResult result =
        dao.search(opContext, ENTITY_NAMES, "revenue", null, null, 10, 10, List.of());

    assertEquals(rowIds(result), range(10, 20));
    assertEquals(result.getNumEntities().intValue(), TOTAL_HITS);
    verify(metrics).increment(ESSearchDAO.class, "hybridReadFailed", 1);
  }

  private SearchSourceBuilder searchedSource() throws IOException {
    ArgumentCaptor<SearchRequest> request = ArgumentCaptor.forClass(SearchRequest.class);
    verify(client)
        .search(any(OperationContext.class), request.capture(), eq(RequestOptions.DEFAULT));
    return request.getValue().source();
  }

  /** Keyword hits for documents 0 to {@code count - 1}, best first. */
  private static SearchResponse response(int count) {
    SearchHit[] hits =
        IntStream.range(0, count)
            .mapToObj(
                i -> {
                  SearchHit hit = new SearchHit(i, "id" + i, Map.of(), Map.of());
                  hit.sourceRef(new BytesArray("{\"urn\":\"" + urn(i) + "\"}"));
                  hit.score(count - i);
                  return hit;
                })
            .toArray(SearchHit[]::new);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits())
        .thenReturn(
            new SearchHits(hits, new TotalHits(TOTAL_HITS, TotalHits.Relation.EQUAL_TO), count));
    return response;
  }

  private static Urn urn(int id) {
    return UrnUtils.getUrn("urn:li:document:" + id);
  }

  private static List<Integer> rowIds(SearchResult result) {
    return result.getEntities().stream()
        .map(entity -> Integer.parseInt(entity.getEntity().getId()))
        .collect(Collectors.toList());
  }

  /** Ids from {@code start} towards {@code end}, exclusive, counting down when end < start. */
  private static List<Integer> range(int start, int end) {
    return start <= end
        ? IntStream.range(start, end).boxed().collect(Collectors.toList())
        : IntStream.iterate(start, i -> i > end, i -> i - 1).boxed().collect(Collectors.toList());
  }
}
