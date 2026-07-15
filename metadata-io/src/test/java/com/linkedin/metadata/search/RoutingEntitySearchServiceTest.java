package com.linkedin.metadata.search;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.opensearch.action.explain.ExplainResponse;
import org.testng.annotations.Test;

/**
 * Verifies read/write routing (same split as OpenSearch: reads vs index mutations). Mirrors how
 * {@link com.linkedin.metadata.search.elasticsearch.ElasticSearchService} stays authoritative for
 * writes while pgSearch serves reads.
 */
public class RoutingEntitySearchServiceTest {

  private static final SearchServiceConfiguration SEARCH_CFG =
      SearchServiceConfiguration.builder()
          .limit(
              LimitConfig.builder()
                  .results(ResultsLimitConfig.builder().apiDefault(100).max(1000).build())
                  .build())
          .build();

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void search_delegatesToPostgres_notElasticsearch() {
    ElasticSearchService es = mock(ElasticSearchService.class);
    PostgresEntitySearchService pg = mock(PostgresEntitySearchService.class);
    when(pg.search(
            any(OperationContext.class),
            anyList(),
            any(),
            any(),
            anyList(),
            anyInt(),
            any(),
            anyList()))
        .thenReturn(new SearchResult());

    RoutingEntitySearchService routing = new RoutingEntitySearchService(es, pg, SEARCH_CFG);

    routing.search(opContext, List.of("dataset"), "test", null, List.of(), 0, 10, List.of());

    verify(pg)
        .search(eq(opContext), anyList(), any(), any(), anyList(), anyInt(), any(), anyList());
    verify(es, never())
        .search(any(OperationContext.class), anyList(), any(), any(), anyList(), anyInt(), any());
  }

  @Test
  public void upsertDocument_delegatesToElasticsearch_only() {
    ElasticSearchService es = mock(ElasticSearchService.class);
    PostgresEntitySearchService pg = mock(PostgresEntitySearchService.class);
    RoutingEntitySearchService routing = new RoutingEntitySearchService(es, pg, SEARCH_CFG);

    routing.upsertDocument(opContext, "dataset", "{}", "id");

    verify(es).upsertDocument(opContext, "dataset", "{}", "id");
    verify(pg, never()).upsertDocument(any(), any(), any(), any());
  }

  @Test
  public void clear_delegatesToElasticsearch_only() {
    ElasticSearchService es = mock(ElasticSearchService.class);
    PostgresEntitySearchService pg = mock(PostgresEntitySearchService.class);
    RoutingEntitySearchService routing = new RoutingEntitySearchService(es, pg, SEARCH_CFG);

    routing.clear(opContext);

    verify(es).clear(opContext);
    verify(pg, never()).clear(any());
  }

  @Test
  public void validateAndSwapAlias_delegatesToElasticsearch_only() throws Exception {
    ElasticSearchService es = mock(ElasticSearchService.class);
    PostgresEntitySearchService pg = mock(PostgresEntitySearchService.class);
    RoutingEntitySearchService routing = new RoutingEntitySearchService(es, pg, SEARCH_CFG);

    routing.validateAndSwapAlias(opContext, "a", "b");

    verify(es).validateAndSwapAlias(opContext, "a", "b");
    verify(pg, never()).validateAndSwapAlias(any(), any(), any());
  }

  @Test
  public void autoComplete_delegatesToPostgres_only() {
    ElasticSearchService es = mock(ElasticSearchService.class);
    PostgresEntitySearchService pg = mock(PostgresEntitySearchService.class);
    when(pg.autoComplete(any(), any(), any(), any(), any(), any()))
        .thenReturn(new AutoCompleteResult());
    RoutingEntitySearchService routing = new RoutingEntitySearchService(es, pg, SEARCH_CFG);

    routing.autoComplete(opContext, "dataset", "q", null, null, 10);

    verify(pg).autoComplete(eq(opContext), eq("dataset"), eq("q"), isNull(), isNull(), eq(10));
    verify(es, never()).autoComplete(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void aggregateByValue_delegatesToPostgres_only() {
    ElasticSearchService es = mock(ElasticSearchService.class);
    PostgresEntitySearchService pg = mock(PostgresEntitySearchService.class);
    when(pg.aggregateByValue(any(), any(), any(), any(), any())).thenReturn(Map.of());
    RoutingEntitySearchService routing = new RoutingEntitySearchService(es, pg, SEARCH_CFG);

    routing.aggregateByValue(opContext, List.of("dataset"), "_entityType", null, 20);

    verify(pg)
        .aggregateByValue(
            eq(opContext), eq(List.of("dataset")), eq("_entityType"), isNull(), eq(20));
    verify(es, never()).aggregateByValue(any(), any(), any(), any(), any());
  }

  @Test
  public void explain_delegatesToPostgres_only() {
    ElasticSearchService es = mock(ElasticSearchService.class);
    PostgresEntitySearchService pg = mock(PostgresEntitySearchService.class);
    ExplainResponse er = mock(ExplainResponse.class);
    when(pg.explain(any(), any(), any(), any(), any(), any(), any(), any(), any(), anyList()))
        .thenReturn(er);
    RoutingEntitySearchService routing = new RoutingEntitySearchService(es, pg, SEARCH_CFG);

    routing.explain(
        opContext,
        "needle",
        "urn:li:dataset:(urn:li:dataPlatform:hive,x,PROD)",
        "dataset",
        null,
        List.of(),
        null,
        null,
        10,
        List.of());

    verify(pg)
        .explain(
            eq(opContext),
            eq("needle"),
            eq("urn:li:dataset:(urn:li:dataPlatform:hive,x,PROD)"),
            eq("dataset"),
            isNull(),
            eq(List.of()),
            isNull(),
            isNull(),
            eq(10),
            eq(List.of()));
    verify(es, never())
        .explain(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
  }
}
