package com.linkedin.metadata.graph.elastic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import org.mockito.ArgumentCaptor;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESGraphWriteDAOProceedOnConflictTest {
  private static final String GRAPH_INDEX = "graph_service_v1";
  private final OperationContext base = TestOperationContexts.systemContextNoValidate();
  private ESBulkProcessor bulkProcessor;
  private ESGraphWriteDAO dao;

  @BeforeMethod
  public void setup() {
    bulkProcessor = mock(ESBulkProcessor.class);
    dao =
        new ESGraphWriteDAO(
            IndexConventionImpl.noPrefix("md5", SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION),
            bulkProcessor,
            0,
            GraphQueryConfiguration.builder().graphStatusEnabled(true).build());
  }

  @Test
  public void includeSoftDeletedDropsTheSoftDeleteExclusion() {
    OperationContext including = base.withSearchFlags(f -> f.setIncludeSoftDeleted(true));
    OperationContext excluding = base.withSearchFlags(f -> f.setIncludeSoftDeleted(false));

    dao.deleteByQueryProceedOnConflict(including, GraphFilters.ALL, null);
    dao.deleteByQueryProceedOnConflict(excluding, GraphFilters.ALL, null);

    ArgumentCaptor<QueryBuilder> queries = ArgumentCaptor.forClass(QueryBuilder.class);
    verify(bulkProcessor, times(2))
        .deleteByQueryProceedOnConflict(
            any(OperationContext.class), queries.capture(), eq(true), eq(GRAPH_INDEX));
    assertTrue(((BoolQueryBuilder) queries.getAllValues().get(0)).mustNot().isEmpty());
    assertFalse(((BoolQueryBuilder) queries.getAllValues().get(1)).mustNot().isEmpty());
  }

  @Test
  public void readOnlyModeFailsInsteadOfReportingSuccess() {
    dao.setWritable(false);

    expectThrows(
        IllegalStateException.class,
        () -> dao.deleteByQueryProceedOnConflict(base, GraphFilters.ALL, null));
    verifyNoInteractions(bulkProcessor);
  }
}
