package com.linkedin.metadata.graph.elastic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ElasticSearchGraphServiceRemoveNodeTest {
  private static final Urn URN = UrnUtils.getUrn("urn:li:container:remove-node-test");
  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();
  private ESGraphWriteDAO writeDAO;
  private ElasticSearchGraphService service;

  @BeforeMethod
  public void setup() {
    writeDAO = mock(ESGraphWriteDAO.class);
    service =
        new ElasticSearchGraphService(
            new LineageRegistry(new TestEntityRegistry()),
            mock(ESBulkProcessor.class),
            IndexConventionImpl.noPrefix("md5", SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION),
            writeDAO,
            mock(ESGraphQueryDAO.class),
            mock(ESIndexBuilder.class),
            "md5");
  }

  @Test
  public void deletesOutgoingIncomingAndOwnedEdgesWithSoftDeletedIncluded() {
    service.removeNodeReportingFailures(opContext, URN, true);

    ArgumentCaptor<OperationContext> contexts = ArgumentCaptor.forClass(OperationContext.class);
    verify(writeDAO, times(2))
        .deleteByQueryProceedOnConflict(
            any(OperationContext.class), any(GraphFilters.class), isNull());
    verify(writeDAO)
        .deleteByQueryProceedOnConflict(
            any(OperationContext.class), eq(GraphFilters.ALL), eq(URN.toString()));
    verify(writeDAO, times(3))
        .deleteByQueryProceedOnConflict(contexts.capture(), any(GraphFilters.class), any());
    contexts
        .getAllValues()
        .forEach(ctx -> assertTrue(ctx.getSearchContext().getSearchFlags().isIncludeSoftDeleted()));
  }

  /** An edge that may remain (a conflict, a failure, a timeout) fails the node removal. */
  @Test
  public void anIncompleteEdgeDeleteFailsTheRemoval() {
    doThrow(new IllegalStateException("2 version conflicts"))
        .when(writeDAO)
        .deleteByQueryProceedOnConflict(
            any(OperationContext.class), any(GraphFilters.class), any());

    expectThrows(
        IllegalStateException.class,
        () -> service.removeNodeReportingFailures(opContext, URN, true));
  }
}
