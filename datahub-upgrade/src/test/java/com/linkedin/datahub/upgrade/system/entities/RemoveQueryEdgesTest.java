package com.linkedin.datahub.upgrade.system.entities;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.graph.elastic.ESGraphQueryDAO.RELATIONSHIP_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.BulkDeleteConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RemoveQueryEdgesTest {

  private static final String UPGRADE_ID = "RemoveQueryEdges_V1";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  @Mock private OperationContext mockOpContext;
  @Mock private EntityService<?> mockEntityService;
  @Mock private ESWriteDAO mockEsWriteDAO;
  @Mock private SearchContext mockSearchContext;
  @Mock private IndexConvention mockIndexConvention;
  @Mock private UpgradeContext mockUpgradeContext;

  private RemoveQueryEdges removeQueryEdges;
  private RemoveQueryEdges.RemoveQueryEdgesStep removeQueryEdgesStep;
  private BulkDeleteConfiguration deleteConfig;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Setup mock chain for index name resolution
    when(mockOpContext.getSearchContext()).thenReturn(mockSearchContext);
    when(mockSearchContext.getIndexConvention()).thenReturn(mockIndexConvention);
    when(mockIndexConvention.getIndexName(ElasticSearchGraphService.INDEX_NAME))
        .thenReturn("test_graph_index");

    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);

    this.deleteConfig = BulkDeleteConfiguration.builder().numRetries(1).build();
  }

  @Test
  public void testRemoveQueryEdgesEnabledCreatesStep() {
    // Test with enabled = true
    removeQueryEdges =
        new RemoveQueryEdges(mockOpContext, mockEntityService, mockEsWriteDAO, true, deleteConfig);

    assertEquals(removeQueryEdges.id(), "RemoveQueryEdges");
    List<UpgradeStep> steps = removeQueryEdges.steps();
    assertEquals(steps.size(), 1);
    assertTrue(steps.get(0) instanceof RemoveQueryEdges.RemoveQueryEdgesStep);
  }

  @Test
  public void testRemoveQueryEdgesDisabledCreatesNoSteps() {
    // Test with enabled = false
    removeQueryEdges =
        new RemoveQueryEdges(mockOpContext, mockEntityService, mockEsWriteDAO, false, deleteConfig);

    assertEquals(removeQueryEdges.id(), "RemoveQueryEdges");
    List<UpgradeStep> steps = removeQueryEdges.steps();
    assertTrue(steps.isEmpty());
  }

  @Test
  public void testStepId() {
    removeQueryEdgesStep =
        new RemoveQueryEdges.RemoveQueryEdgesStep(
            mockOpContext, mockEsWriteDAO, mockEntityService, deleteConfig);

    assertEquals(removeQueryEdgesStep.id(), UPGRADE_ID);
  }

  @Test
  public void testIsOptional() {
    removeQueryEdgesStep =
        new RemoveQueryEdges.RemoveQueryEdgesStep(
            mockOpContext, mockEsWriteDAO, mockEntityService, deleteConfig);

    assertTrue(removeQueryEdgesStep.isOptional());
  }

  @Test
  public void testSkipWhenPreviouslyRun() {
    removeQueryEdgesStep =
        new RemoveQueryEdges.RemoveQueryEdgesStep(
            mockOpContext, mockEsWriteDAO, mockEntityService, deleteConfig);

    // Mock that the upgrade was already run
    when(mockEntityService.exists(
            eq(mockOpContext),
            eq(UPGRADE_ID_URN),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(true);

    assertTrue(removeQueryEdgesStep.skip(mockUpgradeContext));

    verify(mockEntityService)
        .exists(
            eq(mockOpContext),
            eq(UPGRADE_ID_URN),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true));
  }

  @Test
  public void testDontSkipWhenNotPreviouslyRun() {
    removeQueryEdgesStep =
        new RemoveQueryEdges.RemoveQueryEdgesStep(
            mockOpContext, mockEsWriteDAO, mockEntityService, deleteConfig);

    // Mock that the upgrade was not run before
    when(mockEntityService.exists(
            eq(mockOpContext),
            eq(UPGRADE_ID_URN),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(false);

    assertFalse(removeQueryEdgesStep.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableSuccess() throws Exception {
    removeQueryEdgesStep =
        new RemoveQueryEdges.RemoveQueryEdgesStep(
            mockOpContext, mockEsWriteDAO, mockEntityService, deleteConfig);

    // Mock successful delete operation
    ESWriteDAO.DeleteByQueryResult mockResult =
        ESWriteDAO.DeleteByQueryResult.builder()
            .success(true)
            .remainingDocuments(0)
            .timeTaken(1000)
            .retryAttempts(0)
            .build();

    when(mockEsWriteDAO.deleteByQuerySync(
            any(String.class), any(QueryBuilder.class), eq(deleteConfig)))
        .thenReturn(mockResult);

    // Execute the step
    UpgradeStepResult result = removeQueryEdgesStep.executable().apply(mockUpgradeContext);

    // Verify the result
    assertEquals(result.stepId(), UPGRADE_ID);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Capture and verify the delete query
    ArgumentCaptor<String> indexCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<QueryBuilder> queryCaptor = ArgumentCaptor.forClass(QueryBuilder.class);

    verify(mockEsWriteDAO)
        .deleteByQuerySync(indexCaptor.capture(), queryCaptor.capture(), eq(deleteConfig));

    assertEquals(indexCaptor.getValue(), "test_graph_index");

    // Verify the query structure
    QueryBuilder capturedQuery = queryCaptor.getValue();
    assertTrue(capturedQuery instanceof BoolQueryBuilder);

    BoolQueryBuilder boolQuery = (BoolQueryBuilder) capturedQuery;

    // Verify the filters
    assertEquals(boolQuery.filter().size(), 2);

    // Verify relationship type filter
    boolean hasRelationshipFilter =
        boolQuery.filter().stream()
            .anyMatch(
                filter ->
                    filter instanceof TermQueryBuilder
                        && ((TermQueryBuilder) filter).fieldName().equals(RELATIONSHIP_TYPE)
                        && ((TermQueryBuilder) filter).value().equals("IsAssociatedWith"));
    assertTrue(hasRelationshipFilter, "Should have relationship type filter");

    // Verify source URN prefix filter
    boolean hasSourceFilter =
        boolQuery.filter().stream()
            .anyMatch(
                filter ->
                    filter instanceof TermQueryBuilder
                        && ((TermQueryBuilder) filter).fieldName().equals("source.entityType")
                        && ((TermQueryBuilder) filter).value().equals("query"));
    assertTrue(hasSourceFilter, "Should have source URN prefix filter");

    // Verify that bootstrap result was set
    verify(mockEntityService).ingestProposal(eq(mockOpContext), any(), any(), anyBoolean());
  }

  @Test
  public void testExecutableWhenDeleteFails() throws Exception {
    removeQueryEdgesStep =
        new RemoveQueryEdges.RemoveQueryEdgesStep(
            mockOpContext, mockEsWriteDAO, mockEntityService, deleteConfig);

    // Mock failed delete operation
    ESWriteDAO.DeleteByQueryResult mockResult =
        ESWriteDAO.DeleteByQueryResult.builder()
            .success(false)
            .failureReason("Connection timeout")
            .remainingDocuments(100)
            .timeTaken(5000)
            .retryAttempts(3)
            .build();

    when(mockEsWriteDAO.deleteByQuerySync(
            any(String.class), any(QueryBuilder.class), eq(deleteConfig)))
        .thenReturn(mockResult);

    // Execute the step - it should still succeed since the step is optional
    UpgradeStepResult result = removeQueryEdgesStep.executable().apply(mockUpgradeContext);

    // The step should still return success even if delete failed (since it's optional)
    assertEquals(result.stepId(), UPGRADE_ID);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that bootstrap result was still set
    verify(mockEntityService).ingestProposal(eq(mockOpContext), any(), any(), anyBoolean());
  }

  @Test
  public void testExecutableException() throws Exception {
    removeQueryEdgesStep =
        new RemoveQueryEdges.RemoveQueryEdgesStep(
            mockOpContext, mockEsWriteDAO, mockEntityService, deleteConfig);

    // Mock exception during delete
    when(mockEsWriteDAO.deleteByQuerySync(
            any(String.class), any(QueryBuilder.class), eq(deleteConfig)))
        .thenThrow(new RuntimeException("Elasticsearch connection failed"));

    UpgradeStepResult result = removeQueryEdgesStep.executable().apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }
}
