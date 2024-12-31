package com.linkedin.metadata.service.search;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.explain.ExplainResponse;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class EntitySearchServiceTest {
  private static final Urn TEST_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.human_profiles,PROD)");

  private OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  private EntitySearchService testInstance;

  @BeforeClass
  public void setup() {
    testInstance = spy(new TestEntitySearchService());
  }

  @Test
  public void testAppendRunId_EmptyList() {
    List<IngestResult> results = new ArrayList<>();
    testInstance.appendRunId(opContext, results);
    // Verify no interactions since list is empty
    verify(testInstance, never()).appendRunId(any(), any(Urn.class), anyString());
  }

  @Test
  public void testAppendRunId_NullResults() {
    List<IngestResult> results = new ArrayList<>();
    results.add(null);
    testInstance.appendRunId(opContext, results);
    // Verify no interactions since all results are null
    verify(testInstance, never()).appendRunId(any(), any(Urn.class), anyString());
  }

  @Test
  public void testAppendRunId_ValidResult() {
    // Create test data
    List<IngestResult> results = new ArrayList<>();
    IngestResult result = mock(IngestResult.class);
    BatchItem mockRequest = mock(BatchItem.class);
    SystemMetadata mockSystemMetadata = mock(SystemMetadata.class);

    // Setup mock behaviors
    when(result.getUrn()).thenReturn(TEST_URN);
    when(result.isProcessedMCL()).thenReturn(true);
    when(result.getResult()).thenReturn(mock(UpdateAspectResult.class));
    when(result.getRequest()).thenReturn(mockRequest);
    when(mockRequest.getSystemMetadata()).thenReturn(mockSystemMetadata);
    when(mockSystemMetadata.hasRunId()).thenReturn(true);
    when(mockSystemMetadata.getRunId()).thenReturn("test-run-id");
    when(mockRequest.getEntitySpec())
        .thenReturn(opContext.getEntityRegistry().getEntitySpec(TEST_URN.getEntityType()));
    when(mockRequest.getAspectName()).thenReturn("status");

    results.add(result);

    // Execute
    testInstance.appendRunId(opContext, results);

    // Verify appendRunId was called with correct parameters
    verify(testInstance).appendRunId(eq(opContext), eq(TEST_URN), eq("test-run-id"));
  }

  @Test
  public void testAppendRunId_KeyAspectMatch() {
    // Create test data
    List<IngestResult> results = new ArrayList<>();
    IngestResult result = mock(IngestResult.class);
    BatchItem mockRequest = mock(BatchItem.class);
    SystemMetadata mockSystemMetadata = mock(SystemMetadata.class);

    // Setup mock behaviors
    when(result.getUrn()).thenReturn(TEST_URN);
    when(result.isProcessedMCL()).thenReturn(true);
    when(result.getResult()).thenReturn(mock(UpdateAspectResult.class));
    when(result.getRequest()).thenReturn(mockRequest);
    when(mockRequest.getSystemMetadata()).thenReturn(mockSystemMetadata);
    when(mockSystemMetadata.hasRunId()).thenReturn(true);
    when(mockSystemMetadata.getRunId()).thenReturn("test-run-id");
    when(mockRequest.getEntitySpec())
        .thenReturn(opContext.getEntityRegistry().getEntitySpec(TEST_URN.getEntityType()));
    when(mockRequest.getAspectName())
        .thenReturn(
            opContext
                .getEntityRegistry()
                .getEntitySpec(TEST_URN.getEntityType())
                .getKeyAspectName());

    results.add(result);

    // Execute
    testInstance.appendRunId(opContext, results);

    // Verify appendRunId was not called because aspect names match
    verify(testInstance, never()).appendRunId(any(), any(Urn.class), anyString());
  }

  @Test
  public void testAppendRunId_NoRunId() {
    // Create test data
    List<IngestResult> results = new ArrayList<>();
    IngestResult result = mock(IngestResult.class);
    BatchItem mockRequest = mock(BatchItem.class);
    SystemMetadata mockSystemMetadata = mock(SystemMetadata.class);

    // Setup mock behaviors
    when(result.getUrn()).thenReturn(TEST_URN);
    when(result.isProcessedMCL()).thenReturn(true);
    when(result.getResult()).thenReturn(mock(UpdateAspectResult.class));
    when(result.getRequest()).thenReturn(mockRequest);
    when(mockRequest.getSystemMetadata()).thenReturn(mockSystemMetadata);
    when(mockSystemMetadata.hasRunId()).thenReturn(false);

    results.add(result);

    // Execute
    testInstance.appendRunId(opContext, results);

    // Verify appendRunId was not called because there's no run ID
    verify(testInstance, never()).appendRunId(any(), any(Urn.class), anyString());
  }

  @Test
  public void testAppendRunId_NotProcessedOrUpdated() {
    // Create test data
    List<IngestResult> results = new ArrayList<>();
    IngestResult result = mock(IngestResult.class);

    // Setup mock behaviors
    when(result.getUrn()).thenReturn(TEST_URN);
    when(result.isProcessedMCL()).thenReturn(false);
    when(result.isUpdate()).thenReturn(false);

    results.add(result);

    // Execute
    testInstance.appendRunId(opContext, results);

    // Verify appendRunId was not called because result is neither processed nor updated
    verify(testInstance, never()).appendRunId(any(), any(Urn.class), anyString());
  }

  private static class TestEntitySearchService implements EntitySearchService {
    @Override
    public void clear(OperationContext opContext) {}

    @Override
    public long docCount(OperationContext opContext, String entityName, Filter filter) {
      return 0;
    }

    @Override
    public void upsertDocument(
        OperationContext opContext, String entityName, String document, String docId) {}

    @Override
    public void deleteDocument(OperationContext opContext, String entityName, String docId) {}

    @Override
    public void appendRunId(OperationContext opContext, Urn urn, String runId) {}

    @Override
    public SearchResult search(
        OperationContext opContext,
        List<String> entityNames,
        String input,
        Filter postFilters,
        List<SortCriterion> sortCriteria,
        int from,
        int size) {
      return null;
    }

    @Override
    public SearchResult search(
        OperationContext opContext,
        List<String> entityNames,
        String input,
        Filter postFilters,
        List<SortCriterion> sortCriteria,
        int from,
        int size,
        List<String> facets) {
      return null;
    }

    @Override
    public SearchResult filter(
        OperationContext opContext,
        String entityName,
        Filter filters,
        List<SortCriterion> sortCriteria,
        int from,
        int size) {
      return null;
    }

    @Override
    public AutoCompleteResult autoComplete(
        OperationContext opContext,
        String entityName,
        String query,
        String field,
        Filter requestParams,
        int limit) {
      return null;
    }

    @Override
    public Map<String, Long> aggregateByValue(
        OperationContext opContext,
        List<String> entityNames,
        String field,
        Filter requestParams,
        int limit) {
      return null;
    }

    @Override
    public BrowseResult browse(
        OperationContext opContext,
        String entityName,
        String path,
        Filter requestParams,
        int from,
        int size) {
      return null;
    }

    @Override
    public BrowseResultV2 browseV2(
        OperationContext opContext,
        String entityName,
        String path,
        Filter filter,
        String input,
        int start,
        int count) {
      return null;
    }

    @Nonnull
    @Override
    public BrowseResultV2 browseV2(
        @Nonnull OperationContext opContext,
        @Nonnull List<String> entityNames,
        @Nonnull String path,
        @Nullable Filter filter,
        @Nonnull String input,
        int start,
        int count) {
      return null;
    }

    @Override
    public List<String> getBrowsePaths(OperationContext opContext, String entityName, Urn urn) {
      return null;
    }

    @Override
    public ScrollResult fullTextScroll(
        OperationContext opContext,
        List<String> entities,
        String input,
        Filter postFilters,
        List<SortCriterion> sortCriteria,
        String scrollId,
        String keepAlive,
        int size) {
      return null;
    }

    @Override
    public ScrollResult structuredScroll(
        OperationContext opContext,
        List<String> entities,
        String input,
        Filter postFilters,
        List<SortCriterion> sortCriteria,
        String scrollId,
        String keepAlive,
        int size) {
      return null;
    }

    @Override
    public int maxResultSize() {
      return 0;
    }

    @Override
    public ExplainResponse explain(
        OperationContext opContext,
        String query,
        String documentId,
        String entityName,
        Filter postFilters,
        List<SortCriterion> sortCriteria,
        String scrollId,
        String keepAlive,
        int size,
        List<String> facets) {
      return null;
    }

    @Override
    public IndexConvention getIndexConvention() {
      return null;
    }
  }
}
