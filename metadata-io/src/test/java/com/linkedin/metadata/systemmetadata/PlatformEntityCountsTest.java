package com.linkedin.metadata.systemmetadata;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PlatformEntityCountsTest {

  private SearchClientShim<?> searchClient;
  private EntityRegistry entityRegistry;
  private EntitySpec datasetSpec;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    searchClient = mock(SearchClientShim.class);
    entityRegistry = mock(EntityRegistry.class);
    datasetSpec = mock(EntitySpec.class);
    when(datasetSpec.hasAspect("dataPlatformInstance")).thenReturn(true);
    when(datasetSpec.getSearchGroup()).thenReturn("primary");
    when(entityRegistry.getEntitySpec("dataset")).thenReturn(datasetSpec);
    when(entityRegistry.getEntitySpecs()).thenReturn(Map.of("dataset", datasetSpec));
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testNormalizePlatform_shortId() {
    assertEquals(PlatformEntityCounts.normalizePlatform("snowflake"), "snowflake");
  }

  @Test
  public void testNormalizePlatform_urn() {
    assertEquals(
        PlatformEntityCounts.normalizePlatform("urn:li:dataPlatform:snowflake"), "snowflake");
  }

  @Test
  public void testNormalizePlatform_malformedUrnFallsBackToLastSegment() {
    assertEquals(
        PlatformEntityCounts.normalizePlatform("urn:li:dataPlatform:snow flake"), "snow flake");
  }

  @Test
  public void testNormalizePlatform_trailingColonReturnsRaw() {
    assertEquals(
        PlatformEntityCounts.normalizePlatform("urn:li:dataPlatform:"), "urn:li:dataPlatform:");
  }

  @Test
  public void testNormalizePlatform_missing() {
    assertEquals(
        PlatformEntityCounts.normalizePlatform(PlatformEntityCounts.NO_PLATFORM), "NO_PLATFORM");
    assertEquals(PlatformEntityCounts.normalizePlatform(""), "NO_PLATFORM");
    assertEquals(PlatformEntityCounts.normalizePlatform("   "), "NO_PLATFORM");
  }

  @Test
  public void v2QueriesPerEntityIndexAndPlatformKeyword() throws Exception {
    SearchResponse response = responseWithSnowflakeBucket(7, 1);
    when(searchClient.search(any(), any(), any())).thenReturn(response);

    PlatformEntityCountResult result =
        v2Counts().getCountsByPlatform(opContext, List.of("dataset"));

    SearchRequest request = capturedSearch();
    String expectedIndex =
        opContext.getSearchContext().getIndexConvention().getEntityIndexName(opContext, "dataset");
    assertEquals(request.indices()[0], expectedIndex);
    assertTrue(request.source().query() instanceof MatchAllQueryBuilder);
    assertEquals(platformAggField(request), "platform.keyword");
    assertEquals(result.getCounts().size(), 1);
    assertEquals(result.getCounts().get(0).getPlatform(), "snowflake");
    assertEquals(result.getCounts().get(0).getActiveCount(), 7);
    assertEquals(result.getCounts().get(0).getSoftDeletedCount(), 1);
  }

  @Test
  public void v3QueriesSearchGroupIndexAndPlatformField() throws Exception {
    SearchResponse response = responseWithSnowflakeBucket(4, 0);
    when(searchClient.search(any(), any(), any())).thenReturn(response);

    v3Counts().getCountsByPlatform(opContext, List.of("dataset"));

    SearchRequest request = capturedSearch();
    String expectedIndex =
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(opContext, "primary");
    assertEquals(request.indices()[0], expectedIndex);
    assertTrue(request.source().query() instanceof TermQueryBuilder);
    TermQueryBuilder term = (TermQueryBuilder) request.source().query();
    assertEquals(term.fieldName(), "_entityType");
    assertEquals(term.value(), "dataset");
    assertEquals(platformAggField(request), "platform");
  }

  @Test
  public void prefersV2WhenBothEnabled() throws Exception {
    SearchResponse response = responseWithSnowflakeBucket(1, 0);
    when(searchClient.search(any(), any(), any())).thenReturn(response);

    bothEnabledCounts().getCountsByPlatform(opContext, List.of("dataset"));

    SearchRequest request = capturedSearch();
    assertEquals(
        request.indices()[0],
        opContext.getSearchContext().getIndexConvention().getEntityIndexName(opContext, "dataset"));
    assertEquals(platformAggField(request), "platform.keyword");
  }

  @Test
  public void neitherIndexEnabledReturnsEmptyWithoutSearch() throws Exception {
    EntityIndexConfiguration config =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .build();
    PlatformEntityCounts counts =
        new PlatformEntityCounts(searchClient, entityRegistry, config, 50);

    PlatformEntityCountResult result = counts.getCountsByPlatform(opContext, List.of("dataset"));

    assertTrue(result.getCounts().isEmpty());
    verify(searchClient, never()).search(any(), any(), any());
  }

  @Test
  public void missingIndexReturnsEmpty() throws Exception {
    when(searchClient.search(any(), any(), any()))
        .thenThrow(new OpenSearchStatusException("missing", RestStatus.NOT_FOUND));

    PlatformEntityCountResult result =
        v2Counts().getCountsByPlatform(opContext, List.of("dataset"));

    assertTrue(result.getCounts().isEmpty());
  }

  @Test
  public void ioExceptionFailsRequest() throws Exception {
    when(searchClient.search(any(), any(), any())).thenThrow(new IOException("search failed"));

    assertThrows(
        RuntimeException.class,
        () -> v2Counts().getCountsByPlatform(opContext, List.of("dataset")));
  }

  @Test
  public void nonNotFoundOpenSearchExceptionFailsRequest() throws Exception {
    when(searchClient.search(any(), any(), any()))
        .thenThrow(new OpenSearchStatusException("unavailable", RestStatus.INTERNAL_SERVER_ERROR));

    assertThrows(
        RuntimeException.class,
        () -> v2Counts().getCountsByPlatform(opContext, List.of("dataset")));
  }

  @Test
  public void nullAggregationsFailsRequest() throws Exception {
    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(null);
    when(searchClient.search(any(), any(), any())).thenReturn(response);

    assertThrows(
        RuntimeException.class,
        () -> v2Counts().getCountsByPlatform(opContext, List.of("dataset")));
  }

  @Test
  public void missingByPlatformAggregationFailsRequest() throws Exception {
    Aggregations aggregations = mock(Aggregations.class);
    when(aggregations.get("by_platform")).thenReturn(null);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(aggregations);
    when(searchClient.search(any(), any(), any())).thenReturn(response);

    assertThrows(
        RuntimeException.class,
        () -> v2Counts().getCountsByPlatform(opContext, List.of("dataset")));
  }

  @Test
  public void truncatedTermsAggregationFailsRequest() throws Exception {
    Terms terms = mock(Terms.class);
    when(terms.getSumOfOtherDocCounts()).thenReturn(12L);
    Aggregations aggregations = mock(Aggregations.class);
    when(aggregations.get("by_platform")).thenReturn(terms);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(aggregations);
    when(searchClient.search(any(), any(), any())).thenReturn(response);

    assertThrows(
        IllegalStateException.class,
        () -> v2Counts().getCountsByPlatform(opContext, List.of("dataset")));
  }

  @Test
  public void skipsTypesWithoutPlatformSearchField() throws Exception {
    when(datasetSpec.hasAspect("dataPlatformInstance")).thenReturn(false);
    when(datasetSpec.getSearchableFieldSpecs()).thenReturn(List.of());

    PlatformEntityCountResult result =
        v2Counts().getCountsByPlatform(opContext, List.of("dataset"));

    assertTrue(result.getCounts().isEmpty());
    verify(searchClient, never()).search(any(), any(), any());
  }

  private PlatformEntityCounts v2Counts() {
    return new PlatformEntityCounts(searchClient, entityRegistry, indexConfig(true, false), 50);
  }

  private PlatformEntityCounts v3Counts() {
    return new PlatformEntityCounts(searchClient, entityRegistry, indexConfig(false, true), 50);
  }

  private PlatformEntityCounts bothEnabledCounts() {
    return new PlatformEntityCounts(searchClient, entityRegistry, indexConfig(true, true), 50);
  }

  private static EntityIndexConfiguration indexConfig(boolean v2, boolean v3) {
    return EntityIndexConfiguration.builder()
        .v2(EntityIndexVersionConfiguration.builder().enabled(v2).build())
        .v3(EntityIndexVersionConfiguration.builder().enabled(v3).build())
        .build();
  }

  private SearchRequest capturedSearch() throws IOException {
    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(searchClient).search(eq(opContext), captor.capture(), any());
    return captor.getValue();
  }

  private static String platformAggField(SearchRequest request) {
    return ((TermsAggregationBuilder)
            request.source().aggregations().getAggregatorFactories().iterator().next())
        .field();
  }

  private static SearchResponse responseWithSnowflakeBucket(long active, long softDeleted) {
    Filter activeFilter = mock(Filter.class);
    when(activeFilter.getDocCount()).thenReturn(active);
    Filter softFilter = mock(Filter.class);
    when(softFilter.getDocCount()).thenReturn(softDeleted);
    Aggregations bucketAggs = mock(Aggregations.class);
    when(bucketAggs.get("active")).thenReturn(activeFilter);
    when(bucketAggs.get("soft_deleted")).thenReturn(softFilter);

    Terms.Bucket bucket = mock(Terms.Bucket.class);
    when(bucket.getKeyAsString()).thenReturn("snowflake");
    when(bucket.getAggregations()).thenReturn(bucketAggs);

    Terms terms = mock(Terms.class);
    when(terms.getSumOfOtherDocCounts()).thenReturn(0L);
    doReturn(List.of(bucket)).when(terms).getBuckets();

    Aggregations aggregations = mock(Aggregations.class);
    when(aggregations.get("by_platform")).thenReturn(terms);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(aggregations);
    return response;
  }
}
