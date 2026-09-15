package com.linkedin.metadata.search.elasticsearch;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.opensearch.action.search.SearchRequest;
import org.testng.annotations.Test;

public class SearchClientsTest {

  @Test
  public void testMixedFamiliesAreNotSilentlyRoutedToKeyword() {
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClientShim<?> v3 = mock(SearchClientShim.class);
    SearchClusterAccess access = component -> component == SearchComponent.SEARCH_V3 ? v3 : v2;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(
                SearchContext.builder()
                    .indexConvention(
                        IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration()))
                    .searchClusterAccess(access)
                    .build()),
            access);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            SearchClients.forEntityIndices(
                opContext, (EntityIndexConfiguration) null, "datasetindex_v2", "datasetindex_v3"));
  }

  @Test
  public void testEmptyIndicesUseKeywordComponent() {
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClientShim<?> v3 = mock(SearchClientShim.class);
    SearchClusterAccess access = component -> component == SearchComponent.SEARCH_V3 ? v3 : v2;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(), access);

    assertSame(SearchClients.forEntityIndices(opContext, (EntityIndexConfiguration) null), v2);
  }

  @Test
  public void testUnrecognizedIndexIsAnError() {
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClusterAccess access = component -> v2;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(), access);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            SearchClients.forEntityIndices(
                opContext, (EntityIndexConfiguration) null, "graph_service_v1"));
  }

  @Test
  public void testSearchRequestRoutesFromItsIndices() {
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClientShim<?> v3 = mock(SearchClientShim.class);
    SearchClusterAccess access = component -> component == SearchComponent.SEARCH_V3 ? v3 : v2;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(
                SearchContext.builder()
                    .indexConvention(
                        IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration()))
                    .searchClusterAccess(access)
                    .build()),
            access);

    assertSame(SearchClients.forEntityIndices(opContext, new SearchRequest("datasetindex_v3")), v3);
    assertSame(SearchClients.forEntityIndices(opContext, new SearchRequest("datasetindex_v2")), v2);
  }
}
