package com.linkedin.metadata.search.elasticsearch.index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import io.datahubproject.test.search.SearchTestUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchEngineStructuredPropertyMappingLookupTest {

  private SearchClientShim<?> searchClient;
  private IndexConvention indexConvention;
  private SearchEngineStructuredPropertyMappingLookup lookup;

  @BeforeMethod
  public void setup() {
    searchClient = mock(SearchClientShim.class);
    indexConvention = mock(IndexConvention.class);
    lookup = new SearchEngineStructuredPropertyMappingLookup(searchClient, indexConvention);
  }

  @Test
  public void testFindsStructuredPropertyFieldAcrossEntityMappings() throws IOException {
    when(indexConvention.getAllEntityIndicesPatterns(OperationFingerprint.EMPTY))
        .thenReturn(List.of("*index_v2", "*index_v3"));
    stubMappings(
        Map.of(
            "properties",
            Map.of(
                "structuredProperties",
                Map.of("properties", Map.of("certification_status", Map.of("type", "keyword"))))));

    assertTrue(lookup.fieldExists(OperationFingerprint.EMPTY, "certification_status"));
    assertFalse(lookup.fieldExists(OperationFingerprint.EMPTY, "other_status"));
  }

  @Test
  public void testFindsNestedVersionedField() throws IOException {
    when(indexConvention.getAllEntityIndicesPatterns(OperationFingerprint.EMPTY))
        .thenReturn(List.of("*index_v3"));
    Map<String, Object> versionMapping =
        Map.of("properties", Map.of("string", Map.of("type", "keyword")));
    Map<String, Object> propertyMapping =
        Map.of("properties", Map.of("00000000000001", versionMapping));
    Map<String, Object> versionedMapping =
        Map.of("properties", Map.of("certification_status", propertyMapping));
    stubMappings(
        Map.of(
            "properties",
            Map.of(
                "structuredProperties",
                Map.of("properties", Map.of("_versioned", versionedMapping)))));

    assertTrue(
        lookup.fieldExists(
            OperationFingerprint.EMPTY, "_versioned.certification_status.00000000000001.string"));
  }

  @Test
  public void testNoConfiguredEntityIndicesSkipsBackendCall() throws IOException {
    when(indexConvention.getAllEntityIndicesPatterns(OperationFingerprint.EMPTY))
        .thenReturn(List.of());

    assertFalse(lookup.fieldExists(OperationFingerprint.EMPTY, "certification_status"));
    verifyNoInteractions(searchClient);
  }

  @Test
  public void testSplitClustersQueryEachFamilyOnItsClient() throws IOException {
    SearchClientShim<?> v2 = mock(SearchClientShim.class);
    SearchClientShim<?> v3 = mock(SearchClientShim.class);
    IndexConvention convention =
        IndexConventionImpl.noPrefix(
            "MD5", SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION);
    SearchClusterAccess access = component -> component == SearchComponent.SEARCH_V3 ? v3 : v2;
    lookup = new SearchEngineStructuredPropertyMappingLookup(convention, access);

    stubMappings(v2, emptyStructuredProperties());
    stubMappings(
        v3,
        Map.of(
            "properties",
            Map.of(
                "structuredProperties",
                Map.of("properties", Map.of("certification_status", Map.of("type", "keyword"))))));

    assertTrue(lookup.fieldExists(OperationFingerprint.EMPTY, "certification_status"));
    verify(v2).getIndexMapping(any(), any(), any());
    verify(v3).getIndexMapping(any(), any(), any());
  }

  @Test
  public void testUnrecognizedPatternIsAnError() {
    IndexConvention convention = mock(IndexConvention.class);
    lookup =
        new SearchEngineStructuredPropertyMappingLookup(
            convention, SearchClusterAccess.fixed(searchClient));
    when(convention.getAllEntityIndicesPatterns(OperationFingerprint.EMPTY))
        .thenReturn(List.of("not_an_entity_index"));

    assertThrows(
        IllegalArgumentException.class,
        () -> lookup.fieldExists(OperationFingerprint.EMPTY, "certification_status"));
  }

  private static Map<String, Object> emptyStructuredProperties() {
    return Map.of("properties", Map.of("structuredProperties", Map.of("properties", Map.of())));
  }

  private void stubMappings(Map<String, Object> source) throws IOException {
    stubMappings(searchClient, source);
  }

  private void stubMappings(SearchClientShim<?> client, Map<String, Object> source)
      throws IOException {
    GetMappingsResponse response = mock(GetMappingsResponse.class);
    MappingMetadata metadata = mock(MappingMetadata.class);
    when(metadata.getSourceAsMap()).thenReturn(source);
    when(response.mappings()).thenReturn(Map.of("entity-index", metadata));
    when(client.getIndexMapping(any(), any(), any())).thenReturn(response);
  }
}
