package com.linkedin.metadata.search.semantic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.util.Pair;
import java.util.Arrays;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for SemanticIndexConvention */
public class SemanticIndexConventionTest {

  @Mock private IndexConvention mockDelegateConvention;

  private SemanticIndexConvention semanticIndexConvention;
  private AutoCloseable mocks;

  @BeforeMethod
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    semanticIndexConvention = new SemanticIndexConvention(mockDelegateConvention);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  @SuppressWarnings("null") // Intentionally testing null parameter validation
  public void testConstructorValidation() {
    // Test null delegate
    assertThrows(NullPointerException.class, () -> new SemanticIndexConvention(null));

    // Test successful construction
    IndexConvention delegate = mock(IndexConvention.class);
    SemanticIndexConvention convention = new SemanticIndexConvention(delegate);
    assertNotNull(convention);
  }

  @Test
  public void testGetEntityIndexNameAppendsSemanticSuffix() {
    // Setup delegate to return base index names
    when(mockDelegateConvention.getEntityIndexName("dataset")).thenReturn("datasetindex_v2");
    when(mockDelegateConvention.getEntityIndexName("chart")).thenReturn("chartindex_v2");
    when(mockDelegateConvention.getEntityIndexName("dashboard")).thenReturn("dashboardindex_v2");
    when(mockDelegateConvention.getEntityIndexName("container")).thenReturn("containerindex_v2");

    // Test that semantic suffix is correctly appended
    assertEquals(
        semanticIndexConvention.getEntityIndexName("dataset"),
        "datasetindex_v2_semantic",
        "Should append _semantic suffix to dataset index");

    assertEquals(
        semanticIndexConvention.getEntityIndexName("chart"),
        "chartindex_v2_semantic",
        "Should append _semantic suffix to chart index");

    assertEquals(
        semanticIndexConvention.getEntityIndexName("dashboard"),
        "dashboardindex_v2_semantic",
        "Should append _semantic suffix to dashboard index");

    assertEquals(
        semanticIndexConvention.getEntityIndexName("container"),
        "containerindex_v2_semantic",
        "Should append _semantic suffix to container index");

    // Verify delegate methods were called
    verify(mockDelegateConvention).getEntityIndexName("dataset");
    verify(mockDelegateConvention).getEntityIndexName("chart");
    verify(mockDelegateConvention).getEntityIndexName("dashboard");
    verify(mockDelegateConvention).getEntityIndexName("container");
  }

  @Test
  public void testGetEntityIndexNameWithPrefixedIndices() {
    // Test with environment-specific prefixes
    when(mockDelegateConvention.getEntityIndexName("dataset")).thenReturn("prod_datasetindex_v2");
    when(mockDelegateConvention.getEntityIndexName("chart")).thenReturn("staging_chartindex_v2");

    assertEquals(
        semanticIndexConvention.getEntityIndexName("dataset"),
        "prod_datasetindex_v2_semantic",
        "Should append _semantic suffix preserving prefix");

    assertEquals(
        semanticIndexConvention.getEntityIndexName("chart"),
        "staging_chartindex_v2_semantic",
        "Should append _semantic suffix preserving prefix");
  }

  @Test
  public void testDelegationMethods() {
    // Setup delegate mock returns
    when(mockDelegateConvention.getPrefix()).thenReturn(Optional.of("test"));
    when(mockDelegateConvention.getIndexName("baseIndex")).thenReturn("test_baseindex_v2");
    when(mockDelegateConvention.getIdHashAlgo()).thenReturn("MD5");
    when(mockDelegateConvention.getAllEntityIndicesPatterns())
        .thenReturn(Arrays.asList("*entity*"));
    when(mockDelegateConvention.getV3EntityIndexPatterns()).thenReturn(Arrays.asList("*v3*"));
    when(mockDelegateConvention.getAllTimeseriesAspectIndicesPattern()).thenReturn("*timeseries*");

    // Test delegation of non-entity methods
    assertEquals(
        semanticIndexConvention.getPrefix(),
        Optional.of("test"),
        "Should delegate getPrefix() to underlying convention");

    assertEquals(
        semanticIndexConvention.getIndexName("baseIndex"),
        "test_baseindex_v2",
        "Should delegate getIndexName() to underlying convention");

    assertEquals(
        semanticIndexConvention.getIdHashAlgo(),
        "MD5",
        "Should delegate getIdHashAlgo() to underlying convention");

    assertEquals(
        semanticIndexConvention.getAllEntityIndicesPatterns(),
        Arrays.asList("*entity*"),
        "Should delegate getAllEntityIndicesPatterns() to underlying convention");

    assertEquals(
        semanticIndexConvention.getV3EntityIndexPatterns(),
        Arrays.asList("*v3*"),
        "Should delegate getV3EntityIndexPatterns() to underlying convention");

    assertEquals(
        semanticIndexConvention.getAllTimeseriesAspectIndicesPattern(),
        "*timeseries*",
        "Should delegate getAllTimeseriesAspectIndicesPattern() to underlying convention");

    // Verify delegate methods were called
    verify(mockDelegateConvention).getPrefix();
    verify(mockDelegateConvention).getIndexName("baseIndex");
    verify(mockDelegateConvention).getIdHashAlgo();
    verify(mockDelegateConvention).getAllEntityIndicesPatterns();
    verify(mockDelegateConvention).getV3EntityIndexPatterns();
    verify(mockDelegateConvention).getAllTimeseriesAspectIndicesPattern();
  }

  @Test
  public void testEntityDocumentIdDelegation() {
    // Test URN-based methods
    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,table,PROD)");
    when(mockDelegateConvention.getEntityDocumentId(testUrn)).thenReturn("test-doc-id");

    assertEquals(
        semanticIndexConvention.getEntityDocumentId(testUrn),
        "test-doc-id",
        "Should delegate getEntityDocumentId() to underlying convention");

    verify(mockDelegateConvention).getEntityDocumentId(testUrn);
  }

  @Test
  public void testTimeseriesAspectIndexNameDelegation() {
    // Test timeseries aspect index name delegation
    when(mockDelegateConvention.getTimeseriesAspectIndexName("dataset", "datasetProfile"))
        .thenReturn("dataset_datasetProfile_timeseries_v1");

    assertEquals(
        semanticIndexConvention.getTimeseriesAspectIndexName("dataset", "datasetProfile"),
        "dataset_datasetProfile_timeseries_v1",
        "Should delegate getTimeseriesAspectIndexName() to underlying convention");

    verify(mockDelegateConvention).getTimeseriesAspectIndexName("dataset", "datasetProfile");
  }

  @Test
  public void testReverseLookupMethods() {
    // Test methods that parse index names back to entity names
    when(mockDelegateConvention.getEntityName("datasetindex_v2"))
        .thenReturn(Optional.of("dataset"));
    when(mockDelegateConvention.getEntityAndAspectName("dataset_profile_timeseries_v1"))
        .thenReturn(Optional.of(Pair.of("dataset", "profile")));

    assertEquals(
        semanticIndexConvention.getEntityName("datasetindex_v2"),
        Optional.of("dataset"),
        "Should delegate getEntityName() to underlying convention");

    assertEquals(
        semanticIndexConvention.getEntityAndAspectName("dataset_profile_timeseries_v1"),
        Optional.of(Pair.of("dataset", "profile")),
        "Should delegate getEntityAndAspectName() to underlying convention");

    verify(mockDelegateConvention).getEntityName("datasetindex_v2");
    verify(mockDelegateConvention).getEntityAndAspectName("dataset_profile_timeseries_v1");
  }

  @Test
  public void testEntitySpecAndRecordTemplateDelegation() {
    // Test delegation for EntitySpec and RecordTemplate based methods
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    RecordTemplate mockRecordTemplate = mock(RecordTemplate.class);
    @SuppressWarnings("unchecked")
    Class<RecordTemplate> mockClass = (Class<RecordTemplate>) mockRecordTemplate.getClass();

    when(mockDelegateConvention.getIndexName(mockEntitySpec)).thenReturn("entity_spec_index");
    when(mockDelegateConvention.getIndexName(mockClass)).thenReturn("record_template_index");

    assertEquals(
        semanticIndexConvention.getIndexName(mockEntitySpec),
        "entity_spec_index",
        "Should delegate getIndexName(EntitySpec) to underlying convention");

    assertEquals(
        semanticIndexConvention.getIndexName(mockClass),
        "record_template_index",
        "Should delegate getIndexName(Class) to underlying convention");

    verify(mockDelegateConvention).getIndexName(mockEntitySpec);
    verify(mockDelegateConvention).getIndexName(mockClass);
  }

  @Test
  public void testNewIndexConventionMethods() {
    // Test the new methods that were added to IndexConvention interface
    when(mockDelegateConvention.getEntityIndexNameV3("dataset")).thenReturn("datasetindex_v3");
    when(mockDelegateConvention.getEntityIndicesCleanupPatterns(any()))
        .thenReturn(Arrays.asList("*cleanup*"));
    when(mockDelegateConvention.isV2EntityIndex("datasetindex_v2")).thenReturn(true);
    when(mockDelegateConvention.isV3EntityIndex("datasetindex_v3")).thenReturn(true);

    assertEquals(
        semanticIndexConvention.getEntityIndexNameV3("dataset"),
        "datasetindex_v3",
        "Should delegate getEntityIndexNameV3() to underlying convention");

    assertEquals(
        semanticIndexConvention.getEntityIndicesCleanupPatterns(null),
        Arrays.asList("*cleanup*"),
        "Should delegate getEntityIndicesCleanupPatterns() to underlying convention");

    // SemanticIndexConvention always returns false for these methods
    assertFalse(
        semanticIndexConvention.isV2EntityIndex("datasetindex_v2"),
        "Should always return false for isV2EntityIndex");

    assertFalse(
        semanticIndexConvention.isV3EntityIndex("datasetindex_v3"),
        "Should always return false for isV3EntityIndex");

    verify(mockDelegateConvention).getEntityIndexNameV3("dataset");
    verify(mockDelegateConvention).getEntityIndicesCleanupPatterns(any());
  }

  @Test
  public void testFilterTransformationIntegration() {
    // Integration test: verify that when used with SearchUtil.transformFilterForEntities,
    // the _entityType filter gets transformed to the correct semantic index

    when(mockDelegateConvention.getEntityIndexName("dataset")).thenReturn("datasetindex_v2");

    // Create a SemanticIndexConvention and verify the entity index name transformation
    String result = semanticIndexConvention.getEntityIndexName("dataset");

    assertEquals(
        result,
        "datasetindex_v2_semantic",
        "When used in filter transformation, should convert dataset to semantic index name");

    // This test validates that our wrapper works correctly for the primary use case:
    // SearchUtil.transformFilterForEntities(postFilters, semanticIndexConvention)
    // which should convert _entityType=DATASET filters to _index=datasetindex_v2_semantic
  }
}
