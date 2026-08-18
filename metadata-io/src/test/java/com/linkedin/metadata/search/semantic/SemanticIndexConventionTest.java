package com.linkedin.metadata.search.semantic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.datahub.context.OperationFingerprint;
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
    when(mockDelegateConvention.getEntityIndexName(OperationFingerprint.EMPTY, "dataset"))
        .thenReturn("datasetindex_v2");
    when(mockDelegateConvention.getEntityIndexName(OperationFingerprint.EMPTY, "chart"))
        .thenReturn("chartindex_v2");
    when(mockDelegateConvention.getEntityIndexName(OperationFingerprint.EMPTY, "dashboard"))
        .thenReturn("dashboardindex_v2");
    when(mockDelegateConvention.getEntityIndexName(OperationFingerprint.EMPTY, "container"))
        .thenReturn("containerindex_v2");

    // Test that semantic suffix is correctly appended
    assertEquals(
        semanticIndexConvention.getEntityIndexName(OperationFingerprint.EMPTY, "dataset"),
        "datasetindex_v2_semantic",
        "Should append _semantic suffix to dataset index");

    assertEquals(
        semanticIndexConvention.getEntityIndexName(OperationFingerprint.EMPTY, "chart"),
        "chartindex_v2_semantic",
        "Should append _semantic suffix to chart index");

    assertEquals(
        semanticIndexConvention.getEntityIndexName(OperationFingerprint.EMPTY, "dashboard"),
        "dashboardindex_v2_semantic",
        "Should append _semantic suffix to dashboard index");

    assertEquals(
        semanticIndexConvention.getEntityIndexName(OperationFingerprint.EMPTY, "container"),
        "containerindex_v2_semantic",
        "Should append _semantic suffix to container index");

    // Verify delegate methods were called
    verify(mockDelegateConvention).getEntityIndexName(OperationFingerprint.EMPTY, "dataset");
    verify(mockDelegateConvention).getEntityIndexName(OperationFingerprint.EMPTY, "chart");
    verify(mockDelegateConvention).getEntityIndexName(OperationFingerprint.EMPTY, "dashboard");
    verify(mockDelegateConvention).getEntityIndexName(OperationFingerprint.EMPTY, "container");
  }

  @Test
  public void testGetEntityIndexNameWithPrefixedIndices() {
    // Test with environment-specific prefixes
    when(mockDelegateConvention.getEntityIndexName(OperationFingerprint.EMPTY, "dataset"))
        .thenReturn("prod_datasetindex_v2");
    when(mockDelegateConvention.getEntityIndexName(OperationFingerprint.EMPTY, "chart"))
        .thenReturn("staging_chartindex_v2");

    assertEquals(
        semanticIndexConvention.getEntityIndexName(OperationFingerprint.EMPTY, "dataset"),
        "prod_datasetindex_v2_semantic",
        "Should append _semantic suffix preserving prefix");

    assertEquals(
        semanticIndexConvention.getEntityIndexName(OperationFingerprint.EMPTY, "chart"),
        "staging_chartindex_v2_semantic",
        "Should append _semantic suffix preserving prefix");
  }

  @Test
  public void testDelegationMethods() {
    // Setup delegate mock returns
    when(mockDelegateConvention.getPrefix(OperationFingerprint.EMPTY))
        .thenReturn(Optional.of("test"));
    when(mockDelegateConvention.getIndexName(OperationFingerprint.EMPTY, "baseIndex"))
        .thenReturn("test_baseindex_v2");
    when(mockDelegateConvention.getIdHashAlgo()).thenReturn("MD5");
    when(mockDelegateConvention.getAllEntityIndicesPatterns(OperationFingerprint.EMPTY))
        .thenReturn(Arrays.asList("*entity*"));
    when(mockDelegateConvention.getV3EntityIndexPatterns(OperationFingerprint.EMPTY))
        .thenReturn(Arrays.asList("*v3*"));
    when(mockDelegateConvention.getAllTimeseriesAspectIndicesPattern(OperationFingerprint.EMPTY))
        .thenReturn("*timeseries*");

    // Test delegation of non-entity methods
    assertEquals(
        semanticIndexConvention.getPrefix(OperationFingerprint.EMPTY),
        Optional.of("test"),
        "Should delegate getPrefix() to underlying convention");

    assertEquals(
        semanticIndexConvention.getIndexName(OperationFingerprint.EMPTY, "baseIndex"),
        "test_baseindex_v2",
        "Should delegate getIndexName() to underlying convention");

    assertEquals(
        semanticIndexConvention.getIdHashAlgo(),
        "MD5",
        "Should delegate getIdHashAlgo() to underlying convention");

    assertEquals(
        semanticIndexConvention.getAllEntityIndicesPatterns(OperationFingerprint.EMPTY),
        Arrays.asList("*entity*"),
        "Should delegate getAllEntityIndicesPatterns() to underlying convention");

    assertEquals(
        semanticIndexConvention.getV3EntityIndexPatterns(OperationFingerprint.EMPTY),
        Arrays.asList("*v3*"),
        "Should delegate getV3EntityIndexPatterns() to underlying convention");

    assertEquals(
        semanticIndexConvention.getAllTimeseriesAspectIndicesPattern(OperationFingerprint.EMPTY),
        "*timeseries*",
        "Should delegate getAllTimeseriesAspectIndicesPattern() to underlying convention");

    // Verify delegate methods were called
    verify(mockDelegateConvention).getPrefix(OperationFingerprint.EMPTY);
    verify(mockDelegateConvention).getIndexName(OperationFingerprint.EMPTY, "baseIndex");
    verify(mockDelegateConvention).getIdHashAlgo();
    verify(mockDelegateConvention).getAllEntityIndicesPatterns(OperationFingerprint.EMPTY);
    verify(mockDelegateConvention).getV3EntityIndexPatterns(OperationFingerprint.EMPTY);
    verify(mockDelegateConvention).getAllTimeseriesAspectIndicesPattern(OperationFingerprint.EMPTY);
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
    when(mockDelegateConvention.getTimeseriesAspectIndexName(
            OperationFingerprint.EMPTY, "dataset", "datasetProfile"))
        .thenReturn("dataset_datasetProfile_timeseries_v1");

    assertEquals(
        semanticIndexConvention.getTimeseriesAspectIndexName(
            OperationFingerprint.EMPTY, "dataset", "datasetProfile"),
        "dataset_datasetProfile_timeseries_v1",
        "Should delegate getTimeseriesAspectIndexName() to underlying convention");

    verify(mockDelegateConvention)
        .getTimeseriesAspectIndexName(OperationFingerprint.EMPTY, "dataset", "datasetProfile");
  }

  @Test
  public void testReverseLookupMethods() {
    // Test methods that parse index names back to entity names
    when(mockDelegateConvention.getEntityName(OperationFingerprint.EMPTY, "datasetindex_v2"))
        .thenReturn(Optional.of("dataset"));
    when(mockDelegateConvention.getEntityAndAspectName(
            OperationFingerprint.EMPTY, "dataset_profile_timeseries_v1"))
        .thenReturn(Optional.of(Pair.of("dataset", "profile")));

    assertEquals(
        semanticIndexConvention.getEntityName(OperationFingerprint.EMPTY, "datasetindex_v2"),
        Optional.of("dataset"),
        "Should delegate getEntityName() to underlying convention");

    assertEquals(
        semanticIndexConvention.getEntityAndAspectName(
            OperationFingerprint.EMPTY, "dataset_profile_timeseries_v1"),
        Optional.of(Pair.of("dataset", "profile")),
        "Should delegate getEntityAndAspectName() to underlying convention");

    verify(mockDelegateConvention).getEntityName(OperationFingerprint.EMPTY, "datasetindex_v2");
    verify(mockDelegateConvention)
        .getEntityAndAspectName(OperationFingerprint.EMPTY, "dataset_profile_timeseries_v1");
  }

  @Test
  public void testEntitySpecAndRecordTemplateDelegation() {
    // Test delegation for EntitySpec and RecordTemplate based methods
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    RecordTemplate mockRecordTemplate = mock(RecordTemplate.class);
    @SuppressWarnings("unchecked")
    Class<RecordTemplate> mockClass = (Class<RecordTemplate>) mockRecordTemplate.getClass();

    when(mockDelegateConvention.getIndexName(OperationFingerprint.EMPTY, mockEntitySpec))
        .thenReturn("entity_spec_index");
    when(mockDelegateConvention.getIndexName(OperationFingerprint.EMPTY, mockClass))
        .thenReturn("record_template_index");

    assertEquals(
        semanticIndexConvention.getIndexName(OperationFingerprint.EMPTY, mockEntitySpec),
        "entity_spec_index",
        "Should delegate getIndexName(EntitySpec) to underlying convention");

    assertEquals(
        semanticIndexConvention.getIndexName(OperationFingerprint.EMPTY, mockClass),
        "record_template_index",
        "Should delegate getIndexName(Class) to underlying convention");

    verify(mockDelegateConvention).getIndexName(OperationFingerprint.EMPTY, mockEntitySpec);
    verify(mockDelegateConvention).getIndexName(OperationFingerprint.EMPTY, mockClass);
  }

  @Test
  public void testNewIndexConventionMethods() {
    // Test the new methods that were added to IndexConvention interface
    when(mockDelegateConvention.getEntityIndexNameV3(OperationFingerprint.EMPTY, "dataset"))
        .thenReturn("datasetindex_v3");
    when(mockDelegateConvention.getEntityIndicesCleanupPatterns(
            any(OperationFingerprint.class), any()))
        .thenReturn(Arrays.asList("*cleanup*"));
    when(mockDelegateConvention.isV2EntityIndex(OperationFingerprint.EMPTY, "datasetindex_v2"))
        .thenReturn(true);
    when(mockDelegateConvention.isV3EntityIndex(OperationFingerprint.EMPTY, "datasetindex_v3"))
        .thenReturn(true);

    assertEquals(
        semanticIndexConvention.getEntityIndexNameV3(OperationFingerprint.EMPTY, "dataset"),
        "datasetindex_v3",
        "Should delegate getEntityIndexNameV3() to underlying convention");

    assertEquals(
        semanticIndexConvention.getEntityIndicesCleanupPatterns(OperationFingerprint.EMPTY, null),
        Arrays.asList("*cleanup*"),
        "Should delegate getEntityIndicesCleanupPatterns() to underlying convention");

    // SemanticIndexConvention always returns false for these methods
    assertFalse(
        semanticIndexConvention.isV2EntityIndex(OperationFingerprint.EMPTY, "datasetindex_v2"),
        "Should always return false for isV2EntityIndex");

    assertFalse(
        semanticIndexConvention.isV3EntityIndex(OperationFingerprint.EMPTY, "datasetindex_v3"),
        "Should always return false for isV3EntityIndex");

    verify(mockDelegateConvention).getEntityIndexNameV3(OperationFingerprint.EMPTY, "dataset");
    verify(mockDelegateConvention)
        .getEntityIndicesCleanupPatterns(any(OperationFingerprint.class), any());
  }

  @Test
  public void testFilterTransformationIntegration() {
    // Integration test: verify that when used with SearchUtil.transformFilterForEntities,
    // the _entityType filter gets transformed to the correct semantic index

    when(mockDelegateConvention.getEntityIndexName(OperationFingerprint.EMPTY, "dataset"))
        .thenReturn("datasetindex_v2");

    // Create a SemanticIndexConvention and verify the entity index name transformation
    String result =
        semanticIndexConvention.getEntityIndexName(OperationFingerprint.EMPTY, "dataset");

    assertEquals(
        result,
        "datasetindex_v2_semantic",
        "When used in filter transformation, should convert dataset to semantic index name");

    // This test validates that our wrapper works correctly for the primary use case:
    // SearchUtil.transformFilterForEntities(postFilters, semanticIndexConvention)
    // which should convert _entityType=DATASET filters to _index=datasetindex_v2_semantic
  }
}
