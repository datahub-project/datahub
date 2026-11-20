package com.linkedin.metadata.utils.elasticsearch;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import org.testng.annotations.Test;

/**
 * Covers the index-family matchers used to decide which cluster owns an index. The plain type
 * checks only match aliases, so these must also recognize zero-downtime-update backing indices.
 */
public class IndexConventionBackingTypeTest {

  private final IndexConvention convention =
      IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration());

  @Test
  public void testAliasesMatchTheirOwnFamily() {
    assertTrue(convention.isV2EntityIndexOrBackingType("datasetindex_v2"));
    assertTrue(convention.isV3EntityIndexOrBackingType("datasetindex_v3"));
    assertFalse(convention.isV3EntityIndexOrBackingType("datasetindex_v2"));
    assertFalse(convention.isV2EntityIndexOrBackingType("datasetindex_v3"));
  }

  @Test
  public void testZeroDowntimeBackingIndicesMatchTheirFamily() {
    // The plain type check misses these, which would route an in-progress rebuild to the wrong
    // cluster.
    assertTrue(convention.isV3EntityIndexOrBackingType("datasetindex_v3_1712345678"));
    assertTrue(convention.isV2EntityIndexOrBackingType("datasetindex_v2_1712345678"));
    assertFalse(convention.isV3EntityIndexType("datasetindex_v3_1712345678"));
  }

  @Test
  public void testSemanticBackingIsNotAV2Rebuild() {
    assertFalse(convention.isV2EntityIndexOrBackingType("datasetindex_v2_semantic"));
    assertTrue(convention.isSemanticEntityIndexType("datasetindex_v2_semantic"));
    assertTrue(convention.isSemanticEntityIndexOrBackingType("datasetindex_v2_semantic"));
    assertTrue(
        convention.isSemanticEntityIndexOrBackingType("datasetindex_v2_semantic_1712345678"));
    assertFalse(convention.isV2EntityIndexOrBackingType("datasetindex_v2_semantic_1712345678"));
  }

  @Test
  public void testIncrementalRebuildNamesMatchTheirFamily() {
    assertTrue(convention.isV2EntityIndexOrBackingType("datasetindex_v2_next_123"));
    assertTrue(convention.isV3EntityIndexOrBackingType("datasetindex_v3_next_123"));
    assertTrue(convention.isSemanticEntityIndexOrBackingType("datasetindex_v2_semantic_next_123"));
    assertTrue(convention.isV2EntityIndexOrBackingType("datasetindex_v2_0_13_1-0_1712345678"));
    assertFalse(convention.isV2EntityIndexOrBackingType("datasetindex_v2_draft"));
  }

  @Test
  public void testNonNumericSuffixIsNotABackingIndex() {
    assertFalse(convention.isV3EntityIndexOrBackingType("datasetindex_v3_draft"));
  }

  @Test
  public void testPrefixedNamesStillMatch() {
    assertTrue(convention.isV3EntityIndexOrBackingType("prod_datasetindex_v3"));
    assertTrue(convention.isV3EntityIndexOrBackingType("prod_datasetindex_v3_1712345678"));
  }

  @Test
  public void testWildcardPatternsMatchTheirFamily() {
    assertTrue(IndexConvention.matchesV3EntityIndexFamily("*index_v3*"));
    assertTrue(IndexConvention.matchesV2EntityIndexFamily("*index_v2*"));
    assertTrue(IndexConvention.matchesSemanticEntityIndexFamily("*index_v2_semantic*"));
    assertTrue(
        IndexConvention.matchesSemanticEntityIndexFamily("datasetindex_v2_semantic_1712345678"));
    assertFalse(IndexConvention.matchesV2EntityIndexFamily("*index_v2_semantic*"));
    assertFalse(IndexConvention.matchesV2EntityIndexFamily("datasetindex_v2_semantic_1712345678"));
    assertTrue(IndexConvention.matchesV2EntityIndexFamily("datasetindex_v2_next_123"));
    assertFalse(IndexConvention.matchesV3EntityIndexFamily("datasetindex_v2"));
    assertTrue(IndexConvention.matchesTimeseriesAspectIndexFamily("*aspect_v1"));
    assertTrue(
        IndexConvention.matchesTimeseriesAspectIndexFamily(
            "dataset_datasetusagestatisticsaspect_v1"));
    assertFalse(IndexConvention.matchesTimeseriesAspectIndexFamily("aspect_v1"));
  }
}
