package com.linkedin.metadata.utils.elasticsearch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Optional;
import org.testng.annotations.Test;

public class EntityIndexVersionTest {

  @Test
  public void testAliasNames() {
    assertEquals(
        EntityIndexVersion.fromIndexName("datasetindex_v2"), Optional.of(EntityIndexVersion.V2));
    assertEquals(
        EntityIndexVersion.fromIndexName("primaryindex_v3"), Optional.of(EntityIndexVersion.V3));
    assertEquals(
        EntityIndexVersion.fromIndexName("acme_datasetindex_v3"),
        Optional.of(EntityIndexVersion.V3));
  }

  @Test
  public void testPhysicalNames() {
    assertEquals(
        EntityIndexVersion.fromIndexName("datasetindex_v2_1683649932260"),
        Optional.of(EntityIndexVersion.V2));
    assertEquals(
        EntityIndexVersion.fromIndexName("datasetindex_v3_0_13_1_0_1704067200000"),
        Optional.of(EntityIndexVersion.V3));
    assertEquals(
        EntityIndexVersion.fromIndexName("acme_primaryindex_v3_1_2_3-4_1000"),
        Optional.of(EntityIndexVersion.V3));
  }

  @Test
  public void testSemanticIndicesAreNotEntityIndices() {
    assertEquals(EntityIndexVersion.fromIndexName("datasetindex_v2_semantic"), Optional.empty());
    assertEquals(
        EntityIndexVersion.fromIndexName("datasetindex_v2_semantic_0_13_1_0_1704067200000"),
        Optional.empty());
  }

  @Test
  public void testNonEntityNames() {
    assertEquals(EntityIndexVersion.fromIndexName(null), Optional.empty());
    assertEquals(EntityIndexVersion.fromIndexName(""), Optional.empty());
    assertEquals(EntityIndexVersion.fromIndexName("index_v3"), Optional.empty());
    assertEquals(EntityIndexVersion.fromIndexName("dataset_v3"), Optional.empty());
    assertEquals(EntityIndexVersion.fromIndexName("datasetindex_v3_extra"), Optional.empty());
    assertEquals(
        EntityIndexVersion.fromIndexName("dataset_datasetusagestatisticsaspect_v1"),
        Optional.empty());
    assertEquals(EntityIndexVersion.fromIndexName("graph_service_v1"), Optional.empty());
    assertEquals(EntityIndexVersion.fromIndexName("system_metadata_service_v1"), Optional.empty());
  }

  @Test
  public void testLaterTokenWins() {
    assertEquals(
        EntityIndexVersion.fromIndexName("index_v2_thingindex_v3"),
        Optional.of(EntityIndexVersion.V3));
  }

  @Test
  public void testMatchesAndKeyOf() {
    assertTrue(EntityIndexVersion.V3.matches("datasetindex_v3_0_13_1_0_1704067200000"));
    assertFalse(EntityIndexVersion.V2.matches("datasetindex_v3_0_13_1_0_1704067200000"));
    assertTrue(EntityIndexVersion.V3.isAliasName("datasetindex_v3"));
    assertFalse(EntityIndexVersion.V3.isAliasName("datasetindex_v3_0_13_1_0_1704067200000"));
    assertFalse(EntityIndexVersion.V3.isAliasName("index_v3"));
    assertEquals(
        EntityIndexVersion.V3.keyOf("acme_datasetindex_v3_0_13_1_0_1704067200000"),
        Optional.of("acme_dataset"));
    assertEquals(EntityIndexVersion.V3.keyOf("primaryindex_v3"), Optional.of("primary"));
    assertEquals(EntityIndexVersion.V3.keyOf("datasetindex_v2"), Optional.empty());
  }
}
