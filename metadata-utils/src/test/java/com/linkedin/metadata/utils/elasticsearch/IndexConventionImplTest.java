package com.linkedin.metadata.utils.elasticsearch;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.util.Pair;
import java.net.URLEncoder;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.Test;

public class IndexConventionImplTest {

  @Test
  public void testIndexConventionNoPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConventionNoPrefix =
        IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);
    String entityName = "dataset";
    String expectedIndexName = "datasetindex_v2";
    assertEquals(indexConventionNoPrefix.getEntityIndexName(entityName), expectedIndexName);
    assertEquals(indexConventionNoPrefix.getPrefix(), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityName(expectedIndexName), Optional.of(entityName));
    assertEquals(indexConventionNoPrefix.getEntityName("totally not an index"), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityName("dataset_v2"), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityName("dashboardindex_v2_1683649932260"),
        Optional.of("dashboard"));
  }

  @Test
  public void testIndexConventionPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConventionPrefix =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("prefix")
                .hashIdAlgo("MD5")
                .build(),
            entityIndexConfiguration);
    String entityName = "dataset";
    String expectedIndexName = "prefix_datasetindex_v2";
    assertEquals(indexConventionPrefix.getEntityIndexName(entityName), expectedIndexName);
    assertEquals(indexConventionPrefix.getPrefix(), Optional.of("prefix"));
    assertEquals(indexConventionPrefix.getEntityName(expectedIndexName), Optional.of(entityName));
    assertEquals(indexConventionPrefix.getEntityName("totally not an index"), Optional.empty());
    assertEquals(indexConventionPrefix.getEntityName("prefix_dataset_v2"), Optional.empty());
    assertEquals(
        indexConventionPrefix.getEntityName("prefix_dashboardindex_v2_1683649932260"),
        Optional.of("dashboard"));
    assertEquals(
        indexConventionPrefix.getEntityName("dashboardindex_v2_1683649932260"), Optional.empty());
  }

  @Test
  public void testTimeseriesIndexConventionNoPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConventionNoPrefix =
        IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);
    String entityName = "dataset";
    String aspectName = "datasetusagestatistics";
    String expectedIndexName = "dataset_datasetusagestatisticsaspect_v1";
    assertEquals(
        indexConventionNoPrefix.getTimeseriesAspectIndexName(entityName, aspectName),
        expectedIndexName);
    assertEquals(indexConventionNoPrefix.getPrefix(), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName(expectedIndexName),
        Optional.of(Pair.of(entityName, aspectName)));
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName("totally not an index"), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityAndAspectName("dataset_v2"), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName(
            "dashboard_dashboardusagestatisticsaspect_v1"),
        Optional.of(Pair.of("dashboard", "dashboardusagestatistics")));
  }

  @Test
  public void testTimeseriesIndexConventionPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConventionPrefix =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("prefix")
                .hashIdAlgo("MD5")
                .build(),
            entityIndexConfiguration);
    String entityName = "dataset";
    String aspectName = "datasetusagestatistics";
    String expectedIndexName = "prefix_dataset_datasetusagestatisticsaspect_v1";
    assertEquals(
        indexConventionPrefix.getTimeseriesAspectIndexName(entityName, aspectName),
        expectedIndexName);
    assertEquals(indexConventionPrefix.getPrefix(), Optional.of("prefix"));
    assertEquals(
        indexConventionPrefix.getEntityAndAspectName(expectedIndexName),
        Optional.of(Pair.of(entityName, aspectName)));
    assertEquals(
        indexConventionPrefix.getEntityAndAspectName("totally not an index"), Optional.empty());
    assertEquals(
        indexConventionPrefix.getEntityAndAspectName("prefix_datasetusagestatisticsaspect_v1"),
        Optional.empty());
  }

  @Test
  public void testSchemaFieldDocumentId() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    assertEquals(
        new IndexConventionImpl(
                IndexConventionImpl.IndexConventionConfig.builder()
                    .prefix("")
                    .hashIdAlgo("")
                    .schemaFieldDocIdHashEnabled(true)
                    .build(),
                entityIndexConfiguration)
            .getEntityDocumentId(
                UrnUtils.getUrn(
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.factor_income,PROD),year)")),
        URLEncoder.encode(
            String.format(
                "urn:li:schemaField:(%s,%s)",
                sha256Hex(
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.factor_income,PROD)"),
                sha256Hex("year"))));
  }

  @Test
  public void testIsV2EntityIndex() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);

    // Test valid v2 entity indices
    assertTrue(
        indexConvention.isV2EntityIndex("datasetindex_v2"), "Should identify v2 entity index");
    assertTrue(
        indexConvention.isV2EntityIndex("dashboardindex_v2"), "Should identify v2 entity index");
    assertTrue(
        indexConvention.isV2EntityIndex("prefix_datasetindex_v2"),
        "Should identify v2 entity index with prefix");
    assertTrue(
        indexConvention.isV2EntityIndex("very_long_entity_nameindex_v2"),
        "Should identify v2 entity index with long name");

    // Test invalid indices
    assertFalse(
        indexConvention.isV2EntityIndex("datasetindex_v3"), "Should not identify v3 index as v2");
    assertFalse(
        indexConvention.isV2EntityIndex("datasetindex_v1"), "Should not identify v1 index as v2");
    assertFalse(
        indexConvention.isV2EntityIndex("dataset_v2"),
        "Should not identify index without 'index' suffix");
    assertFalse(
        indexConvention.isV2EntityIndex("index_v2"), "Should not identify standalone suffix");
    assertFalse(
        indexConvention.isV2EntityIndex("datasetindex_v2_extra"),
        "Should not identify index with extra suffix");
    assertFalse(indexConvention.isV2EntityIndex(""), "Should not identify empty string");
    assertFalse(
        indexConvention.isV2EntityIndex("not_an_index"), "Should not identify non-index string");
    assertFalse(
        indexConvention.isV2EntityIndex("datasetindex_v2_1683649932260"),
        "Should not identify timestamped index");
  }

  @Test
  public void testIsV3EntityIndex() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);

    // Test valid v3 entity indices
    assertTrue(
        indexConvention.isV3EntityIndex("datasetindex_v3"), "Should identify v3 entity index");
    assertTrue(
        indexConvention.isV3EntityIndex("dashboardindex_v3"), "Should identify v3 entity index");
    assertTrue(
        indexConvention.isV3EntityIndex("prefix_datasetindex_v3"),
        "Should identify v3 entity index with prefix");
    assertTrue(
        indexConvention.isV3EntityIndex("very_long_entity_nameindex_v3"),
        "Should identify v3 entity index with long name");

    // Test invalid indices
    assertFalse(
        indexConvention.isV3EntityIndex("datasetindex_v2"), "Should not identify v2 index as v3");
    assertFalse(
        indexConvention.isV3EntityIndex("datasetindex_v1"), "Should not identify v1 index as v3");
    assertFalse(
        indexConvention.isV3EntityIndex("dataset_v3"),
        "Should not identify index without 'index' suffix");
    assertFalse(
        indexConvention.isV3EntityIndex("index_v3"), "Should not identify standalone suffix");
    assertFalse(
        indexConvention.isV3EntityIndex("datasetindex_v3_extra"),
        "Should not identify index with extra suffix");
    assertFalse(indexConvention.isV3EntityIndex(""), "Should not identify empty string");
    assertFalse(
        indexConvention.isV3EntityIndex("not_an_index"), "Should not identify non-index string");
    assertFalse(
        indexConvention.isV3EntityIndex("datasetindex_v3_1683649932260"),
        "Should not identify timestamped index");
  }

  @Test
  public void testIsV2EntityIndexWithPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("test_prefix")
                .hashIdAlgo("MD5")
                .build(),
            entityIndexConfiguration);

    // Test valid v2 entity indices with prefix
    assertTrue(
        indexConvention.isV2EntityIndex("test_prefix_datasetindex_v2"),
        "Should identify v2 entity index with prefix");
    assertTrue(
        indexConvention.isV2EntityIndex("test_prefix_dashboardindex_v2"),
        "Should identify v2 entity index with prefix");

    // Test invalid indices
    assertFalse(
        indexConvention.isV2EntityIndex("datasetindex_v2"),
        "Should not identify v2 index without prefix");
    assertFalse(
        indexConvention.isV2EntityIndex("wrong_prefix_datasetindex_v2"),
        "Should not identify v2 index with wrong prefix");
  }

  @Test
  public void testIsV3EntityIndexWithPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("test_prefix")
                .hashIdAlgo("MD5")
                .build(),
            entityIndexConfiguration);

    // Test valid v3 entity indices with prefix
    assertTrue(
        indexConvention.isV3EntityIndex("test_prefix_datasetindex_v3"),
        "Should identify v3 entity index with prefix");
    assertTrue(
        indexConvention.isV3EntityIndex("test_prefix_dashboardindex_v3"),
        "Should identify v3 entity index with prefix");

    // Test invalid indices
    assertFalse(
        indexConvention.isV3EntityIndex("datasetindex_v3"),
        "Should not identify v3 index without prefix");
    assertFalse(
        indexConvention.isV3EntityIndex("wrong_prefix_datasetindex_v3"),
        "Should not identify v3 index with wrong prefix");
  }

  @Test
  public void testGetAllEntityIndicesPatterns() {
    // Test with no prefix and both V2 and V3 enabled
    EntityIndexConfiguration entityIndexConfiguration =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .build();

    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);
    List<String> patterns = indexConvention.getAllEntityIndicesPatterns();

    assertEquals(patterns.size(), 2, "Should return both V2 and V3 patterns");
    assertTrue(patterns.contains("*index_v2"), "Should contain V2 pattern");
    assertTrue(patterns.contains("*index_v3"), "Should contain V3 pattern");
  }

  @Test
  public void testGetAllEntityIndicesPatternsWithPrefix() {
    // Test with prefix and both V2 and V3 enabled
    EntityIndexConfiguration entityIndexConfiguration =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .build();

    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("test_prefix")
                .hashIdAlgo("MD5")
                .build(),
            entityIndexConfiguration);

    List<String> patterns = indexConvention.getAllEntityIndicesPatterns();

    assertEquals(patterns.size(), 2, "Should return both V2 and V3 patterns");
    assertTrue(patterns.contains("test_prefix_*index_v2"), "Should contain V2 pattern with prefix");
    assertTrue(patterns.contains("test_prefix_*index_v3"), "Should contain V3 pattern with prefix");
  }

  @Test
  public void testGetAllEntityIndicesPatternsOnlyV2() {
    // Test with only V2 enabled
    EntityIndexConfiguration entityIndexConfiguration =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
            .build();

    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);
    List<String> patterns = indexConvention.getAllEntityIndicesPatterns();

    assertEquals(patterns.size(), 1, "Should return only V2 pattern");
    assertTrue(patterns.contains("*index_v2"), "Should contain V2 pattern");
    assertFalse(patterns.contains("*index_v3"), "Should not contain V3 pattern");
  }

  @Test
  public void testGetAllEntityIndicesPatternsOnlyV3() {
    // Test with only V3 enabled
    EntityIndexConfiguration entityIndexConfiguration =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .build();

    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);
    List<String> patterns = indexConvention.getAllEntityIndicesPatterns();

    assertEquals(patterns.size(), 1, "Should return only V3 pattern");
    assertFalse(patterns.contains("*index_v2"), "Should not contain V2 pattern");
    assertTrue(patterns.contains("*index_v3"), "Should contain V3 pattern");
  }

  @Test
  public void testGetAllEntityIndicesPatternsNoneEnabled() {
    // Test with neither V2 nor V3 enabled
    EntityIndexConfiguration entityIndexConfiguration =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
            .build();

    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);
    List<String> patterns = indexConvention.getAllEntityIndicesPatterns();

    assertEquals(patterns.size(), 0, "Should return empty list when no versions are enabled");
  }

  @Test
  public void testGetV3EntityIndexPatterns() {
    // Test with no prefix - should return v3 pattern regardless of configuration
    EntityIndexConfiguration entityIndexConfiguration =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
            .build();

    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);
    List<String> v3Patterns = indexConvention.getV3EntityIndexPatterns();

    assertEquals(v3Patterns.size(), 1, "Should return one v3 pattern");
    assertEquals(
        v3Patterns.get(0), "*index_v3", "Should return v3 pattern regardless of configuration");
  }

  @Test
  public void testGetV3EntityIndexPatternsWithPrefix() {
    // Test with prefix - should return v3 pattern with prefix regardless of configuration
    EntityIndexConfiguration entityIndexConfiguration =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
            .build();

    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("test_prefix")
                .hashIdAlgo("MD5")
                .build(),
            entityIndexConfiguration);

    List<String> v3Patterns = indexConvention.getV3EntityIndexPatterns();

    assertEquals(v3Patterns.size(), 1, "Should return one v3 pattern");
    assertEquals(
        v3Patterns.get(0),
        "test_prefix_*index_v3",
        "Should return v3 pattern with prefix regardless of configuration");
  }

  @Test
  public void testGetV3EntityIndexPatternsWhenV3Disabled() {
    // Test that v3 pattern is returned even when v3 is disabled
    EntityIndexConfiguration entityIndexConfiguration =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
            .build();

    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);
    List<String> v3Patterns = indexConvention.getV3EntityIndexPatterns();

    assertEquals(v3Patterns.size(), 1, "Should return one v3 pattern");
    assertEquals(
        v3Patterns.get(0), "*index_v3", "Should return v3 pattern even when v3 is disabled");

    // Verify that getAllEntityIndicesPatterns doesn't include v3 when disabled
    List<String> allPatterns = indexConvention.getAllEntityIndicesPatterns();
    assertFalse(
        allPatterns.contains("*index_v3"),
        "getAllEntityIndicesPatterns should not include v3 when disabled");
  }
}
