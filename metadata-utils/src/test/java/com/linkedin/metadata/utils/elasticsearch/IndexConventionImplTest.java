package com.linkedin.metadata.utils.elasticsearch;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;
import static org.testng.Assert.*;

import com.datahub.context.Enrichment;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.util.Pair;
import java.net.URLEncoder;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

public class IndexConventionImplTest {

  // No tenant / request identity: resolvers fall back to their static/deploy prefix.
  private static final OperationFingerprint OP = OperationFingerprint.EMPTY;

  private static IndexConvention withPrefix(
      String prefix, EntityIndexConfiguration entityIndexConfiguration) {
    return new IndexConventionImpl(
        IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
        new ConfiguredIndexPrefixResolver(prefix),
        entityIndexConfiguration);
  }

  @Test
  public void testIndexConventionNoPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConventionNoPrefix =
        IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);
    String entityName = "dataset";
    String expectedIndexName = "datasetindex_v2";
    assertEquals(indexConventionNoPrefix.getEntityIndexName(OP, entityName), expectedIndexName);
    assertEquals(indexConventionNoPrefix.getPrefix(OP), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityName(OP, expectedIndexName), Optional.of(entityName));
    assertEquals(
        indexConventionNoPrefix.getEntityName(OP, "totally not an index"), Optional.empty());
    assertEquals(indexConventionNoPrefix.getEntityName(OP, "dataset_v2"), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityName(OP, "dashboardindex_v2_1683649932260"),
        Optional.of("dashboard"));
  }

  @Test
  public void testIndexConventionPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConventionPrefix = withPrefix("prefix", entityIndexConfiguration);
    String entityName = "dataset";
    String expectedIndexName = "prefix_datasetindex_v2";
    assertEquals(indexConventionPrefix.getEntityIndexName(OP, entityName), expectedIndexName);
    assertEquals(indexConventionPrefix.getPrefix(OP), Optional.of("prefix"));
    assertEquals(
        indexConventionPrefix.getEntityName(OP, expectedIndexName), Optional.of(entityName));
    assertEquals(indexConventionPrefix.getEntityName(OP, "totally not an index"), Optional.empty());
    assertEquals(indexConventionPrefix.getEntityName(OP, "prefix_dataset_v2"), Optional.empty());
    assertEquals(
        indexConventionPrefix.getEntityName(OP, "prefix_dashboardindex_v2_1683649932260"),
        Optional.of("dashboard"));
    assertEquals(
        indexConventionPrefix.getEntityName(OP, "dashboardindex_v2_1683649932260"),
        Optional.empty());
  }

  /**
   * The prefix is resolved from the operation, not baked into the convention: a single convention
   * instance yields different index names for different operations, and its inverse strips the
   * matching prefix. This is the core contract enabling per-operation (e.g. per-tenant) index
   * isolation.
   */
  @Test
  public void testPrefixResolvedPerOperation() {
    EntityIndexConfiguration entityIndexConfiguration =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(true).cleanup(true).build())
            .v3(EntityIndexVersionConfiguration.builder().enabled(false).cleanup(false).build())
            .build();
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new PrefixEnrichmentResolver("fallback"),
            entityIndexConfiguration);

    OperationFingerprint acme = operationWithPrefix("acme");
    OperationFingerprint beta = operationWithPrefix("beta");

    assertEquals(indexConvention.getEntityIndexName(acme, "dataset"), "acme_datasetindex_v2");
    assertEquals(indexConvention.getEntityIndexName(beta, "dataset"), "beta_datasetindex_v2");
    // No enrichment -> deploy/fallback prefix.
    assertEquals(indexConvention.getEntityIndexName(OP, "dataset"), "fallback_datasetindex_v2");

    // Inverse strips the operation's own prefix, and rejects another operation's prefix.
    assertEquals(
        indexConvention.getEntityName(acme, "acme_datasetindex_v2"), Optional.of("dataset"));
    assertEquals(indexConvention.getEntityName(acme, "beta_datasetindex_v2"), Optional.empty());

    assertEquals(indexConvention.getAllEntityIndicesPatterns(acme), List.of("acme_*index_v2"));
    assertEquals(indexConvention.getAllEntityIndicesPatterns(beta), List.of("beta_*index_v2"));
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
        indexConventionNoPrefix.getTimeseriesAspectIndexName(OP, entityName, aspectName),
        expectedIndexName);
    assertEquals(indexConventionNoPrefix.getPrefix(OP), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName(OP, expectedIndexName),
        Optional.of(Pair.of(entityName, aspectName)));
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName(OP, "totally not an index"),
        Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName(OP, "dataset_v2"), Optional.empty());
    assertEquals(
        indexConventionNoPrefix.getEntityAndAspectName(
            OP, "dashboard_dashboardusagestatisticsaspect_v1"),
        Optional.of(Pair.of("dashboard", "dashboardusagestatistics")));
  }

  @Test
  public void testTimeseriesIndexConventionPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConventionPrefix = withPrefix("prefix", entityIndexConfiguration);
    String entityName = "dataset";
    String aspectName = "datasetusagestatistics";
    String expectedIndexName = "prefix_dataset_datasetusagestatisticsaspect_v1";
    assertEquals(
        indexConventionPrefix.getTimeseriesAspectIndexName(OP, entityName, aspectName),
        expectedIndexName);
    assertEquals(indexConventionPrefix.getPrefix(OP), Optional.of("prefix"));
    assertEquals(
        indexConventionPrefix.getEntityAndAspectName(OP, expectedIndexName),
        Optional.of(Pair.of(entityName, aspectName)));
    assertEquals(
        indexConventionPrefix.getEntityAndAspectName(OP, "totally not an index"), Optional.empty());
    assertEquals(
        indexConventionPrefix.getEntityAndAspectName(OP, "prefix_datasetusagestatisticsaspect_v1"),
        Optional.empty());
  }

  @Test
  public void testSchemaFieldDocumentId() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    assertEquals(
        new IndexConventionImpl(
                IndexConventionImpl.IndexConventionConfig.builder()
                    .hashIdAlgo("")
                    .schemaFieldDocIdHashEnabled(true)
                    .build(),
                new ConfiguredIndexPrefixResolver(""),
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
        indexConvention.isV2EntityIndex(OP, "datasetindex_v2"), "Should identify v2 entity index");
    assertTrue(
        indexConvention.isV2EntityIndex(OP, "dashboardindex_v2"),
        "Should identify v2 entity index");
    assertTrue(
        indexConvention.isV2EntityIndex(OP, "prefix_datasetindex_v2"),
        "Should identify v2 entity index with prefix");
    assertTrue(
        indexConvention.isV2EntityIndex(OP, "very_long_entity_nameindex_v2"),
        "Should identify v2 entity index with long name");

    // Test invalid indices
    assertFalse(
        indexConvention.isV2EntityIndex(OP, "datasetindex_v3"),
        "Should not identify v3 index as v2");
    assertFalse(
        indexConvention.isV2EntityIndex(OP, "datasetindex_v1"),
        "Should not identify v1 index as v2");
    assertFalse(
        indexConvention.isV2EntityIndex(OP, "dataset_v2"),
        "Should not identify index without 'index' suffix");
    assertFalse(
        indexConvention.isV2EntityIndex(OP, "index_v2"), "Should not identify standalone suffix");
    assertFalse(
        indexConvention.isV2EntityIndex(OP, "datasetindex_v2_extra"),
        "Should not identify index with extra suffix");
    assertFalse(indexConvention.isV2EntityIndex(OP, ""), "Should not identify empty string");
    assertFalse(
        indexConvention.isV2EntityIndex(OP, "not_an_index"),
        "Should not identify non-index string");
    assertFalse(
        indexConvention.isV2EntityIndex(OP, "datasetindex_v2_1683649932260"),
        "Should not identify timestamped index");
  }

  @Test
  public void testIsV3EntityIndex() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);

    // Test valid v3 entity indices
    assertTrue(
        indexConvention.isV3EntityIndex(OP, "datasetindex_v3"), "Should identify v3 entity index");
    assertTrue(
        indexConvention.isV3EntityIndex(OP, "dashboardindex_v3"),
        "Should identify v3 entity index");
    assertTrue(
        indexConvention.isV3EntityIndex(OP, "prefix_datasetindex_v3"),
        "Should identify v3 entity index with prefix");
    assertTrue(
        indexConvention.isV3EntityIndex(OP, "very_long_entity_nameindex_v3"),
        "Should identify v3 entity index with long name");

    // Test invalid indices
    assertFalse(
        indexConvention.isV3EntityIndex(OP, "datasetindex_v2"),
        "Should not identify v2 index as v3");
    assertFalse(
        indexConvention.isV3EntityIndex(OP, "datasetindex_v1"),
        "Should not identify v1 index as v3");
    assertFalse(
        indexConvention.isV3EntityIndex(OP, "dataset_v3"),
        "Should not identify index without 'index' suffix");
    assertFalse(
        indexConvention.isV3EntityIndex(OP, "index_v3"), "Should not identify standalone suffix");
    assertFalse(
        indexConvention.isV3EntityIndex(OP, "datasetindex_v3_extra"),
        "Should not identify index with extra suffix");
    assertFalse(indexConvention.isV3EntityIndex(OP, ""), "Should not identify empty string");
    assertFalse(
        indexConvention.isV3EntityIndex(OP, "not_an_index"),
        "Should not identify non-index string");
    assertFalse(
        indexConvention.isV3EntityIndex(OP, "datasetindex_v3_1683649932260"),
        "Should not identify timestamped index");
  }

  @Test
  public void testIsV2EntityIndexWithPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = withPrefix("test_prefix", entityIndexConfiguration);

    // Test valid v2 entity indices with prefix
    assertTrue(
        indexConvention.isV2EntityIndex(OP, "test_prefix_datasetindex_v2"),
        "Should identify v2 entity index with prefix");
    assertTrue(
        indexConvention.isV2EntityIndex(OP, "test_prefix_dashboardindex_v2"),
        "Should identify v2 entity index with prefix");

    // Test invalid indices
    assertFalse(
        indexConvention.isV2EntityIndex(OP, "datasetindex_v2"),
        "Should not identify v2 index without prefix");
    assertFalse(
        indexConvention.isV2EntityIndex(OP, "wrong_prefix_datasetindex_v2"),
        "Should not identify v2 index with wrong prefix");
  }

  @Test
  public void testIsV3EntityIndexWithPrefix() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = withPrefix("test_prefix", entityIndexConfiguration);

    // Test valid v3 entity indices with prefix
    assertTrue(
        indexConvention.isV3EntityIndex(OP, "test_prefix_datasetindex_v3"),
        "Should identify v3 entity index with prefix");
    assertTrue(
        indexConvention.isV3EntityIndex(OP, "test_prefix_dashboardindex_v3"),
        "Should identify v3 entity index with prefix");

    // Test invalid indices
    assertFalse(
        indexConvention.isV3EntityIndex(OP, "datasetindex_v3"),
        "Should not identify v3 index without prefix");
    assertFalse(
        indexConvention.isV3EntityIndex(OP, "wrong_prefix_datasetindex_v3"),
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
    List<String> patterns = indexConvention.getAllEntityIndicesPatterns(OP);

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

    IndexConvention indexConvention = withPrefix("test_prefix", entityIndexConfiguration);

    List<String> patterns = indexConvention.getAllEntityIndicesPatterns(OP);

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
    List<String> patterns = indexConvention.getAllEntityIndicesPatterns(OP);

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
    List<String> patterns = indexConvention.getAllEntityIndicesPatterns(OP);

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
    List<String> patterns = indexConvention.getAllEntityIndicesPatterns(OP);

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
    List<String> v3Patterns = indexConvention.getV3EntityIndexPatterns(OP);

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

    IndexConvention indexConvention = withPrefix("test_prefix", entityIndexConfiguration);

    List<String> v3Patterns = indexConvention.getV3EntityIndexPatterns(OP);

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
    List<String> v3Patterns = indexConvention.getV3EntityIndexPatterns(OP);

    assertEquals(v3Patterns.size(), 1, "Should return one v3 pattern");
    assertEquals(
        v3Patterns.get(0), "*index_v3", "Should return v3 pattern even when v3 is disabled");

    // Verify that getAllEntityIndicesPatterns doesn't include v3 when disabled
    List<String> allPatterns = indexConvention.getAllEntityIndicesPatterns(OP);
    assertFalse(
        allPatterns.contains("*index_v3"),
        "getAllEntityIndicesPatterns should not include v3 when disabled");
  }

  @Test
  public void testIsSemanticEntityIndex() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);

    assertTrue(
        indexConvention.isSemanticEntityIndex(OP, "datasetindex_v2_semantic"),
        "Should identify semantic entity index");

    assertFalse(
        indexConvention.isSemanticEntityIndex(OP, "datasetindex_v2"),
        "Should not identify v2 index as semantic");
    // A versioned semantic backing index must NOT be treated as the bare semantic index, so
    // orphan cleanup still reclaims stale semantic backing indices.
    assertFalse(
        indexConvention.isSemanticEntityIndex(OP, "datasetindex_v2_semantic_1700000000000"),
        "Should not identify a versioned semantic backing index");
  }

  /**
   * The resolved-name cache is a bounded LRU (see {@code
   * IndexConventionImpl#INDEX_NAME_CACHE_MAX_SIZE}): resolving more distinct (prefix, base) pairs
   * than it can hold must never return a stale or wrong name for an evicted entry — an eviction
   * just forces a (cheap) recompute. Guards the singleton bean against per-prefix (e.g. per-tenant)
   * unbounded growth and stale reads after eviction.
   */
  @Test
  public void testBoundedNameCacheStaysCorrectUnderManyPrefixes() {
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new PrefixEnrichmentResolver("fallback"),
            entityIndexConfiguration);

    // Exceed the LRU bound (10_000) with distinct prefixes; every resolved name must be correct.
    for (int i = 0; i < 15_000; i++) {
      assertEquals(
          indexConvention.getEntityIndexName(operationWithPrefix("t" + i), "dataset"),
          "t" + i + "_datasetindex_v2");
    }
    // "t0" is now evicted — re-resolving it must still yield the correct name, never a stale one.
    assertEquals(
        indexConvention.getEntityIndexName(operationWithPrefix("t0"), "dataset"),
        "t0_datasetindex_v2");
  }

  // --- Test fixtures for per-operation prefix resolution -------------------------------------

  /** A test enrichment carrying an index prefix, mirroring how a deployment stamps identity. */
  private record PrefixEnrichment(String prefix) implements Enrichment {}

  /** Resolves the prefix from {@link PrefixEnrichment}, falling back to a fixed default. */
  private static final class PrefixEnrichmentResolver implements IndexPrefixResolver {
    private final String fallback;

    private PrefixEnrichmentResolver(String fallback) {
      this.fallback = fallback;
    }

    @Nonnull
    @Override
    public String resolvePrefix(@Nonnull OperationFingerprint operation) {
      return operation
          .getEnrichment(PrefixEnrichment.class)
          .map(PrefixEnrichment::prefix)
          .orElse(fallback);
    }
  }

  private static OperationFingerprint operationWithPrefix(String prefix) {
    return new OperationFingerprint() {
      @Nonnull
      @Override
      public com.linkedin.common.urn.Urn getActor() {
        return OperationFingerprint.EMPTY.getActor();
      }

      @Nonnull
      @Override
      public String getRequestID() {
        return OperationFingerprint.EMPTY.getRequestID();
      }

      @Nonnull
      @Override
      public com.linkedin.common.AuditStamp getAuditStamp() {
        return OperationFingerprint.EMPTY.getAuditStamp();
      }

      @Nonnull
      @Override
      public String getGlobalContextId() {
        return OperationFingerprint.EMPTY.getGlobalContextId();
      }

      @Nonnull
      @Override
      public String getSearchContextId() {
        return OperationFingerprint.EMPTY.getSearchContextId();
      }

      @Nonnull
      @Override
      public String getEntityContextId() {
        return OperationFingerprint.EMPTY.getEntityContextId();
      }

      @Nonnull
      @Override
      @SuppressWarnings("unchecked")
      public <T extends Enrichment> Optional<T> getEnrichment(@Nonnull Class<T> type) {
        if (type.equals(PrefixEnrichment.class)) {
          return Optional.of((T) new PrefixEnrichment(prefix));
        }
        return Optional.empty();
      }
    };
  }
}
