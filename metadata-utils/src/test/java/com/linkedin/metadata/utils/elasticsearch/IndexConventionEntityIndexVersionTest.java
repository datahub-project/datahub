package com.linkedin.metadata.utils.elasticsearch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

/** Generation and V3-key parsing on {@link IndexConvention} for alias and physical index names. */
public class IndexConventionEntityIndexVersionTest {

  private static final OperationFingerprint OP = OperationFingerprint.EMPTY;

  private static IndexConvention noPrefix() {
    return IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration());
  }

  private static IndexConvention withPrefix(String prefix) {
    return new IndexConventionImpl(
        IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
        new ConfiguredIndexPrefixResolver(prefix),
        new EntityIndexConfiguration());
  }

  @Test
  public void testEntityIndexVersionOf() {
    IndexConvention convention = noPrefix();
    assertEquals(
        convention.entityIndexVersionOf("datasetindex_v2"), Optional.of(EntityIndexVersion.V2));
    assertEquals(
        convention.entityIndexVersionOf("primaryindex_v3_0_13_1_0_1704067200000"),
        Optional.of(EntityIndexVersion.V3));
    assertEquals(convention.entityIndexVersionOf("graph_service_v1"), Optional.empty());
  }

  @Test
  public void testGetEntityIndexKeyV3RoundTripsAliasAndPhysical() {
    IndexConvention convention = noPrefix();
    String alias = convention.getEntityIndexNameV3(OP, "primary");
    assertEquals(alias, "primaryindex_v3");
    assertEquals(convention.getEntityIndexKeyV3(OP, alias), Optional.of("primary"));
    assertEquals(
        convention.getEntityIndexKeyV3(OP, alias + "_0_13_1_0_1704067200000"),
        Optional.of("primary"));
    assertEquals(convention.getEntityIndexKeyV3(OP, "datasetindex_v3"), Optional.of("dataset"));
    assertEquals(convention.getEntityIndexKeyV3(OP, "datasetindex_v2"), Optional.empty());
    assertEquals(convention.getEntityIndexKeyV3(OP, "index_v3"), Optional.empty());
  }

  @Test
  public void testGetEntityIndexKeyV3WithPrefix() {
    IndexConvention convention = withPrefix("acme");
    String alias = convention.getEntityIndexNameV3(OP, "primary");
    assertEquals(alias, "acme_primaryindex_v3");
    assertEquals(convention.getEntityIndexKeyV3(OP, alias), Optional.of("primary"));
    assertEquals(
        convention.getEntityIndexKeyV3(OP, alias + "_1683649932260"), Optional.of("primary"));
    assertEquals(convention.getEntityIndexKeyV3(OP, "primaryindex_v3"), Optional.empty());
    assertEquals(convention.getEntityIndexKeyV3(OP, "other_primaryindex_v3"), Optional.empty());
  }

  @Test
  public void testGetEntityIndexKeyV3UsesTheV3ClusterPrefix() {
    // elasticsearch.clusters.<v3 cluster>.index.prefix replaces the deploy-wide prefix for the V3
    // family only, so the inverse must strip that overlay rather than the deploy-wide value.
    IndexConvention convention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("acme"),
            new EntityIndexConfiguration(),
            Map.of(SearchComponent.SEARCH_V3, "tenant"));
    String alias = convention.getEntityIndexNameV3(OP, "primary");
    assertEquals(alias, "tenant_primaryindex_v3");
    assertEquals(convention.getEntityIndexKeyV3(OP, alias), Optional.of("primary"));
    assertEquals(
        convention.getEntityIndexKeyV3(OP, alias + "_0_13_1_0_1704067200000"),
        Optional.of("primary"));
    assertEquals(convention.getEntityIndexKeyV3(OP, "acme_primaryindex_v3"), Optional.empty());
    assertEquals(convention.getEntityIndexKeyV3(OP, "primaryindex_v3"), Optional.empty());
    // The alias checks resolve the same prefix the names are built under: the V3 overlay for V3
    // aliases, the deploy-wide prefix for V2. Reindex versioning and V3 query shaping key off them.
    assertTrue(convention.isV3EntityIndex(OP, alias));
    assertFalse(convention.isV3EntityIndex(OP, "acme_primaryindex_v3"));
    assertFalse(convention.isV3EntityIndex(OP, "primaryindex_v3"));
    assertTrue(convention.isV2EntityIndex(OP, convention.getEntityIndexName(OP, "dataset")));
    assertTrue(convention.isV2EntityIndex(OP, "acme_datasetindex_v2"));
    assertFalse(convention.isV2EntityIndex(OP, "tenant_datasetindex_v2"));
  }

  @Test
  public void testV3AliasCheckRequiresTheClusterPrefixEvenWithoutADeployWidePrefix() {
    // No deploy-wide prefix, but the V3 cluster has one: an unprefixed V3 name is not this
    // deployment's alias, and an empty resolver prefix must not stand in for the overlay.
    IndexConvention convention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver(null),
            new EntityIndexConfiguration(),
            Map.of(SearchComponent.SEARCH_V3, "tenant"));
    assertEquals(convention.getEntityIndexNameV3(OP, "primary"), "tenant_primaryindex_v3");
    assertTrue(convention.isV3EntityIndex(OP, "tenant_primaryindex_v3"));
    assertFalse(convention.isV3EntityIndex(OP, "primaryindex_v3"));
    // V2 has no overlay and there is no deploy-wide prefix, so its check keeps the convention's
    // unprefixed behaviour and accepts any V2 alias.
    assertTrue(convention.isV2EntityIndex(OP, "datasetindex_v2"));
  }

  @Test
  public void testBackingNamesClassifyByGenerationButAreNotAliases() {
    IndexConvention convention = noPrefix();
    String backing = "primaryindex_v3_0_13_1_0_1704067200000";
    assertTrue(convention.isV3EntityIndexOrBackingType(backing));
    assertTrue(convention.isV3EntityIndexOrBackingType("primaryindex_v3"));
    assertFalse(convention.isV3EntityIndexType(backing));
    assertFalse(convention.isV3EntityIndex(OP, backing));
    assertTrue(convention.isV2EntityIndexOrBackingType("datasetindex_v2_1683649932260"));
    assertFalse(convention.isV2EntityIndexOrBackingType("datasetindex_v2_semantic_1683649932260"));
    assertFalse(convention.isV3EntityIndexOrBackingType("datasetindex_v2_1683649932260"));
  }
}
