package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class EntityMappingLimitsTest {

  private static final OperationFingerprint OP = OperationFingerprint.EMPTY;
  private static final String LIMIT = "mapping.total_fields.limit";
  private static final IndexConvention NO_PREFIX =
      IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration());
  private static final IndexConvention PROD_PREFIX =
      new IndexConventionImpl(
          IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
          new ConfiguredIndexPrefixResolver("prod"),
          new EntityIndexConfiguration());

  @Test
  public void testFromConfigNullOrEmptyIsEmpty() {
    assertSame(EntityMappingLimits.fromConfig(null), EntityMappingLimits.EMPTY);
    assertSame(EntityMappingLimits.fromConfig(Map.of()), EntityMappingLimits.EMPTY);
    assertTrue(EntityMappingLimits.EMPTY.forIndex(NO_PREFIX, OP, "datasetindex_v2").isEmpty());
  }

  @Test
  public void testFromConfigTranslatesKeysCaseInsensitively() {
    // Env vars bind as lower-case keys (DATAJOB_TOTALFIELDS -> datajob.totalfields); YAML keeps
    // camelCase (dataJob.totalFields). Both must land on the same entity and ES setting.
    EntityMappingLimits fromEnv =
        EntityMappingLimits.fromConfig(Map.of("datajob", Map.of("totalfields", 2500)));
    EntityMappingLimits fromYaml =
        EntityMappingLimits.fromConfig(
            Map.of("dataJob", Map.of("totalFields", 2500), "DEFAULT", Map.of("totalFields", 1500)));

    assertEquals(fromEnv.byEntity(), Map.of("datajob", Map.of(LIMIT, "2500")));
    assertEquals(fromYaml.byEntity(), fromEnv.byEntity());
    assertEquals(fromYaml.defaults(), Map.of(LIMIT, "1500"));
  }

  @Test
  public void testFromConfigDropsUnsupportedAndEmptyEntries() {
    Map<String, Integer> nullValue = new HashMap<>();
    nullValue.put("totalFields", null);
    Map<String, Map<String, Integer>> config = new HashMap<>();
    config.put("dataset", Map.of("totalFields", 2500, "nestedFields", 100));
    config.put("chart", Map.of("nestedFields", 100));
    config.put("dashboard", nullValue);
    config.put("glossaryterm", null);

    EntityMappingLimits limits = EntityMappingLimits.fromConfig(config);

    assertEquals(limits.byEntity(), Map.of("dataset", Map.of(LIMIT, "2500")));
    assertTrue(limits.defaults().isEmpty());
  }

  @Test
  public void testForIndexResolvesV2AndSemanticIndicesForEntity() {
    EntityMappingLimits limits =
        EntityMappingLimits.fromConfig(Map.of("dataset", Map.of("totalFields", 2500)));

    assertEquals(limits.forIndex(NO_PREFIX, OP, "datasetindex_v2"), Map.of(LIMIT, "2500"));
    assertEquals(limits.forIndex(NO_PREFIX, OP, "datasetindex_v2_semantic"), Map.of(LIMIT, "2500"));
    assertEquals(limits.forIndex(PROD_PREFIX, OP, "prod_datasetindex_v2"), Map.of(LIMIT, "2500"));
    // Unlisted entity with no default configured.
    assertTrue(limits.forIndex(NO_PREFIX, OP, "chartindex_v2").isEmpty());
  }

  @Test
  public void testDefaultAppliesOnlyToEntityIndices() {
    EntityMappingLimits limits =
        EntityMappingLimits.fromConfig(
            Map.of("default", Map.of("totalFields", 1500), "dataset", Map.of("totalFields", 2500)));

    assertEquals(limits.forIndex(NO_PREFIX, OP, "datasetindex_v2"), Map.of(LIMIT, "2500"));
    assertEquals(limits.forIndex(NO_PREFIX, OP, "chartindex_v2"), Map.of(LIMIT, "1500"));
    assertEquals(limits.forIndex(NO_PREFIX, OP, "chartindex_v2_semantic"), Map.of(LIMIT, "1500"));
    // V3 indices carry their own field limit; graph / system metadata / timeseries are not entity
    // indices.
    assertTrue(limits.forIndex(NO_PREFIX, OP, "entityindex_v3").isEmpty());
    assertTrue(limits.forIndex(NO_PREFIX, OP, "graph_service_v1").isEmpty());
    assertTrue(limits.forIndex(NO_PREFIX, OP, "system_metadata_service_v1").isEmpty());
    assertTrue(limits.forIndex(NO_PREFIX, OP, "dataset_datasetprofileaspect_v1").isEmpty());
    // An entity-index-shaped name outside this operation's prefix is not ours to change.
    assertTrue(limits.forIndex(PROD_PREFIX, OP, "staging_chartindex_v2").isEmpty());
  }
}
