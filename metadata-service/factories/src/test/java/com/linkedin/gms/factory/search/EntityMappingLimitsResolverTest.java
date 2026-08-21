package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityMappingLimits;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Behavior tests for {@link ElasticSearchIndexBuilderFactory#resolveEntityMappingLimits} and {@link
 * EntityMappingLimits#forIndex}. The integration with Spring lives in {@link
 * ElasticSearchIndexBuilderFactoryOverridesTest}; this suite is the fast path for the
 * config-to-resolver translation.
 */
public class EntityMappingLimitsResolverTest {

  private IndexConvention indexConvention;

  @BeforeMethod
  void setUp() {
    indexConvention = mock(IndexConvention.class);
    lenient()
        .when(
            indexConvention.getEntityIndexName(
                org.mockito.ArgumentMatchers.any(com.datahub.context.OperationFingerprint.class),
                org.mockito.ArgumentMatchers.anyString()))
        .thenAnswer(inv -> inv.getArgument(1, String.class) + "index_v2");
  }

  @Test
  void nullConfigYieldsEmptyResolver() {
    EntityMappingLimits resolved =
        ElasticSearchIndexBuilderFactory.resolveEntityMappingLimits(null, indexConvention);
    assertSame(resolved, EntityMappingLimits.EMPTY);
  }

  @Test
  void emptyConfigYieldsEmptyResolver() {
    EntityMappingLimits resolved =
        ElasticSearchIndexBuilderFactory.resolveEntityMappingLimits(Map.of(), indexConvention);
    assertSame(resolved, EntityMappingLimits.EMPTY);
  }

  @Test
  void translatesTotalFieldsAndStringifiesValues() {
    EntityMappingLimits resolved =
        ElasticSearchIndexBuilderFactory.resolveEntityMappingLimits(
            Map.of("dataset", Map.of("totalFields", 2500)), indexConvention);

    assertEquals(
        resolved.byIndex(),
        Map.of("datasetindex_v2", Map.of("mapping.total_fields.limit", "2500")));
    assertEquals(resolved.defaults(), Map.of());
  }

  @Test
  void defaultKeyPopulatesDefaultsAndDoesNotPassThroughIndexConvention() {
    EntityMappingLimits resolved =
        ElasticSearchIndexBuilderFactory.resolveEntityMappingLimits(
            Map.of(
                "default", Map.of("totalFields", 1500),
                "dataset", Map.of("totalFields", 2500)),
            indexConvention);

    assertEquals(resolved.defaults(), Map.of("mapping.total_fields.limit", "1500"));
    assertEquals(
        resolved.byIndex(),
        Map.of("datasetindex_v2", Map.of("mapping.total_fields.limit", "2500")));
    // Verify the reserved key never went through indexConvention (e.g. no "defaultindex_v2").
    org.mockito.Mockito.verify(indexConvention, org.mockito.Mockito.never())
        .getEntityIndexName(
            org.mockito.ArgumentMatchers.any(com.datahub.context.OperationFingerprint.class),
            org.mockito.ArgumentMatchers.eq("default"));
  }

  @Test
  void limitKeyLookupIsCaseInsensitive() {
    // Spring's MapBinder lowercases env-var-derived map keys (e.g. TOTALFIELDS -> totalfields),
    // while YAML/property files preserve case. Both must resolve to the same allowlist entry.
    EntityMappingLimits fromEnvVarStyle =
        ElasticSearchIndexBuilderFactory.resolveEntityMappingLimits(
            Map.of("dataset", Map.of("totalfields", 2500)), indexConvention);
    EntityMappingLimits fromYamlStyle =
        ElasticSearchIndexBuilderFactory.resolveEntityMappingLimits(
            Map.of("dataset", Map.of("totalFields", 2500)), indexConvention);

    Map<String, Map<String, String>> expected =
        Map.of("datasetindex_v2", Map.of("mapping.total_fields.limit", "2500"));
    assertEquals(fromEnvVarStyle.byIndex(), expected);
    assertEquals(fromYamlStyle.byIndex(), expected);
  }

  @Test
  void unsupportedLimitKeyIsDropped() {
    EntityMappingLimits resolved =
        ElasticSearchIndexBuilderFactory.resolveEntityMappingLimits(
            Map.of("dataset", Map.of("nestedFields", 100)), indexConvention);

    assertTrue(resolved.byIndex().isEmpty());
    assertTrue(resolved.defaults().isEmpty());
  }

  @Test
  void supportedAndUnsupportedKeysCoexist() {
    EntityMappingLimits resolved =
        ElasticSearchIndexBuilderFactory.resolveEntityMappingLimits(
            Map.of("dataset", Map.of("totalFields", 2500, "nestedFields", 100)), indexConvention);

    assertEquals(
        resolved.byIndex(),
        Map.of("datasetindex_v2", Map.of("mapping.total_fields.limit", "2500")));
  }

  @Test
  void forIndexPrefersExplicitOverDefault() {
    EntityMappingLimits limits =
        new EntityMappingLimits(
            Map.of("datasetindex_v2", Map.of("mapping.total_fields.limit", "2500")),
            Map.of("mapping.total_fields.limit", "1500"));

    assertEquals(limits.forIndex("datasetindex_v2"), Map.of("mapping.total_fields.limit", "2500"));
    assertEquals(
        limits.forIndex("dashboardindex_v2"), Map.of("mapping.total_fields.limit", "1500"));
  }

  @Test
  void forIndexReturnsEmptyWhenNothingConfigured() {
    assertTrue(EntityMappingLimits.EMPTY.forIndex("datasetindex_v2").isEmpty());
  }

  @Test
  void resolverIsImmutableAfterConstruction() {
    EntityMappingLimits resolved =
        ElasticSearchIndexBuilderFactory.resolveEntityMappingLimits(
            new java.util.HashMap<>(Map.of("dataset", Map.of("totalFields", 2500))),
            indexConvention);
    assertNotSame(resolved, EntityMappingLimits.EMPTY);
    try {
      resolved.byIndex().put("evil", Map.of("k", "v"));
      org.testng.Assert.fail("byIndex map should be immutable");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }
}
