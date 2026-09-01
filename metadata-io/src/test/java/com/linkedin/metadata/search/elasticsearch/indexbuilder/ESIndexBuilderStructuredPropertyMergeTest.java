package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import java.util.Map;
import org.testng.annotations.Test;

public class ESIndexBuilderStructuredPropertyMergeTest {

  private ESIndexBuilder builder() {
    ElasticSearchConfiguration config = mock(ElasticSearchConfiguration.class, RETURNS_DEEP_STUBS);
    when(config.getBuildIndices().getSlowOperationTimeoutSeconds()).thenReturn(10);
    return new ESIndexBuilder(
        mock(SearchClientShim.class),
        config,
        mock(StructuredPropertiesConfiguration.class),
        Map.of(),
        mock(GitVersion.class));
  }

  /**
   * Regression for the AI-smoke system-update failure: target mappings supplied by
   * V2SemanticSearchMappingsBuilder are ImmutableMaps at every level, and the structured-property
   * merge writes into the target map, its "properties" entry, and the structuredProperties entry
   * beneath it. Merging into the caller's maps threw a message-less UnsupportedOperationException,
   * which BuildIndicesIncrementalStep swallowed into "No index builder found". The merge must
   * operate on a mutable copy and leave the caller's maps untouched.
   */
  @Test
  public void testMergeIntoImmutableTargetMappings() {
    Map<String, Object> immutableTarget =
        ImmutableMap.of(
            "properties",
            ImmutableMap.of(
                "urn",
                ImmutableMap.of("type", "keyword"),
                "structuredProperties",
                ImmutableMap.of("type", "object", "dynamic", "false")));

    Map<String, Object> currentMappings =
        ImmutableMap.of(
            "properties",
            ImmutableMap.of(
                "structuredProperties",
                ImmutableMap.of(
                    "type",
                    "object",
                    "dynamic",
                    "false",
                    "properties",
                    ImmutableMap.of("propertyA", ImmutableMap.of("type", "keyword")))));

    Map<String, Object> copied = ESIndexBuilder.copyForStructuredPropertyMerge(immutableTarget);
    builder().mergeStructuredPropertyMappings(copied, currentMappings);

    @SuppressWarnings("unchecked")
    Map<String, Object> mergedProperties = (Map<String, Object>) copied.get("properties");
    @SuppressWarnings("unchecked")
    Map<String, Object> mergedStructuredProperties =
        (Map<String, Object>) mergedProperties.get("structuredProperties");
    @SuppressWarnings("unchecked")
    Map<String, Object> mergedSpFields =
        (Map<String, Object>) mergedStructuredProperties.get("properties");
    assertTrue(mergedSpFields.containsKey("propertyA"), "current SP fields must be merged in");
    assertEquals(mergedProperties.containsKey("urn"), true, "non-SP target fields preserved");

    // The caller's immutable maps must be untouched.
    @SuppressWarnings("unchecked")
    Map<String, Object> originalSp =
        (Map<String, Object>)
            ((Map<String, Object>) immutableTarget.get("properties")).get("structuredProperties");
    assertFalse(originalSp.containsKey("properties"), "caller's maps must not be mutated");
  }

  /** A target with no properties entry at all must still merge without mutation errors. */
  @Test
  public void testMergeIntoImmutableTargetWithoutPropertiesEntry() {
    Map<String, Object> immutableTarget = ImmutableMap.of("dynamic", "false");
    Map<String, Object> currentMappings =
        ImmutableMap.of(
            "properties",
            ImmutableMap.of(
                "structuredProperties",
                ImmutableMap.of(
                    "properties",
                    ImmutableMap.of("propertyB", ImmutableMap.of("type", "keyword")))));

    Map<String, Object> copied = ESIndexBuilder.copyForStructuredPropertyMerge(immutableTarget);
    // Must not throw; merging into a properties-less target is otherwise a no-op today
    // (the merge writes into a detached map), so only the no-mutation contract is asserted.
    builder().mergeStructuredPropertyMappings(copied, currentMappings);
    assertFalse(immutableTarget.containsKey("properties"));
  }
}
