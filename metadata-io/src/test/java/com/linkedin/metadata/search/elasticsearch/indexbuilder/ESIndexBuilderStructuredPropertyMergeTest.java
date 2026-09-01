package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.Test;

public class ESIndexBuilderStructuredPropertyMergeTest {

  private static final String INDEX = "documentindex_v2_semantic";

  /**
   * Regression for the AI-smoke system-update failure: target mappings supplied by
   * V2SemanticSearchMappingsBuilder are ImmutableMaps at every level, and with structured-property
   * system-update disabled (the default) buildReindexState merges the current index's
   * structured-property mappings into the target on every pass. Merging into the caller's maps
   * threw a message-less UnsupportedOperationException, which BuildIndicesIncrementalStep swallowed
   * into "No index builder found". Drives the real buildReindexState path with an immutable target
   * against an existing index whose mappings carry structured properties.
   */
  @Test
  public void testBuildReindexStateMergesIntoImmutableTargetMappings() throws Exception {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();

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

    SearchClientShim<?> searchClient = mock(SearchClientShim.class, RETURNS_DEEP_STUBS);
    when(searchClient.indexExists(any(), any(), any())).thenReturn(true);
    when(searchClient.getIndexSettings(any(), any(), any()).getIndexToSettings())
        .thenReturn(Map.of(INDEX, Settings.builder().build()));
    MappingMetadata mappingMetadata = mock(MappingMetadata.class);
    when(mappingMetadata.getSourceAsMap()).thenReturn(currentMappings);
    when(searchClient.getIndexMapping(any(), any(), any()).mappings())
        .thenReturn(Map.of(INDEX, mappingMetadata));

    ElasticSearchConfiguration config = mock(ElasticSearchConfiguration.class, RETURNS_DEEP_STUBS);
    when(config.getBuildIndices().getSlowOperationTimeoutSeconds()).thenReturn(10);
    GitVersion gitVersion = mock(GitVersion.class);
    when(gitVersion.getVersion()).thenReturn("test");
    // Default mock booleans are false: structured-property system-update disabled, so
    // shouldPreserveStructuredPropertyMappings(false) is true — the failing configuration.
    ESIndexBuilder builder =
        spy(
            new ESIndexBuilder(
                searchClient,
                config,
                mock(StructuredPropertiesConfiguration.class),
                Map.of(),
                gitVersion));
    doReturn(false).when(builder).isOpenSearch29OrHigher(any());

    // The semantic target: immutable at every level the merge writes to.
    Map<String, Object> immutableTarget =
        ImmutableMap.of(
            "properties",
            ImmutableMap.of(
                "urn",
                ImmutableMap.of("type", "keyword"),
                "structuredProperties",
                ImmutableMap.of("type", "object", "dynamic", "false")));

    ReindexConfig result =
        builder.buildReindexState(opContext, INDEX, immutableTarget, ImmutableMap.of(), false);

    @SuppressWarnings("unchecked")
    Map<String, Object> targetProperties =
        (Map<String, Object>) result.targetMappings().get("properties");
    @SuppressWarnings("unchecked")
    Map<String, Object> targetStructuredProperties =
        (Map<String, Object>) targetProperties.get("structuredProperties");
    @SuppressWarnings("unchecked")
    Map<String, Object> mergedSpFields =
        (Map<String, Object>) targetStructuredProperties.get("properties");
    assertTrue(
        mergedSpFields.containsKey("propertyA"),
        "current structured-property fields must be merged into the target");
    assertTrue(targetProperties.containsKey("urn"), "non-SP target fields preserved");

    // The caller's immutable maps must be untouched.
    @SuppressWarnings("unchecked")
    Map<String, Object> originalSp =
        (Map<String, Object>)
            ((Map<String, Object>) immutableTarget.get("properties")).get("structuredProperties");
    assertFalse(originalSp.containsKey("properties"), "caller's maps must not be mutated");
  }
}
