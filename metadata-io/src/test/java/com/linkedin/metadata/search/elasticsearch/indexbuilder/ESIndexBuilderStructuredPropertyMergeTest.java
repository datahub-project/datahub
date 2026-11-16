package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.Test;

public class ESIndexBuilderStructuredPropertyMergeTest {

  private static final String INDEX = "documentindex_v2_semantic";

  /**
   * Spy builder whose mocked search client reports {@code INDEX} existing with the given current
   * mappings. Default mock booleans make shouldPreserveStructuredPropertyMappings(false) return
   * true, selecting the live-mapping merge fallback — the configuration that crashed.
   */
  private static ESIndexBuilder builderForExistingIndex(Map<String, Object> currentMappings)
      throws Exception {
    return builderForExistingIndex(currentMappings, mock(StructuredPropertiesConfiguration.class));
  }

  private static ESIndexBuilder builderForExistingIndex(
      Map<String, Object> currentMappings, StructuredPropertiesConfiguration structPropConfig)
      throws Exception {
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
    when(config.getIndex().isEnableMappingsReindex()).thenReturn(true);
    GitVersion gitVersion = mock(GitVersion.class);
    when(gitVersion.getVersion()).thenReturn("test");
    ESIndexBuilder builder =
        spy(new ESIndexBuilder(searchClient, config, structPropConfig, Map.of(), gitVersion));
    doReturn(false).when(builder).isOpenSearch29OrHigher(any());
    return builder;
  }

  /**
   * Regression for the AI-smoke system-update failure: target mappings supplied by
   * V2SemanticSearchMappingsBuilder are ImmutableMaps at every level, and buildReindexState merges
   * the current index's structured-property mappings into the target when structured properties are
   * disabled (see shouldPreserveStructuredPropertyMappings; the mock configuration reports them
   * disabled). Merging into the caller's maps threw a message-less UnsupportedOperationException,
   * which BuildIndicesIncrementalStep swallowed into "No index builder found". Drives the real
   * buildReindexState path with an immutable target against an existing index whose mappings carry
   * structured properties.
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

    ESIndexBuilder builder = builderForExistingIndex(currentMappings);

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

  /**
   * Companion to the above, where the target already carries its own structured-property field
   * definitions. The target's structuredProperties.properties (the L4 field map) is itself a
   * non-empty ImmutableMap, so this exercises mergeStructuredProperties' copy-then-replace of that
   * level: it must union the current and target fields without mutating the caller's L4 in place.
   */
  @Test
  public void testBuildReindexStateMergesWhenTargetHasImmutableStructuredPropertyFields()
      throws Exception {
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
                    ImmutableMap.of("propertyFromCurrent", ImmutableMap.of("type", "keyword")))));

    ESIndexBuilder builder = builderForExistingIndex(currentMappings);

    // Target whose structuredProperties.properties (L4) is a non-empty ImmutableMap.
    Map<String, Object> immutableTarget =
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
                    ImmutableMap.of("propertyFromTarget", ImmutableMap.of("type", "keyword")))));

    ReindexConfig result =
        builder.buildReindexState(opContext, INDEX, immutableTarget, ImmutableMap.of(), false);

    @SuppressWarnings("unchecked")
    Map<String, Object> mergedSpFields =
        (Map<String, Object>)
            ((Map<String, Object>)
                    ((Map<String, Object>) result.targetMappings().get("properties"))
                        .get("structuredProperties"))
                .get("properties");
    assertTrue(mergedSpFields.containsKey("propertyFromTarget"), "target's own SP field preserved");
    assertTrue(mergedSpFields.containsKey("propertyFromCurrent"), "current SP field merged in");

    // The caller's immutable L4 must be untouched — still just its one original field.
    @SuppressWarnings("unchecked")
    Map<String, Object> originalL4 =
        (Map<String, Object>)
            ((Map<String, Object>)
                    ((Map<String, Object>) immutableTarget.get("properties"))
                        .get("structuredProperties"))
                .get("properties");
    assertEquals(
        originalL4.size(), 1, "caller's structuredProperties.properties must not be mutated");
    assertTrue(originalL4.containsKey("propertyFromTarget"));
  }

  /**
   * A target with no root "properties" entry at all still receives the merge. Before the merge
   * returned its own copy, this case wrote the current structured properties into a detached map
   * and silently discarded them.
   */
  @Test
  public void testBuildReindexStateMergesIntoTargetWithoutPropertiesEntry() throws Exception {
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

    ESIndexBuilder builder = builderForExistingIndex(currentMappings);

    Map<String, Object> immutableTarget = ImmutableMap.of();

    ReindexConfig result =
        builder.buildReindexState(opContext, INDEX, immutableTarget, ImmutableMap.of(), false);

    @SuppressWarnings("unchecked")
    Map<String, Object> mergedSpFields =
        (Map<String, Object>)
            ((Map<String, Object>)
                    ((Map<String, Object>) result.targetMappings().get("properties"))
                        .get("structuredProperties"))
                .get("properties");
    assertTrue(
        mergedSpFields.containsKey("propertyA"),
        "merge must land on a target without a properties entry");
    assertTrue(immutableTarget.isEmpty(), "caller's map untouched");
  }

  /**
   * With structured properties enabled and the system-update reindex gate off (the defaults), the
   * target comes from the definitions: a field the live index holds without a definition is left
   * out, a dynamically mapped field takes its definition's type, a definition the index lacks is
   * added, and no difference triggers a reindex, even with mappings reindex on.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testBuildReindexStateTargetsDefinitionsWhenEnabled() throws Exception {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Map<String, Object> currentMappings =
        Map.of(
            "properties",
            Map.of(
                "structuredProperties",
                Map.of(
                    "dynamic",
                    "false",
                    "properties",
                    Map.of(
                        "stale", Map.of("type", "keyword"),
                        "propertyA", Map.of("type", "text")))));
    ESIndexBuilder builder =
        builderForExistingIndex(
            currentMappings,
            StructuredPropertiesConfiguration.builder()
                .enabled(true)
                .systemUpdateEnabled(false)
                .typeMismatchReindexEnabled(true)
                .build());
    Map<String, Object> definitionTarget =
        Map.of(
            "properties",
            Map.of(
                "structuredProperties",
                Map.of(
                    "type",
                    "object",
                    "dynamic",
                    false,
                    "properties",
                    Map.of(
                        "propertyA",
                        Map.of("type", "keyword"),
                        "propertyB",
                        Map.of("type", "double")))));

    ReindexConfig result =
        builder.buildReindexState(opContext, INDEX, definitionTarget, Map.of(), false);

    Map<String, Object> targetFields =
        (Map<String, Object>)
            ((Map<String, Object>)
                    ((Map<String, Object>) result.targetMappings().get("properties"))
                        .get("structuredProperties"))
                .get("properties");
    assertEquals(
        targetFields,
        Map.of("propertyA", Map.of("type", "keyword"), "propertyB", Map.of("type", "double")));
    assertTrue(result.hasNewStructuredProperty());
    assertTrue(result.hasRemovedStructuredProperty());
    assertTrue(result.hasStructuredPropertyTypeMismatch());
    assertFalse(result.requiresReindex());
  }

  private static final Map<String, Object> KEYWORD = Map.of("type", "keyword");

  private static Map<String, Object> structuredPropertyMappings(Map<String, Object> fields) {
    return Map.of(
        "properties",
        Map.of(
            "structuredProperties",
            Map.of("type", "object", "dynamic", false, "properties", fields)));
  }

  /** A reindex whose target and live index declare the given structured property fields. */
  private static ReindexConfig reindexState(
      Map<String, Object> targetFields, Map<String, Object> liveFields) {
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.name()).thenReturn(INDEX);
    when(indexState.targetMappings()).thenReturn(structuredPropertyMappings(targetFields));
    when(indexState.currentMappings()).thenReturn(structuredPropertyMappings(liveFields));
    when(indexState.targetSettings()).thenReturn(Map.of("index", Map.of("number_of_shards", 1)));
    return indexState;
  }

  private static OpenSearchStatusException totalFieldsRejection() {
    return new OpenSearchStatusException(
        "OpenSearch exception [type=illegal_argument_exception, reason=Limit of total fields [3]"
            + " has been exceeded]",
        RestStatus.BAD_REQUEST);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRejectedReindexDestinationTakesTheLiveStructuredPropertyFields()
      throws Exception {
    ESIndexBuilder builder = builderForExistingIndex(Map.of());
    SearchClientShim<?> searchClient = builder.getSearchClient();
    when(searchClient.createIndex(any(), any(), any()))
        .thenThrow(totalFieldsRejection())
        .thenReturn(null);

    builder.createReindexDestination(
        TestOperationContexts.systemContextNoSearchAuthorization(),
        "destination",
        reindexState(Map.of("propLive", KEYWORD, "propA", KEYWORD), Map.of("propLive", KEYWORD)));

    ArgumentCaptor<CreateIndexRequest> requests = ArgumentCaptor.forClass(CreateIndexRequest.class);
    verify(searchClient, times(2)).createIndex(any(), requests.capture(), any());
    Map<String, Object> retried =
        ReindexConfig.OBJECT_MAPPER.readValue(
            requests.getAllValues().get(1).mappings().utf8ToString(), Map.class);
    Map<String, Object> container =
        (Map<String, Object>)
            ((Map<String, Object>) retried.get("properties")).get("structuredProperties");
    assertEquals(((Map<String, Object>) container.get("properties")).keySet(), Set.of("propLive"));
    assertEquals(container.get("dynamic"), false);
  }

  @Test
  public void testRejectedReindexDestinationFailsWhenStructuredPropertyDifferencesReindex()
      throws Exception {
    ESIndexBuilder builder = builderForExistingIndex(Map.of());
    SearchClientShim<?> searchClient = builder.getSearchClient();
    OpenSearchStatusException rejection = totalFieldsRejection();
    when(searchClient.createIndex(any(), any(), any())).thenThrow(rejection);
    ReindexConfig indexState =
        reindexState(Map.of("propA", KEYWORD), Map.of("propRemoved", KEYWORD));
    // The live fields would keep propRemoved, which starts the same reindex on every run.
    when(indexState.enableIndexMappingsReindex()).thenReturn(true);
    when(indexState.enableStructuredPropertiesReindex()).thenReturn(true);
    when(indexState.hasRemovedStructuredProperty()).thenReturn(true);

    OpenSearchStatusException thrown =
        expectThrows(
            OpenSearchStatusException.class,
            () ->
                builder.createReindexDestination(
                    TestOperationContexts.systemContextNoSearchAuthorization(),
                    "destination",
                    indexState));
    assertSame(thrown, rejection);
    verify(searchClient, times(1)).createIndex(any(), any(), any());
  }

  @Test
  public void testRejectedReindexDestinationFallsBackWhenNoLiveDifferenceStartsAReindex()
      throws Exception {
    ESIndexBuilder builder = builderForExistingIndex(Map.of());
    SearchClientShim<?> searchClient = builder.getSearchClient();
    when(searchClient.createIndex(any(), any(), any()))
        .thenThrow(totalFieldsRejection())
        .thenReturn(null);
    // Structured property reindexing is on, but the live fields only lack propA.
    ReindexConfig indexState = reindexState(Map.of("propA", KEYWORD), Map.of());
    when(indexState.enableIndexMappingsReindex()).thenReturn(true);
    when(indexState.enableStructuredPropertiesReindex()).thenReturn(true);

    builder.createReindexDestination(
        TestOperationContexts.systemContextNoSearchAuthorization(), "destination", indexState);

    verify(searchClient, times(2)).createIndex(any(), any(), any());
  }

  @Test
  public void testRejectedReindexDestinationRetryFailureKeepsTheFirstRejection() throws Exception {
    ESIndexBuilder builder = builderForExistingIndex(Map.of());
    SearchClientShim<?> searchClient = builder.getSearchClient();
    OpenSearchStatusException first = totalFieldsRejection();
    OpenSearchStatusException second = totalFieldsRejection();
    when(searchClient.createIndex(any(), any(), any())).thenThrow(first, second);

    OpenSearchStatusException thrown =
        expectThrows(
            OpenSearchStatusException.class,
            () ->
                builder.createReindexDestination(
                    TestOperationContexts.systemContextNoSearchAuthorization(),
                    "destination",
                    reindexState(Map.of("propA", KEYWORD), Map.of())));
    assertSame(thrown, second);
    assertEquals(List.of(second.getSuppressed()), List.of(first));
    verify(searchClient, times(2)).createIndex(any(), any(), any());
  }

  @Test
  public void testRejectedReindexDestinationWithNothingToLeaveOutPropagates() throws Exception {
    ESIndexBuilder builder = builderForExistingIndex(Map.of());
    SearchClientShim<?> searchClient = builder.getSearchClient();
    OpenSearchStatusException rejection = totalFieldsRejection();
    when(searchClient.createIndex(any(), any(), any())).thenThrow(rejection);

    OpenSearchStatusException thrown =
        expectThrows(
            OpenSearchStatusException.class,
            () ->
                builder.createReindexDestination(
                    TestOperationContexts.systemContextNoSearchAuthorization(),
                    "destination",
                    reindexState(Map.of("propLive", KEYWORD), Map.of("propLive", KEYWORD))));
    assertSame(thrown, rejection);
    verify(searchClient, times(1)).createIndex(any(), any(), any());
  }

  @Test
  public void testIncrementalReindexCreatesItsDestinationWithTheFallback() throws Throwable {
    ESIndexBuilder builder = builderForExistingIndex(Map.of());
    ReindexConfig indexState = reindexState(Map.of(), Map.of());
    doNothing().when(builder).createReindexDestination(any(), any(), any());

    builder.buildIndexIncremental(
        TestOperationContexts.systemContextNoSearchAuthorization(), indexState, "0.0.1-0");

    verify(builder).createReindexDestination(any(), any(), eq(indexState));
  }
}
