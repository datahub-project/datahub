package com.linkedin.metadata.usage.registry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.registry.graphql.GraphqlUsageClassificationRegistryBuilder;
import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.List;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GraphqlUsageClassificationRegistryTest {

  private static GraphqlUsageClassificationRegistry registry;

  @BeforeClass
  public void setUp() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    registry =
        GraphqlUsageClassificationRegistryBuilder.fromManifest(
            new UsageOperationsLoader(yamlMapper).loadBundled());
  }

  @Test
  public void testManifestBuildsWithoutDuplicateGraphqlMappings() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    GraphqlUsageClassificationRegistryBuilder.fromManifest(
        new UsageOperationsLoader(yamlMapper).loadBundled());
  }

  @Test
  public void testResolveByOperationName() {
    assertEquals(
        registry.resolveByOperationName("search").orElseThrow(), UsageOperation.SEARCH_QUERY);
    assertEquals(
        registry.resolveByOperationName("autocomplete").orElseThrow(), UsageOperation.SEARCH_QUERY);
    assertEquals(
        registry.resolveByOperationName("autoComplete").orElseThrow(), UsageOperation.SEARCH_QUERY);
    assertEquals(
        registry.resolveByOperationName("getAutoCompleteResults").orElseThrow(),
        UsageOperation.SEARCH_QUERY);
    assertEquals(
        registry.resolveByOperationName("getAutoCompleteMultipleResults").orElseThrow(),
        UsageOperation.SEARCH_QUERY);
    assertTrue(registry.resolveByOperationName("graphql").isEmpty());
    assertTrue(registry.resolveByOperationName("getDataset").isEmpty());
    assertEquals(
        registry.resolveByOperationName("createView").orElseThrow(), UsageOperation.OTHER_WRITE);
  }

  @Test
  public void testSmokeGetDatasetRootFieldIsMetadataQuery() {
    assertEquals(
        registry.resolve("smokeGetDataset", GraphQLOperationKind.QUERY, List.of("dataset")),
        UsageOperation.METADATA_QUERY);
  }

  @Test
  public void testGetDatasetSchemaRootFieldIsMetadataQuery() {
    assertEquals(
        registry.resolve("getDatasetSchema", GraphQLOperationKind.QUERY, List.of("dataset")),
        UsageOperation.METADATA_QUERY);
  }

  @Test
  public void testAnonymousDatasetRootFieldIsMetadataQuery() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.QUERY, List.of("dataset")),
        UsageOperation.METADATA_QUERY);
  }

  @Test
  public void testAnonymousSearchRootFieldIsSearchQuery() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.QUERY, List.of("search")),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testMeAndDatasetRootFieldsStayMetadataQuery() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.QUERY, List.of("me", "dataset")),
        UsageOperation.METADATA_QUERY);
  }

  @Test
  public void testMeAndSearchRootFieldsPreferSearchQuery() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.QUERY, List.of("me", "search")),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testSearchOperationNameWinsOverDatasetRootField() {
    assertEquals(
        registry.resolve("search", GraphQLOperationKind.QUERY, List.of("dataset")),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testGetDatasetOperationNameIsMetadataQuery() {
    assertEquals(
        registry.resolve("getDataset", GraphQLOperationKind.QUERY, List.of("dataset")),
        UsageOperation.METADATA_QUERY);
  }

  @Test
  public void testExplicitSearchOperationNameWithoutRootFields() {
    assertEquals(
        registry.resolve("search", GraphQLOperationKind.QUERY, List.of()),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testDeleteMutationRootFieldIsEntityDelete() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.MUTATION, List.of("deleteEntity")),
        UsageOperation.ENTITY_DELETE);
  }

  @Test
  public void testTwoArgOverloadDelegates() {
    assertEquals(
        registry.resolve("listPolicies", GraphQLOperationKind.QUERY), UsageOperation.OTHER_READ);
  }

  @Test
  public void testAnonymousGetInviteTokenRootFieldIsOtherWrite() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.MUTATION, List.of("getInviteToken")),
        UsageOperation.OTHER_WRITE);
  }

  @Test
  public void testCreateAccessTokenRootFieldIsOtherOperations() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.MUTATION, List.of("createAccessToken")),
        UsageOperation.OTHER_OPERATIONS);
  }

  @Test
  public void testRevokeAccessTokenRootFieldIsOtherOperations() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.MUTATION, List.of("revokeAccessToken")),
        UsageOperation.OTHER_OPERATIONS);
  }

  @Test
  public void testCreateViewRootFieldIsOtherWrite() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.MUTATION, List.of("createView")),
        UsageOperation.OTHER_WRITE);
  }

  @Test
  public void testDeleteViewRootFieldIsOtherWrite() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.MUTATION, List.of("deleteView")),
        UsageOperation.OTHER_WRITE);
  }

  @Test
  public void testCustomOperationNameWithSearchSubstringFallsThroughToRootFields() {
    assertEquals(
        registry.resolve("researchDashboard", GraphQLOperationKind.QUERY, List.of("dataset")),
        UsageOperation.METADATA_QUERY);
  }

  @Test
  public void testUnmappedNamedOperationFallsThroughToRootFields() {
    assertEquals(
        registry.resolve("customLineageHelper", GraphQLOperationKind.QUERY, List.of("search")),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testSearchConfigurationYamlOverrideBeatsSearchHeuristic() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.QUERY, List.of("searchConfiguration")),
        UsageOperation.OTHER_READ);
  }

  @Test
  public void testSearchHeuristicMatchesSemanticSearchRootField() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.QUERY, List.of("semanticSearch")),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testLineageHeuristicMatchesSearchAcrossLineage() {
    assertEquals(
        registry.resolve("graphql", GraphQLOperationKind.QUERY, List.of("searchAcrossLineage")),
        UsageOperation.LINEAGE_QUERY);
  }
}
