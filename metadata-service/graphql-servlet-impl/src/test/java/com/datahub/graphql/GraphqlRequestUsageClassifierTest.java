package com.datahub.graphql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.ratelimit.GraphqlDocumentAnalyzer;
import com.linkedin.metadata.ratelimit.GraphqlDocumentMetadata;
import com.linkedin.metadata.usage.registry.graphql.GraphqlUsageClassificationRegistryBuilder;
import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import io.datahubproject.metadata.context.usage.UsageOperation;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GraphqlRequestUsageClassifierTest {

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
  public void testTier2FastPathSkipsParseAndClassifiesSearch() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            "search",
            "query { dataset(urn: \"urn:li:dataset:(a,b,c)\") { urn } }",
            name -> registry.resolveByOperationName(name).isPresent());

    assertFalse(metadata.isParsed());
    GraphqlRequestUsageClassifier.Result result =
        GraphqlRequestUsageClassifier.classify(metadata, registry);
    assertEquals(result.usageOperation(), UsageOperation.SEARCH_QUERY);
    assertEquals(result.kind(), GraphQLOperationKind.QUERY);
  }

  @Test
  public void testTier2FastPathGetInviteTokenUsesMutationKind() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            "getInviteToken",
            "mutation getInviteToken { getInviteToken(input: {}) { token } }",
            name -> registry.resolveByOperationName(name).isPresent());

    assertFalse(metadata.isParsed());
    GraphqlRequestUsageClassifier.Result result =
        GraphqlRequestUsageClassifier.classify(metadata, registry);
    assertEquals(result.usageOperation(), UsageOperation.OTHER_WRITE);
    assertEquals(result.kind(), GraphQLOperationKind.MUTATION);
  }

  @Test
  public void testTier2FastPathCreateAccessTokenIsOtherOperations() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            "createAccessToken",
            "mutation createAccessToken($input: CreateAccessTokenInput!) {"
                + " createAccessToken(input: $input) { accessToken } }",
            name -> registry.resolveByOperationName(name).isPresent());

    assertFalse(metadata.isParsed());
    GraphqlRequestUsageClassifier.Result result =
        GraphqlRequestUsageClassifier.classify(metadata, registry);
    assertEquals(result.usageOperation(), UsageOperation.OTHER_OPERATIONS);
    assertEquals(result.kind(), GraphQLOperationKind.MUTATION);
  }

  @Test
  public void testTier2FastPathCreateViewIsOtherWrite() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            "createView",
            "mutation createView($input: CreateViewInput!) {"
                + " createView(input: $input) { urn } }",
            name -> registry.resolveByOperationName(name).isPresent());

    assertFalse(metadata.isParsed());
    GraphqlRequestUsageClassifier.Result result =
        GraphqlRequestUsageClassifier.classify(metadata, registry);
    assertEquals(result.usageOperation(), UsageOperation.OTHER_WRITE);
    assertEquals(result.kind(), GraphQLOperationKind.MUTATION);
  }

  @Test
  public void testTier2FastPathRevokeAccessTokenIsOtherOperations() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            "revokeAccessToken",
            "mutation revokeAccessToken($tokenId: String!) { revokeAccessToken(tokenId: $tokenId) }",
            name -> registry.resolveByOperationName(name).isPresent());

    assertFalse(metadata.isParsed());
    GraphqlRequestUsageClassifier.Result result =
        GraphqlRequestUsageClassifier.classify(metadata, registry);
    assertEquals(result.usageOperation(), UsageOperation.OTHER_OPERATIONS);
    assertEquals(result.kind(), GraphQLOperationKind.MUTATION);
  }

  @Test
  public void testUnmappedNamedOperationParsesAndClassifies() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            "getDataset",
            "query getDataset { dataset(urn: \"urn:li:dataset:(a,b,c)\") { urn } }",
            name -> registry.resolveByOperationName(name).isPresent());

    assertTrue(metadata.isParsed());
    GraphqlRequestUsageClassifier.Result result =
        GraphqlRequestUsageClassifier.classify(metadata, registry);
    assertEquals(result.usageOperation(), UsageOperation.METADATA_QUERY);
    assertEquals(result.kind(), GraphQLOperationKind.QUERY);
  }

  @Test
  public void testAnonymousSearchUsesParsedRoots() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(null, "{ search(input: {}) { total } }");

    assertTrue(metadata.isParsed());
    GraphqlRequestUsageClassifier.Result result =
        GraphqlRequestUsageClassifier.classify(metadata, registry);
    assertEquals(result.usageOperation(), UsageOperation.SEARCH_QUERY);
    assertEquals(result.kind(), GraphQLOperationKind.QUERY);
  }

  @Test
  public void testUnparsedDocumentUsesPrefixKindWhenOperationNameUnmapped() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentMetadata.unparsed(
            "zzzzUnmappedUsageOp98765",
            "zzzzUnmappedUsageOp98765",
            "mutation zzzzUnmappedUsageOp98765 { createDataset { urn } }");

    assertFalse(metadata.isParsed());
    GraphqlRequestUsageClassifier.Result result =
        GraphqlRequestUsageClassifier.classify(metadata, registry);
    assertEquals(result.kind(), GraphQLOperationKind.MUTATION);
    assertEquals(result.usageOperation(), UsageOperation.METADATA_WRITE);
  }

  @Test
  public void testParsedSubscriptionUsesSubscriptionKind() {
    GraphqlDocumentMetadata metadata =
        GraphqlDocumentAnalyzer.analyze(
            null, "subscription { datasetUpdates { urn } }", name -> false);

    assertTrue(metadata.isParsed());
    GraphqlRequestUsageClassifier.Result result =
        GraphqlRequestUsageClassifier.classify(metadata, registry);
    assertEquals(result.kind(), GraphQLOperationKind.SUBSCRIPTION);
  }
}
