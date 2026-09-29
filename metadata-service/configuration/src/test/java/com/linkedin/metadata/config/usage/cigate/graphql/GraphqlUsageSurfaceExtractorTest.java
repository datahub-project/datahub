package com.linkedin.metadata.config.usage.cigate.graphql;

import com.linkedin.metadata.config.usage.cigate.UsageRegistryRepoPaths;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GraphqlUsageSurfaceExtractorTest {

  @Test
  public void testExtractsRootFieldsAndClientOperations() throws Exception {
    Path repoRoot = UsageRegistryRepoPaths.repoRoot();
    GraphqlUsageSurface surface = GraphqlUsageSurfaceExtractor.extract(repoRoot);
    Assert.assertTrue(surface.queryRootFields().contains("dataset"));
    Assert.assertTrue(surface.queryRootFields().contains("search"));
    Assert.assertFalse(surface.clientOperationNames().isEmpty());
  }

  @Test
  public void testParsesInlineSchemaFixture() throws Exception {
    Path tempDir = Files.createTempDirectory("graphql-surface-test");
    Path schemaDir = tempDir.resolve("datahub-graphql-core/src/main/resources");
    Files.createDirectories(schemaDir);
    Files.writeString(
        schemaDir.resolve("fixture.graphql"),
        """
        type Query {
          sampleField: String
        }
        type Mutation {
          sampleMutation(input: String!): Boolean
        }
        """);
    Path clientDir = tempDir.resolve("datahub-web-react/src/app");
    Files.createDirectories(clientDir);
    Files.writeString(
        clientDir.resolve("fixture.graphql"),
        """
        query sampleClientOp {
          sampleField
        }
        """);

    GraphqlUsageSurface surface = GraphqlUsageSurfaceExtractor.extract(tempDir);
    Assert.assertEquals(surface.queryRootFields(), java.util.Set.of("sampleField"));
    Assert.assertEquals(surface.mutationRootFields(), java.util.Set.of("sampleMutation"));
    Assert.assertEquals(surface.clientOperationNames(), java.util.Set.of("sampleClientOp"));
  }
}
