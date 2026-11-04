package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.resolvers.knowledge.RelatedDocumentsResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityLineageResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityRelationshipsResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import graphql.schema.DataFetcher;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Entity fields that are declared in the schema must resolve through the shared resolvers rather
 * than graphql-java's default property fetcher, which always answers null for them.
 */
public class GmsGraphQLEngineWiringTest {

  private GraphQLSchema schema;

  @BeforeClass
  public void buildSchema() throws Exception {
    schema =
        MockedGmsGraphQLEngine.schema(
            getMockAllowContext().getOperationContext().getEntityRegistry());
  }

  @DataProvider
  public Object[][] relationshipsTypes() {
    return GmsGraphQLEngine.GENERIC_RELATIONSHIPS_TYPES.stream()
        .map(type -> new Object[] {type})
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "relationshipsTypes")
  public void testRelationshipsIsWired(String type) {
    assertFetcher(type, "relationships", EntityRelationshipsResultResolver.class);
  }

  @Test
  public void testLineageIsWired() {
    assertFetcher("Assertion", "lineage", EntityLineageResultResolver.class);
    assertFetcher("ERModelRelationship", "lineage", EntityLineageResultResolver.class);
  }

  @Test
  public void testDataHubRoleAspectsIsWired() {
    assertFetcher("DataHubRole", "aspects", WeaklyTypedAspectsResolver.class);
  }

  @Test
  public void testSchemaFieldRelatedDocumentsIsWired() {
    assertFetcher("SchemaFieldEntity", "relatedDocuments", RelatedDocumentsResolver.class);
  }

  @Test
  public void testGenericRelationshipsKeepTheTypesOtherFetchers() {
    // relationships is added alongside each type's own wiring, not in place of it.
    assertFetcher("AccessTokenMetadata", "owner", LoadableTypeResolver.class);
    assertFetcher("DataPlatformInstance", "platform", LoadableTypeResolver.class);
    assertFetcher("ExecutionRequest", "source", LoadableTypeResolver.class);
  }

  private void assertFetcher(String type, String field, Class<?> expected) {
    GraphQLObjectType objectType = schema.getObjectType(type);
    assertTrue(objectType != null, type + " is not in the schema");
    assertTrue(
        objectType.getFieldDefinition(field) != null, type + "." + field + " is not declared");
    DataFetcher<?> fetcher =
        schema
            .getCodeRegistry()
            .getDataFetcher(
                FieldCoordinates.coordinates(type, field), objectType.getFieldDefinition(field));
    assertTrue(
        expected.isInstance(fetcher),
        type + "." + field + " resolves with " + fetcher.getClass().getName());
  }
}
