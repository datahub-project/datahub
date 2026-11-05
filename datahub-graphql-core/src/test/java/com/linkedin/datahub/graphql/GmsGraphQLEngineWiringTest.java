package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityRelationshipsResult;
import com.linkedin.datahub.graphql.generated.RelationshipDirection;
import com.linkedin.datahub.graphql.generated.RelationshipsInput;
import com.linkedin.datahub.graphql.resolvers.knowledge.RelatedDocumentsResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityLineageResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityRelationshipsResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Entity fields that are declared in the schema must resolve through the shared resolvers rather
 * than graphql-java's default property fetcher, which always answers null for them.
 */
public class GmsGraphQLEngineWiringTest {

  private static final Urn LIVE_NEIGHBOR = UrnUtils.getUrn("urn:li:corpuser:live");
  private static final Urn DELETED_NEIGHBOR = UrnUtils.getUrn("urn:li:corpuser:deleted");

  private final GraphClient graphClient = mock(GraphClient.class);
  private final EntityService<?> entityService = mock(EntityService.class);
  private GraphQLSchema schema;

  @BeforeClass
  public void buildSchema() throws Exception {
    schema =
        MockedGmsGraphQLEngine.schema(
            getMockAllowContext().getOperationContext().getEntityRegistry(),
            args -> {
              args.setGraphClient(graphClient);
              args.setEntityService(entityService);
            });
    when(graphClient.getRelatedEntities(any(), any(), any(), any(), any(), any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(2)
                .setTotal(2)
                .setRelationships(
                    new EntityRelationshipArray(
                        new EntityRelationship().setEntity(LIVE_NEIGHBOR).setType("SomeType"),
                        new EntityRelationship().setEntity(DELETED_NEIGHBOR).setType("SomeType"))));
    when(entityService.exists(any(), eq(Set.of(LIVE_NEIGHBOR, DELETED_NEIGHBOR)), eq(false)))
        .thenReturn(Set.of(LIVE_NEIGHBOR));
  }

  /**
   * Listed here rather than read from GmsGraphQLEngine.GENERIC_RELATIONSHIPS_TYPES, so dropping a
   * type from the engine's list fails the test instead of shrinking it.
   */
  @DataProvider
  public Object[][] relationshipsTypes() {
    return Stream.of(
            "AccessTokenMetadata",
            "BusinessAttribute",
            "DataContract",
            "DataHubConnection",
            "DataHubPageModule",
            "DataHubPageTemplate",
            "DataHubPolicy",
            "DataPlatform",
            "DataPlatformInstance",
            "DataTypeEntity",
            "EntityTypeEntity",
            "ExecutionRequest",
            "Form",
            "Post",
            "StructuredPropertyEntity",
            "Test",
            "VersionSet")
        .map(type -> new Object[] {type})
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "relationshipsTypes")
  public void testRelationshipsIsWired(String type) {
    assertFetcher(type, "relationships", EntityRelationshipsResultResolver.class);
  }

  @Test(dataProvider = "relationshipsTypes")
  public void testRelationshipsDropsDeletedNeighbors(String type) throws Exception {
    RelationshipsInput input = new RelationshipsInput();
    input.setTypes(List.of("SomeType"));
    input.setDirection(RelationshipDirection.OUTGOING);
    input.setStart(0);
    input.setCount(10);
    input.setIncludeSoftDelete(false);
    Entity source = mock(Entity.class);
    when(source.getUrn()).thenReturn("urn:li:post:source");
    QueryContext context = getMockAllowContext();
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getSource()).thenReturn(source);
    when(env.getArgument("input")).thenReturn(input);

    EntityRelationshipsResult result =
        ((CompletableFuture<EntityRelationshipsResult>) fetcher(type, "relationships").get(env))
            .get();

    assertEquals(
        result.getRelationships().stream()
            .map(relationship -> relationship.getEntity().getUrn())
            .toList(),
        List.of(LIVE_NEIGHBOR.toString()),
        type + ".relationships kept a deleted neighbor");
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
    DataFetcher<?> fetcher = fetcher(type, field);
    assertTrue(
        expected.isInstance(fetcher),
        type + "." + field + " resolves with " + fetcher.getClass().getName());
  }

  private DataFetcher<?> fetcher(String type, String field) {
    GraphQLObjectType objectType = schema.getObjectType(type);
    assertTrue(objectType != null, type + " is not in the schema");
    assertTrue(
        objectType.getFieldDefinition(field) != null, type + "." + field + " is not declared");
    return schema
        .getCodeRegistry()
        .getDataFetcher(
            FieldCoordinates.coordinates(type, field), objectType.getFieldDefinition(field));
  }
}
