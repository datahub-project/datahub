package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityRelationshipsResultResolverTest {
  private final Urn existentUser = Urn.createFromString("urn:li:corpuser:johndoe");
  private final Urn softDeletedUser = Urn.createFromString("urn:li:corpuser:deletedUser");

  private CorpUser existentEntity;
  private CorpUser softDeletedEntity;

  private EntityService _entityService;
  private GraphClient _graphClient;

  private EntityRelationshipsResultResolver resolver;
  private RelationshipsInput input;
  private DataFetchingEnvironment mockEnv;

  public EntityRelationshipsResultResolverTest() throws URISyntaxException {}

  @BeforeMethod
  public void setupTest() {
    _entityService = mock(EntityService.class);
    _graphClient = mock(GraphClient.class);
    resolver = new EntityRelationshipsResultResolver(_graphClient, _entityService);

    mockEnv = mock(DataFetchingEnvironment.class);
    QueryContext context = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(context);

    CorpGroup source = new CorpGroup();
    source.setUrn("urn:li:corpGroup:group1");
    when(mockEnv.getSource()).thenReturn(source);

    when(_entityService.exists(any(), eq(Set.of(existentUser, softDeletedUser)), eq(true)))
        .thenReturn(Set.of(existentUser, softDeletedUser));
    when(_entityService.exists(any(), eq(Set.of(existentUser, softDeletedUser)), eq(false)))
        .thenReturn(Set.of(existentUser));

    input = new RelationshipsInput();
    input.setStart(0);
    input.setCount(10);
    input.setDirection(RelationshipDirection.INCOMING);
    input.setTypes(List.of("SomeType"));

    EntityRelationships entityRelationships =
        new EntityRelationships()
            .setStart(0)
            .setCount(2)
            .setTotal(2)
            .setRelationships(
                new EntityRelationshipArray(
                    new EntityRelationship().setEntity(existentUser).setType("SomeType"),
                    new EntityRelationship().setEntity(softDeletedUser).setType("SomeType")));

    // always expected INCOMING, and "SomeType" in all tests
    when(_graphClient.getRelatedEntities(
            eq(source.getUrn()),
            eq(input.getTypes()),
            same(com.linkedin.metadata.query.filter.RelationshipDirection.INCOMING),
            eq(input.getStart()),
            eq(input.getCount()),
            any()))
        .thenReturn(entityRelationships);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    existentEntity = new CorpUser();
    existentEntity.setUrn(existentUser.toString());
    existentEntity.setType(EntityType.CORP_USER);

    softDeletedEntity = new CorpUser();
    softDeletedEntity.setUrn(softDeletedUser.toString());
    softDeletedEntity.setType(EntityType.CORP_USER);
  }

  @Test
  public void testIncludeSoftDeleted() throws ExecutionException, InterruptedException {
    EntityRelationshipsResult expected = new EntityRelationshipsResult();
    expected.setRelationships(
        List.of(resultRelationship(existentEntity), resultRelationship(softDeletedEntity)));
    expected.setStart(0);
    expected.setCount(2);
    expected.setTotal(2);
    assertEquals(resolver.get(mockEnv).get().toString(), expected.toString());
  }

  @Test
  public void testExcludeSoftDeleted() throws ExecutionException, InterruptedException {
    input.setIncludeSoftDelete(false);
    EntityRelationshipsResult expected = new EntityRelationshipsResult();
    expected.setRelationships(List.of(resultRelationship(existentEntity)));
    expected.setStart(0);
    expected.setCount(1);
    expected.setTotal(1);
    assertEquals(resolver.get(mockEnv).get().toString(), expected.toString());
  }

  private com.linkedin.datahub.graphql.generated.EntityRelationship resultRelationship(
      Entity entity) {
    return new com.linkedin.datahub.graphql.generated.EntityRelationship(
        "SomeType", RelationshipDirection.INCOMING, entity, null);
  }
}
