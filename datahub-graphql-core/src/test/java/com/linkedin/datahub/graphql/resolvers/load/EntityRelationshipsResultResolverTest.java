package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.context.RelationshipTraversalContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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
            eq(new HashSet<>(input.getTypes())),
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

  @Test
  public void testFilterByRelatedEntityTypesExcludesNonMatching()
      throws ExecutionException, InterruptedException {
    // The graph returns corpuser relationships; restricting to dataHubPolicy should drop them all.
    // This mirrors the role "policies" query, where IsAssociatedWithRole edges from non-policy
    // sources (e.g. global settings) must be excluded.
    input.setRelatedEntityTypes(List.of("dataHubPolicy"));
    EntityRelationshipsResult result = resolver.get(mockEnv).get();
    assertTrue(result.getRelationships().isEmpty());
    assertEquals(result.getCount().intValue(), 0);
    assertEquals(result.getTotal().intValue(), 0);
  }

  @Test
  public void testFilterByRelatedEntityTypesKeepsMatching()
      throws ExecutionException, InterruptedException {
    input.setRelatedEntityTypes(List.of("corpuser"));
    EntityRelationshipsResult result = resolver.get(mockEnv).get();
    assertEquals(result.getCount().intValue(), 2);
    assertEquals(result.getRelationships().size(), 2);
  }

  /**
   * Reproduces the Settings → Roles page crash. A role's {@code policies} are its incoming {@code
   * IsAssociatedWithRole} edges, but that relationship name is shared: it is emitted toward a role
   * by both policy actor filters ({@code DataHubPolicyInfo.actors}) and the global-settings
   * maintenance-window audience ({@code GlobalSettingsInfo.maintenanceWindow.audience}), since both
   * embed {@code DataHubActorFilter.roles}. So when an admin scopes a maintenance window to the
   * Admin role, that role's edge set legitimately contains a {@code globalSettings} source mixed in
   * with real {@code dataHubPolicy} sources. {@code UrnToEntityMapper} has no case for {@code
   * globalSettings}, so under the {@code ... on DataHubPolicy} fragment it resolves to a null
   * entity, and the frontend crashed dereferencing {@code .urn}. The {@code relatedEntityTypes:
   * ["dataHubPolicy"]} filter must drop the {@code globalSettings} edge server-side while keeping
   * the real policy.
   */
  @Test
  public void testRolePoliciesExcludeMaintenanceWindowGlobalSettingsEdge()
      throws ExecutionException, InterruptedException, URISyntaxException {
    final Urn roleUrn = Urn.createFromString("urn:li:dataHubRole:Admin");
    final Urn policyUrn = Urn.createFromString("urn:li:dataHubPolicy:test-policy");
    final Urn globalSettingsUrn = Urn.createFromString("urn:li:globalSettings:0");

    CorpGroup source = new CorpGroup();
    source.setUrn(roleUrn.toString());
    when(mockEnv.getSource()).thenReturn(source);

    EntityRelationships roleEdges =
        new EntityRelationships()
            .setStart(0)
            .setCount(2)
            .setTotal(2)
            .setRelationships(
                new EntityRelationshipArray(
                    new EntityRelationship().setEntity(policyUrn).setType("IsAssociatedWithRole"),
                    new EntityRelationship()
                        .setEntity(globalSettingsUrn)
                        .setType("IsAssociatedWithRole")));
    when(_graphClient.getRelatedEntities(eq(roleUrn.toString()), any(), any(), any(), any(), any()))
        .thenReturn(roleEdges);

    RelationshipsInput rolePoliciesInput = new RelationshipsInput();
    rolePoliciesInput.setStart(0);
    rolePoliciesInput.setCount(50);
    rolePoliciesInput.setDirection(RelationshipDirection.INCOMING);
    rolePoliciesInput.setTypes(List.of("IsAssociatedWithRole"));
    rolePoliciesInput.setRelatedEntityTypes(List.of("dataHubPolicy"));
    when(mockEnv.getArgument(eq("input"))).thenReturn(rolePoliciesInput);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getRelationships().size(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), policyUrn.toString());
    assertEquals(result.getCount().intValue(), 1);
    assertEquals(result.getTotal().intValue(), 1);
  }

  /**
   * Without the {@code relatedEntityTypes} filter, the {@code globalSettings} maintenance-window
   * edge survives into the result and resolves to a null entity (no {@code globalSettings} case in
   * {@code UrnToEntityMapper}) — the exact precondition that crashed the Roles page. This guards
   * against the filter being removed and the crash regressing.
   */
  @Test
  public void testRolePoliciesWithoutFilterLeaksNullGlobalSettingsEntity()
      throws ExecutionException, InterruptedException, URISyntaxException {
    final Urn roleUrn = Urn.createFromString("urn:li:dataHubRole:Admin");
    final Urn policyUrn = Urn.createFromString("urn:li:dataHubPolicy:test-policy");
    final Urn globalSettingsUrn = Urn.createFromString("urn:li:globalSettings:0");

    CorpGroup source = new CorpGroup();
    source.setUrn(roleUrn.toString());
    when(mockEnv.getSource()).thenReturn(source);

    EntityRelationships roleEdges =
        new EntityRelationships()
            .setStart(0)
            .setCount(2)
            .setTotal(2)
            .setRelationships(
                new EntityRelationshipArray(
                    new EntityRelationship().setEntity(policyUrn).setType("IsAssociatedWithRole"),
                    new EntityRelationship()
                        .setEntity(globalSettingsUrn)
                        .setType("IsAssociatedWithRole")));
    when(_graphClient.getRelatedEntities(eq(roleUrn.toString()), any(), any(), any(), any(), any()))
        .thenReturn(roleEdges);

    RelationshipsInput unfilteredInput = new RelationshipsInput();
    unfilteredInput.setStart(0);
    unfilteredInput.setCount(50);
    unfilteredInput.setDirection(RelationshipDirection.INCOMING);
    unfilteredInput.setTypes(List.of("IsAssociatedWithRole"));
    when(mockEnv.getArgument(eq("input"))).thenReturn(unfilteredInput);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getRelationships().size(), 2);
    assertTrue(
        result.getRelationships().stream().anyMatch(rel -> rel.getEntity() == null),
        "globalSettings edge should resolve to a null entity without the filter");
  }

  @Test
  public void testResolverWithGraphClientOnlyUsesNullEntityService()
      throws ExecutionException, InterruptedException {
    EntityRelationshipsResultResolver resolverGraphOnly =
        new EntityRelationshipsResultResolver(_graphClient);
    EntityRelationshipsResult result = resolverGraphOnly.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.getCount(), 2);
    assertEquals(result.getRelationships().size(), 2);
  }

  @Test
  public void testRelationshipTraversalContextTryVisitAndIsVisited() {
    RelationshipTraversalContext ctx = new RelationshipTraversalContext(20_000);
    String urn = "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)";
    assertTrue(ctx.tryVisit(urn));
    assertTrue(ctx.isVisited(urn));
    // Same URN again: still proceed (multiple relationship types for one entity in one request)
    assertTrue(ctx.tryVisit(urn));
    assertFalse(ctx.isVisited("urn:li:other:xyz"));
  }

  @Test
  public void testRelationshipTraversalContextAtCapShortCircuits() {
    RelationshipTraversalContext ctx = new RelationshipTraversalContext(1);
    assertTrue(ctx.tryVisit("urn:li:dataset:one"));
    assertFalse(ctx.tryVisit("urn:li:dataset:two"));
  }

  @Test
  public void testNullContextReturnsEmptyAndDoesNotCallGraphClient()
      throws ExecutionException, InterruptedException {
    when(mockEnv.getContext()).thenReturn(null);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertTrue(result.getRelationships() == null || result.getRelationships().isEmpty());
    assertEquals(result.getCount(), 0);
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testFirstVisitWithTraversalContextPresentProceedsAndCallsGraphClient()
      throws ExecutionException, InterruptedException {
    RelationshipTraversalContext traversalContext = new RelationshipTraversalContext(20_000);
    QueryContext queryContext = getMockAllowContext();
    when(queryContext.getRelationshipTraversalContext()).thenReturn(Optional.of(traversalContext));
    when(mockEnv.getContext()).thenReturn(queryContext);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.getCount(), 2);
    verify(_graphClient).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testMapEntityRelationshipWithCreatedStamp()
      throws ExecutionException, InterruptedException {
    com.linkedin.common.AuditStamp created =
        new com.linkedin.common.AuditStamp().setTime(12345L).setActor(existentUser);
    EntityRelationship withCreated =
        new EntityRelationship().setEntity(existentUser).setType("SomeType").setCreated(created);
    EntityRelationships entityRelationships =
        new EntityRelationships()
            .setStart(0)
            .setCount(1)
            .setTotal(1)
            .setRelationships(new EntityRelationshipArray(withCreated));
    when(_graphClient.getRelatedEntities(any(), any(), any(), any(), any(), any()))
        .thenReturn(entityRelationships);
    when(_entityService.exists(any(), eq(Set.of(existentUser)), eq(false)))
        .thenReturn(Set.of(existentUser));
    when(_entityService.exists(any(), eq(Set.of(existentUser)), eq(true)))
        .thenReturn(Set.of(existentUser));

    EntityRelationshipsResult result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals(result.getRelationships().size(), 1);
    assertNotNull(result.getRelationships().get(0).getCreated());
  }

  /**
   * When the traversal context is at its URN cap, the resolver must return empty and must not call
   * the graph client. This prevents OOM on very large or cyclic graphs.
   */
  @Test
  public void testAtCapReturnsEmptyAndDoesNotCallGraphClient()
      throws ExecutionException, InterruptedException {
    String sourceUrn = "urn:li:glossaryNode:0fdc52a7d97255a4568a3255ad7e8414";
    CorpGroup source = new CorpGroup();
    source.setUrn(sourceUrn);
    when(mockEnv.getSource()).thenReturn(source);

    RelationshipTraversalContext traversalContext = new RelationshipTraversalContext(1);
    traversalContext.tryVisit("urn:li:other:already-visited");

    QueryContext queryContext = getMockAllowContext();
    when(queryContext.getRelationshipTraversalContext()).thenReturn(Optional.of(traversalContext));
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput cycleInput = new RelationshipsInput();
    cycleInput.setTypes(List.of("IsPartOf"));
    cycleInput.setDirection(RelationshipDirection.INCOMING);
    cycleInput.setCount(500);
    when(mockEnv.getArgument(eq("input"))).thenReturn(cycleInput);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertTrue(result.getRelationships() == null || result.getRelationships().isEmpty());
    assertEquals(result.getCount(), 0);
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  /**
   * Same entity can have multiple relationship types resolved in one request (e.g. ML model
   * features and trainedBy). Both resolutions must succeed and call the graph client.
   */
  @Test
  public void testSameUrnMultipleRelationshipTypesBothSucceed()
      throws ExecutionException, InterruptedException {
    String sourceUrn = "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,cypress-model,PROD)";
    CorpGroup source = new CorpGroup();
    source.setUrn(sourceUrn);
    when(mockEnv.getSource()).thenReturn(source);

    EntityRelationships twoRelationships =
        new EntityRelationships()
            .setStart(0)
            .setCount(2)
            .setTotal(2)
            .setRelationships(
                new EntityRelationshipArray(
                    new EntityRelationship().setEntity(existentUser).setType("SomeType"),
                    new EntityRelationship().setEntity(softDeletedUser).setType("SomeType")));
    when(_graphClient.getRelatedEntities(eq(sourceUrn), any(), any(), any(), any(), any()))
        .thenReturn(twoRelationships);

    RelationshipTraversalContext traversalContext = new RelationshipTraversalContext(20_000);
    QueryContext queryContext = getMockAllowContext();
    when(queryContext.getRelationshipTraversalContext()).thenReturn(Optional.of(traversalContext));
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput cycleInput = new RelationshipsInput();
    cycleInput.setTypes(List.of("IsPartOf"));
    cycleInput.setDirection(RelationshipDirection.INCOMING);
    cycleInput.setCount(500);
    when(mockEnv.getArgument(eq("input"))).thenReturn(cycleInput);

    EntityRelationshipsResult firstResult = resolver.get(mockEnv).get();
    assertNotNull(firstResult);
    assertEquals(firstResult.getCount(), 2);

    EntityRelationshipsResult secondResult = resolver.get(mockEnv).get();
    assertNotNull(secondResult);
    assertEquals(secondResult.getCount(), 2);

    verify(_graphClient, times(2)).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  private com.linkedin.datahub.graphql.generated.EntityRelationship resultRelationship(
      Entity entity) {
    return new com.linkedin.datahub.graphql.generated.EntityRelationship(
        "SomeType", RelationshipDirection.INCOMING, entity, null);
  }
}
