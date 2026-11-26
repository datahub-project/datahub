package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.withSessionActorIdentity;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.SessionActorIdentity;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.context.RelationshipTraversalContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
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
  public void testDocumentUsesSharedRelationshipLoadPath()
      throws ExecutionException, InterruptedException {
    Urn documentUrn = UrnUtils.getUrn("urn:li:document:restricted");
    Document source = new Document();
    source.setUrn(documentUrn.toString());
    when(mockEnv.getSource()).thenReturn(source);
    EntityRelationships emptyRelationships =
        new EntityRelationships()
            .setStart(0)
            .setCount(0)
            .setTotal(0)
            .setRelationships(new EntityRelationshipArray());
    when(_graphClient.getRelatedEntities(
            eq(documentUrn.toString()),
            eq(new HashSet<>(input.getTypes())),
            same(com.linkedin.metadata.query.filter.RelationshipDirection.INCOMING),
            eq(input.getStart()),
            eq(input.getCount()),
            any()))
        .thenReturn(emptyRelationships);

    assertTrue(resolver.get(mockEnv).get().getRelationships().isEmpty());
    verify(_graphClient)
        .getRelatedEntities(
            eq(documentUrn.toString()),
            eq(new HashSet<>(input.getTypes())),
            same(com.linkedin.metadata.query.filter.RelationshipDirection.INCOMING),
            eq(input.getStart()),
            eq(input.getCount()),
            any());
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

  // Callers pass entity types in the GraphQL enum form ("CORPUSER"/"DATASET"); the URN entity type
  // is lowercase ("corpuser"/"dataset"). The filter must match case-insensitively rather than drop
  // every edge and return an empty, correct-looking result.
  @Test
  public void testFilterByRelatedEntityTypesIsCaseInsensitive()
      throws ExecutionException, InterruptedException {
    input.setRelatedEntityTypes(List.of("CORPUSER"));
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

  @Test
  public void testDomainChildRelationshipsExcludeSoftDeleted()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn parentDomain = Urn.createFromString("urn:li:domain:parent");
    Urn activeChild = Urn.createFromString("urn:li:domain:activeChild");
    Urn softDeletedChild = Urn.createFromString("urn:li:domain:softDeletedChild");

    Domain domainSource = new Domain();
    domainSource.setUrn(parentDomain.toString());
    when(mockEnv.getSource()).thenReturn(domainSource);

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    stubParentAncestorWalk(entityGraphCache, "domain");
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("domain").source(GraphSnapshotSource.SEARCH).build();
    when(entityGraphCache.bindingForPolicyField("DOMAIN")).thenReturn(Optional.of(binding));
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.DOMAIN))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(parentDomain.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(
                    parentDomain.toString(), activeChild.toString(), softDeletedChild.toString())));

    QueryContext queryContext = getMockAllowContext();
    OperationContext baseContext = queryContext.getOperationContext();
    OperationContext opContext =
        baseContext.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .entityGraphCache(entityGraphCache)
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(mock(AspectRetriever.class))
                    .build())
            .build(baseContext.getSessionAuthentication(), false);
    when(queryContext.getOperationContext()).thenReturn(opContext);
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput domainInput = new RelationshipsInput();
    domainInput.setTypes(List.of("IsPartOf"));
    domainInput.setDirection(RelationshipDirection.INCOMING);
    domainInput.setIncludeSoftDelete(false);
    domainInput.setStart(0);
    domainInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(domainInput);

    when(_entityService.exists(any(), eq(Set.of(activeChild, softDeletedChild)), eq(false)))
        .thenReturn(Set.of(activeChild));

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), activeChild.toString());
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testGlossaryChildRelationshipsExcludeSoftDeleted()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn parentNode = Urn.createFromString("urn:li:glossaryNode:parent");
    Urn activeChildNode = Urn.createFromString("urn:li:glossaryNode:activeChild");
    Urn activeChildTerm = Urn.createFromString("urn:li:glossaryTerm:activeTerm");
    Urn softDeletedChild = Urn.createFromString("urn:li:glossaryTerm:softDeletedChild");

    GlossaryNode glossarySource = new GlossaryNode();
    glossarySource.setUrn(parentNode.toString());
    when(mockEnv.getSource()).thenReturn(glossarySource);

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    stubParentAncestorWalk(entityGraphCache, "glossary");
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(parentNode.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(
                    parentNode.toString(),
                    activeChildNode.toString(),
                    activeChildTerm.toString(),
                    softDeletedChild.toString())));

    QueryContext queryContext = getMockAllowContext();
    OperationContext baseContext = queryContext.getOperationContext();
    OperationContext opContext =
        baseContext.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .entityGraphCache(entityGraphCache)
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(mock(AspectRetriever.class))
                    .build())
            .build(baseContext.getSessionAuthentication(), false);
    when(queryContext.getOperationContext()).thenReturn(opContext);
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput glossaryInput = new RelationshipsInput();
    glossaryInput.setTypes(List.of("IsPartOf"));
    glossaryInput.setDirection(RelationshipDirection.INCOMING);
    glossaryInput.setIncludeSoftDelete(false);
    glossaryInput.setStart(0);
    glossaryInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(glossaryInput);

    when(_entityService.exists(
            any(), eq(Set.of(activeChildNode, activeChildTerm, softDeletedChild)), eq(false)))
        .thenReturn(Set.of(activeChildNode, activeChildTerm));

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 2);
    assertEquals(result.getTotal(), 2);
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testGlossaryChildRelationshipsGraphClientOnlyDoesNotFilterSoftDeleted()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn parentNode = Urn.createFromString("urn:li:glossaryNode:parent");
    Urn activeChildTerm = Urn.createFromString("urn:li:glossaryTerm:activeTerm");
    Urn softDeletedChild = Urn.createFromString("urn:li:glossaryTerm:softDeletedChild");

    GlossaryNode glossarySource = new GlossaryNode();
    glossarySource.setUrn(parentNode.toString());
    when(mockEnv.getSource()).thenReturn(glossarySource);

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    stubParentAncestorWalk(entityGraphCache, "glossary");
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(parentNode.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(
                    parentNode.toString(),
                    activeChildTerm.toString(),
                    softDeletedChild.toString())));

    QueryContext queryContext = getMockAllowContext();
    OperationContext baseContext = queryContext.getOperationContext();
    OperationContext opContext =
        baseContext.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .entityGraphCache(entityGraphCache)
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(mock(AspectRetriever.class))
                    .build())
            .build(baseContext.getSessionAuthentication(), false);
    when(queryContext.getOperationContext()).thenReturn(opContext);
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput glossaryInput = new RelationshipsInput();
    glossaryInput.setTypes(List.of("IsPartOf"));
    glossaryInput.setDirection(RelationshipDirection.INCOMING);
    glossaryInput.setIncludeSoftDelete(false);
    glossaryInput.setStart(0);
    glossaryInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(glossaryInput);

    EntityRelationshipsResultResolver graphOnlyResolver =
        new EntityRelationshipsResultResolver(_graphClient);
    EntityRelationshipsResult result = graphOnlyResolver.get(mockEnv).get();

    assertEquals(result.getCount(), 2);
    verify(_entityService, never()).exists(any(), anySet(), eq(false));
  }

  @Test
  public void testGlossaryChildRelationshipsFallsBackToGraphClientWhenNotDirectChildrenQuery()
      throws ExecutionException, InterruptedException {
    GlossaryNode glossarySource = new GlossaryNode();
    glossarySource.setUrn("urn:li:glossaryNode:parent");
    when(mockEnv.getSource()).thenReturn(glossarySource);

    RelationshipsInput nonGlossaryChildrenInput = new RelationshipsInput();
    nonGlossaryChildrenInput.setTypes(List.of("SomeType"));
    nonGlossaryChildrenInput.setDirection(RelationshipDirection.INCOMING);
    nonGlossaryChildrenInput.setStart(0);
    nonGlossaryChildrenInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(nonGlossaryChildrenInput);

    when(_graphClient.getRelatedEntities(
            eq("urn:li:glossaryNode:parent"),
            eq(new HashSet<>(nonGlossaryChildrenInput.getTypes())),
            same(com.linkedin.metadata.query.filter.RelationshipDirection.INCOMING),
            eq(nonGlossaryChildrenInput.getStart()),
            eq(nonGlossaryChildrenInput.getCount()),
            any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(0)
                .setTotal(0)
                .setRelationships(new EntityRelationshipArray()));

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    verify(_graphClient).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testContainerIsPartOfIncomingUsesGraphClientIncludingAssets()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn parentContainer = Urn.createFromString("urn:li:container:parent");
    Urn childContainer = Urn.createFromString("urn:li:container:child");
    Urn childDataset =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");

    Container containerSource = new Container();
    containerSource.setUrn(parentContainer.toString());
    when(mockEnv.getSource()).thenReturn(containerSource);

    RelationshipsInput containerInput = new RelationshipsInput();
    containerInput.setTypes(List.of("IsPartOf"));
    containerInput.setDirection(RelationshipDirection.INCOMING);
    containerInput.setIncludeSoftDelete(false);
    containerInput.setStart(0);
    containerInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(containerInput);

    when(_graphClient.getRelatedEntities(
            eq(parentContainer.toString()),
            eq(new HashSet<>(containerInput.getTypes())),
            same(com.linkedin.metadata.query.filter.RelationshipDirection.INCOMING),
            eq(containerInput.getStart()),
            eq(containerInput.getCount()),
            any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(2)
                .setTotal(2)
                .setRelationships(
                    new EntityRelationshipArray(
                        List.of(
                            new EntityRelationship().setEntity(childContainer).setType("IsPartOf"),
                            new EntityRelationship()
                                .setEntity(childDataset)
                                .setType("IsPartOf")))));

    // Soft-deleted dataset is present in the graph response but filtered via exists().
    when(_entityService.exists(any(), eq(Set.of(childContainer, childDataset)), eq(false)))
        .thenReturn(Set.of(childContainer));

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), childContainer.toString());
    verify(_graphClient)
        .getRelatedEntities(
            eq(parentContainer.toString()),
            eq(new HashSet<>(containerInput.getTypes())),
            same(com.linkedin.metadata.query.filter.RelationshipDirection.INCOMING),
            eq(containerInput.getStart()),
            eq(containerInput.getCount()),
            any());
  }

  @Test
  public void testSessionUserOutgoingGroupsDoesNotCallGraphClient()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn actorUrn = Urn.createFromString("urn:li:corpuser:test");
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:session-group");

    CorpUser userSource = new CorpUser();
    userSource.setUrn(actorUrn.toString());
    when(mockEnv.getSource()).thenReturn(userSource);

    QueryContext queryContext =
        TestUtils.getMockAllowContext(actorUrn.toString(), List.of(groupUrn));
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput groupsInput = new RelationshipsInput();
    groupsInput.setTypes(List.of("IsMemberOfGroup"));
    groupsInput.setDirection(RelationshipDirection.OUTGOING);
    groupsInput.setStart(0);
    groupsInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(groupsInput);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), groupUrn.toString());
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testSessionUserOutgoingDualGroupTypesLabelsCorpAndNativeSeparately()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn actorUrn = Urn.createFromString("urn:li:corpuser:test");
    Urn corpGroupUrn = Urn.createFromString("urn:li:corpGroup:corp-group");
    Urn nativeGroupUrn = Urn.createFromString("urn:li:corpGroup:native-group");

    CorpUser userSource = new CorpUser();
    userSource.setUrn(actorUrn.toString());
    when(mockEnv.getSource()).thenReturn(userSource);

    SessionActorIdentity sessionIdentity =
        new SessionActorIdentity(
            actorUrn, List.of(corpGroupUrn), List.of(nativeGroupUrn), Set.of());
    QueryContext queryContext =
        withSessionActorIdentity(getMockAllowContext(actorUrn.toString()), sessionIdentity);
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput groupsInput = new RelationshipsInput();
    groupsInput.setTypes(List.of("IsMemberOfGroup", "IsMemberOfNativeGroup"));
    groupsInput.setDirection(RelationshipDirection.OUTGOING);
    groupsInput.setStart(0);
    groupsInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(groupsInput);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 2);
    assertEquals(result.getTotal(), 2);
    assertTrue(
        result.getRelationships().stream()
            .anyMatch(
                rel ->
                    rel.getType().equals("IsMemberOfGroup")
                        && rel.getEntity().getUrn().equals(corpGroupUrn.toString())));
    assertTrue(
        result.getRelationships().stream()
            .anyMatch(
                rel ->
                    rel.getType().equals("IsMemberOfNativeGroup")
                        && rel.getEntity().getUrn().equals(nativeGroupUrn.toString())));
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testSessionUserOutgoingMembershipHonorsRelatedEntityTypes()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn actorUrn = Urn.createFromString("urn:li:corpuser:test");
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:session-group");
    Urn roleUrn = Urn.createFromString("urn:li:dataHubRole:Editor");

    CorpUser userSource = new CorpUser();
    userSource.setUrn(actorUrn.toString());
    when(mockEnv.getSource()).thenReturn(userSource);

    SessionActorIdentity sessionIdentity =
        new SessionActorIdentity(actorUrn, List.of(groupUrn), Set.of(roleUrn));
    QueryContext queryContext =
        withSessionActorIdentity(getMockAllowContext(actorUrn.toString()), sessionIdentity);
    when(queryContext
            .getOperationContext()
            .getAuthorizationContext()
            .resolveSessionActorRoles(any(), any()))
        .thenReturn(Set.of(roleUrn));
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput membershipInput = new RelationshipsInput();
    membershipInput.setTypes(List.of("IsMemberOfGroup", "IsMemberOfRole"));
    membershipInput.setDirection(RelationshipDirection.OUTGOING);
    membershipInput.setRelatedEntityTypes(List.of("dataHubRole"));
    membershipInput.setStart(0);
    // count=1 forces filter-before-paginate: page-then-filter would keep only the group and drop
    // the role.
    membershipInput.setCount(1);
    when(mockEnv.getArgument(eq("input"))).thenReturn(membershipInput);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getTotal().intValue(), 1);
    assertEquals(result.getCount().intValue(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), roleUrn.toString());
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testCorpGroupIncomingMembersUsesMembershipCache()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:eng");
    Urn memberUrn = Urn.createFromString("urn:li:corpuser:member");

    CorpGroup groupSource = new CorpGroup();
    groupSource.setUrn(groupUrn.toString());
    when(mockEnv.getSource()).thenReturn(groupSource);

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder()
            .graphId("membership")
            .source(GraphSnapshotSource.GRAPH)
            .build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.MEMBERSHIP))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.listRelated(
            eq("membership"),
            eq(GraphSnapshotSource.GRAPH),
            eq(groupUrn.toString()),
            eq(TraversalDirection.REVERSE),
            eq(Set.of("IsMemberOfGroup")),
            eq(1),
            eq(0),
            eq(10),
            any(ReadMode.class)))
        .thenReturn(
            MembershipNeighborResult.fromNeighbors(
                List.of(
                    new MembershipNeighborResult.Neighbor(memberUrn.toString(), "IsMemberOfGroup")),
                1));

    QueryContext queryContext = getMockAllowContext();
    OperationContext baseContext = queryContext.getOperationContext();
    OperationContext opContext =
        baseContext.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .entityGraphCache(entityGraphCache)
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(mock(AspectRetriever.class))
                    .build())
            .build(baseContext.getSessionAuthentication(), false);
    when(queryContext.getOperationContext()).thenReturn(opContext);
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput membersInput = new RelationshipsInput();
    membersInput.setTypes(List.of("IsMemberOfGroup"));
    membersInput.setDirection(RelationshipDirection.INCOMING);
    membersInput.setIncludeSoftDelete(false);
    membersInput.setStart(0);
    membersInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(membersInput);

    when(_entityService.exists(any(), eq(Set.of(memberUrn)), eq(false)))
        .thenReturn(Set.of(memberUrn));

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), memberUrn.toString());
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  /**
   * A member of a group via BOTH ingested ({@code IsMemberOfGroup}) and native ({@code
   * IsMemberOfNativeGroup}) membership yields two membership edges to the same corpuser. The
   * members list must collapse them to a single unique member (issue #14471), preferring the native
   * (GUI-managed) edge. The total stays the raw edge count: per-page dedup keeps the read cheap
   * (fetch only the requested page) rather than pre-scanning the whole membership set to correct
   * the rare dual-membership count.
   */
  @Test
  public void testCorpGroupIncomingMembersDeduplicatesAcrossMembershipEdgeTypes()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:eng");
    Urn memberUrn = Urn.createFromString("urn:li:corpuser:member");

    CorpGroup groupSource = new CorpGroup();
    groupSource.setUrn(groupUrn.toString());
    when(mockEnv.getSource()).thenReturn(groupSource);

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder()
            .graphId("membership")
            .source(GraphSnapshotSource.GRAPH)
            .build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.MEMBERSHIP))
        .thenReturn(Optional.of(binding));
    // Same neighbor URN returned once per requested edge type; count reflects edges, not members.
    when(entityGraphCache.listRelated(
            eq("membership"),
            eq(GraphSnapshotSource.GRAPH),
            eq(groupUrn.toString()),
            eq(TraversalDirection.REVERSE),
            eq(Set.of("IsMemberOfGroup", "IsMemberOfNativeGroup")),
            eq(1),
            eq(0),
            anyInt(),
            any(ReadMode.class)))
        .thenReturn(
            MembershipNeighborResult.fromNeighbors(
                List.of(
                    new MembershipNeighborResult.Neighbor(memberUrn.toString(), "IsMemberOfGroup"),
                    new MembershipNeighborResult.Neighbor(
                        memberUrn.toString(), "IsMemberOfNativeGroup")),
                2));

    QueryContext queryContext = getMockAllowContext();
    OperationContext baseContext = queryContext.getOperationContext();
    OperationContext opContext =
        baseContext.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .entityGraphCache(entityGraphCache)
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(mock(AspectRetriever.class))
                    .build())
            .build(baseContext.getSessionAuthentication(), false);
    when(queryContext.getOperationContext()).thenReturn(opContext);
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput membersInput = new RelationshipsInput();
    membersInput.setTypes(List.of("IsMemberOfGroup", "IsMemberOfNativeGroup"));
    membersInput.setDirection(RelationshipDirection.INCOMING);
    membersInput.setIncludeSoftDelete(false);
    membersInput.setStart(0);
    membersInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(membersInput);

    when(_entityService.exists(any(), eq(Set.of(memberUrn)), eq(false)))
        .thenReturn(Set.of(memberUrn));

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    // Raw edge total (2) is preserved; the returned list is deduped to the single unique member.
    assertEquals(result.getTotal().intValue(), 2);
    assertEquals(result.getCount().intValue(), 1);
    assertEquals(result.getRelationships().size(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), memberUrn.toString());
    assertEquals(result.getRelationships().get(0).getType(), "IsMemberOfNativeGroup");
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());

    // Per-page dedup must not pre-scan the membership set: the resolver fetches only the requested
    // page size (10), never the bounds.maxEdges cap.
    verify(entityGraphCache)
        .listRelated(
            eq("membership"),
            eq(GraphSnapshotSource.GRAPH),
            eq(groupUrn.toString()),
            eq(TraversalDirection.REVERSE),
            eq(Set.of("IsMemberOfGroup", "IsMemberOfNativeGroup")),
            eq(1),
            eq(0),
            eq(10),
            any(ReadMode.class));
  }

  /**
   * A role's members ({@code IsMemberOfRole} INCOMING) can be both users and groups. The membership
   * graph stores both edge types; reverse listing must be served from the membership path and
   * surface group grants alongside users (not drop groups or bypass to the live graph).
   */
  @Test
  public void testDataHubRoleIncomingMembersHonorsRelatedEntityTypes()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn userUrn = Urn.createFromString("urn:li:corpuser:alice");
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:eng");
    setupDataHubRoleIncomingMembersMembershipCache(userUrn, groupUrn, List.of("corpuser"), null, 2);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getTotal().intValue(), 1);
    assertEquals(result.getCount().intValue(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), userUrn.toString());
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testDataHubRoleIncomingMembersRelatedEntityTypesUsesMaxEdgesFetchCap()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn userUrn = Urn.createFromString("urn:li:corpuser:alice");
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:eng");
    int fetchCap = 2;
    EntityGraphCache entityGraphCache =
        setupDataHubRoleIncomingMembersMembershipCache(
            userUrn, groupUrn, List.of("corpuser"), fetchCap, 2);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getTotal().intValue(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), userUrn.toString());
    verify(entityGraphCache)
        .listRelated(
            eq("membership"),
            eq(GraphSnapshotSource.GRAPH),
            eq("urn:li:dataHubRole:Editor"),
            eq(TraversalDirection.REVERSE),
            eq(Set.of("IsMemberOfRole")),
            anyInt(),
            eq(0),
            eq(fetchCap),
            any(ReadMode.class));
  }

  @Test
  public void testDataHubRoleIncomingMembersRelatedEntityTypesFailsClosedWhenFetchTruncated()
      throws URISyntaxException {
    Urn userUrn = Urn.createFromString("urn:li:corpuser:alice");
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:eng");
    // Cap fetches 2 neighbors but backend reports total 5 → refuse truncated filter+page.
    setupDataHubRoleIncomingMembersMembershipCache(userUrn, groupUrn, List.of("corpuser"), 2, 5);

    ExecutionException thrown =
        expectThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    assertTrue(thrown.getCause() instanceof IllegalStateException);
    assertTrue(thrown.getCause().getMessage().contains("fetch cap"));
  }

  @Test
  public void testDataHubRoleIncomingMembersIncludeGroupsViaMembershipCache()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn userUrn = Urn.createFromString("urn:li:corpuser:alice");
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:eng");
    setupDataHubRoleIncomingMembersMembershipCache(userUrn, groupUrn, null, null, 2);

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    Set<String> members = new HashSet<>();
    for (var rel : result.getRelationships()) {
      members.add(rel.getEntity().getUrn());
    }
    assertTrue(
        members.contains(groupUrn.toString()),
        "role member listing must include group grants, not only users");
    assertTrue(members.contains(userUrn.toString()));
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  /**
   * @param relatedTypeFilterFetchCap when non-null (and relatedEntityTypes set), stubs {@code
   *     maxEdgesForGraph} and expects {@code listRelated} count to match the cap
   * @param reportedTotal total reported by the membership hit (may exceed fetched neighbors to
   *     simulate truncation)
   */
  @Nonnull
  private EntityGraphCache setupDataHubRoleIncomingMembersMembershipCache(
      @Nonnull Urn userUrn,
      @Nonnull Urn groupUrn,
      @Nullable List<String> relatedEntityTypes,
      @Nullable Integer relatedTypeFilterFetchCap,
      int reportedTotal)
      throws URISyntaxException {
    Urn roleUrn = Urn.createFromString("urn:li:dataHubRole:Editor");

    CorpGroup roleSource = new CorpGroup();
    roleSource.setUrn(roleUrn.toString());
    when(mockEnv.getSource()).thenReturn(roleSource);

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder()
            .graphId("membership")
            .source(GraphSnapshotSource.GRAPH)
            .build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.MEMBERSHIP))
        .thenReturn(Optional.of(binding));
    if (relatedTypeFilterFetchCap != null) {
      when(entityGraphCache.maxEdgesForGraph("membership"))
          .thenReturn(OptionalInt.of(relatedTypeFilterFetchCap));
    }
    when(entityGraphCache.listRelated(
            eq("membership"),
            eq(GraphSnapshotSource.GRAPH),
            eq(roleUrn.toString()),
            eq(TraversalDirection.REVERSE),
            eq(Set.of("IsMemberOfRole")),
            anyInt(),
            eq(0),
            relatedTypeFilterFetchCap != null && relatedEntityTypes != null
                ? eq(relatedTypeFilterFetchCap)
                : anyInt(),
            any(ReadMode.class)))
        .thenReturn(
            MembershipNeighborResult.fromNeighbors(
                List.of(
                    new MembershipNeighborResult.Neighbor(userUrn.toString(), "IsMemberOfRole"),
                    new MembershipNeighborResult.Neighbor(groupUrn.toString(), "IsMemberOfRole")),
                reportedTotal));

    QueryContext queryContext = getMockAllowContext();
    OperationContext baseContext = queryContext.getOperationContext();
    OperationContext opContext =
        baseContext.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .entityGraphCache(entityGraphCache)
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(mock(AspectRetriever.class))
                    .build())
            .build(baseContext.getSessionAuthentication(), false);
    when(queryContext.getOperationContext()).thenReturn(opContext);
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput membersInput = new RelationshipsInput();
    membersInput.setTypes(List.of("IsMemberOfRole"));
    membersInput.setDirection(RelationshipDirection.INCOMING);
    if (relatedEntityTypes != null) {
      membersInput.setRelatedEntityTypes(relatedEntityTypes);
    }
    membersInput.setIncludeSoftDelete(false);
    membersInput.setStart(0);
    membersInput.setCount(50);
    when(mockEnv.getArgument(eq("input"))).thenReturn(membersInput);

    when(_entityService.exists(any(), anySet(), eq(false))).thenAnswer(inv -> inv.getArgument(1));
    return entityGraphCache;
  }

  @Test
  public void testGlossaryChildListingResolvesParentAncestorsOnceForAllChildren()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn grandparent = Urn.createFromString("urn:li:glossaryNode:grandparent");
    Urn parentNode = Urn.createFromString("urn:li:glossaryNode:parent");
    Urn termA = Urn.createFromString("urn:li:glossaryTerm:a");
    Urn termB = Urn.createFromString("urn:li:glossaryTerm:b");

    GlossaryNode glossarySource = new GlossaryNode();
    glossarySource.setUrn(parentNode.toString());
    when(mockEnv.getSource()).thenReturn(glossarySource);

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(parentNode.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(parentNode.toString(), termA.toString(), termB.toString())));
    when(entityGraphCache.expand(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.FORWARD),
            anySet(),
            anyInt(),
            anyInt(),
            any()))
        .thenReturn(
            GraphReadResult.fromVertices(Set.of(parentNode.toString(), grandparent.toString())));

    QueryContext queryContext = getMockAllowContext();
    OperationContext baseContext = queryContext.getOperationContext();
    OperationContext opContext =
        baseContext.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .entityGraphCache(entityGraphCache)
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(mock(AspectRetriever.class))
                    .build())
            .build(baseContext.getSessionAuthentication(), false);
    when(queryContext.getOperationContext()).thenReturn(opContext);
    when(mockEnv.getContext()).thenReturn(queryContext);

    RelationshipsInput glossaryInput = new RelationshipsInput();
    glossaryInput.setTypes(List.of("IsPartOf"));
    glossaryInput.setDirection(RelationshipDirection.INCOMING);
    glossaryInput.setIncludeSoftDelete(false);
    glossaryInput.setStart(0);
    glossaryInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(glossaryInput);
    when(_entityService.exists(any(), anySet(), eq(false))).thenAnswer(inv -> inv.getArgument(1));

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getTotal(), 2);
    // One ancestor walk for the parent, however many children are checked.
    verify(entityGraphCache, times(1))
        .expand(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.FORWARD),
            anySet(),
            anyInt(),
            anyInt(),
            any());
    Map<EntitySpec, Map<EntityFieldType, FieldResolver>> overrides =
        opContext.getAuthorizationContext().getSessionResourceFieldOverrides();
    for (Urn term : List.of(termA, termB)) {
      Set<String> glossaryValues =
          overrides
              .get(new EntitySpec(term.getEntityType(), term.toString()))
              .get(EntityFieldType.GLOSSARY)
              .getFieldValuesFuture()
              .join()
              .getValues();
      assertEquals(
          glossaryValues, Set.of(term.toString(), parentNode.toString(), grandparent.toString()));
    }
  }

  private static final ViewAuthorizationConfiguration VIEW_AUTH_ALL_RESTRICTED =
      ViewAuthorizationConfiguration.builder()
          .enabled(true)
          .effectiveUnrestrictedEntityTypes(Set.of())
          .build();

  private record GlossaryChildListing(
      QueryContext queryContext, OperationContext opContext, EntityGraphCache cache) {

    Authorizer authorizer() {
      return queryContext.getAuthorizer();
    }
  }

  private static List<Urn> glossaryTerms(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> UrnUtils.getUrn(String.format("urn:li:glossaryTerm:term%04d", i)))
        .collect(Collectors.toList());
  }

  /**
   * Serves {@code children} as the direct children of {@code parent} from the entity graph cache,
   * with the given View Authorization configuration (null keeps the default test configuration,
   * which has it off) and the system actor when {@code asSystem} is set.
   */
  private GlossaryChildListing givenGlossaryChildren(
      Urn parent,
      List<Urn> children,
      @Nullable ViewAuthorizationConfiguration viewAuthorization,
      boolean asSystem) {
    GlossaryNode glossarySource = new GlossaryNode();
    glossarySource.setUrn(parent.toString());
    when(mockEnv.getSource()).thenReturn(glossarySource);

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    stubParentAncestorWalk(entityGraphCache, "glossary");
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(binding));
    Set<String> vertices = new HashSet<>();
    vertices.add(parent.toString());
    children.forEach(child -> vertices.add(child.toString()));
    when(entityGraphCache.expand(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(parent.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.fromVertices(vertices));

    QueryContext queryContext = getMockAllowContext();
    OperationContext baseContext = queryContext.getOperationContext();
    var builder =
        baseContext.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .entityGraphCache(entityGraphCache)
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(mock(AspectRetriever.class))
                    .build());
    if (viewAuthorization != null) {
      builder.operationContextConfig(
          baseContext.getOperationContextConfig().toBuilder()
              .viewAuthorizationConfiguration(viewAuthorization)
              .build());
    }
    OperationContext opContext =
        builder.build(
            asSystem
                ? baseContext.getSystemActorContext().getAuthentication()
                : baseContext.getSessionAuthentication(),
            false);
    when(queryContext.getOperationContext()).thenReturn(opContext);
    when(mockEnv.getContext()).thenReturn(queryContext);
    when(_entityService.exists(any(), anySet(), eq(false))).thenAnswer(inv -> inv.getArgument(1));
    return new GlossaryChildListing(queryContext, opContext, entityGraphCache);
  }

  private EntityRelationshipsResult listGlossaryChildren(
      @Nullable Integer start, @Nullable Integer count)
      throws ExecutionException, InterruptedException {
    RelationshipsInput glossaryInput = new RelationshipsInput();
    glossaryInput.setTypes(List.of("IsPartOf"));
    glossaryInput.setDirection(RelationshipDirection.INCOMING);
    glossaryInput.setIncludeSoftDelete(false);
    glossaryInput.setStart(start);
    glossaryInput.setCount(count);
    when(mockEnv.getArgument(eq("input"))).thenReturn(glossaryInput);
    return resolver.get(mockEnv).get();
  }

  private static List<String> relationshipUrns(EntityRelationshipsResult result) {
    return result.getRelationships().stream()
        .map(relationship -> relationship.getEntity().getUrn())
        .collect(Collectors.toList());
  }

  private static Set<String> authorizedResources(Authorizer authorizer) {
    ArgumentCaptor<AuthorizationRequest> requests =
        ArgumentCaptor.forClass(AuthorizationRequest.class);
    verify(authorizer, atLeast(0)).authorize(requests.capture());
    return requests.getAllValues().stream()
        .flatMap(request -> request.getResourceSpec().stream())
        .map(EntitySpec::getEntity)
        .collect(Collectors.toSet());
  }

  @Test
  public void testChildListingWithCountZeroReturnsTotalAndChecksNothing()
      throws ExecutionException, InterruptedException {
    Urn parent = UrnUtils.getUrn("urn:li:glossaryNode:parent");
    GlossaryChildListing listing =
        givenGlossaryChildren(parent, glossaryTerms(5), VIEW_AUTH_ALL_RESTRICTED, false);

    EntityRelationshipsResult result = listGlossaryChildren(0, 0);

    assertEquals(result.getTotal(), 5);
    assertEquals(result.getCount(), 0);
    assertTrue(result.getRelationships().isEmpty());
    assertTrue(authorizedResources(listing.authorizer()).isEmpty());
    assertTrue(
        listing.opContext().getAuthorizationContext().getSessionResourceFieldOverrides().isEmpty());
    verify(listing.cache(), never())
        .expand(
            eq("glossary"),
            any(),
            eq(TraversalDirection.FORWARD),
            anySet(),
            anyInt(),
            anyInt(),
            any());
  }

  @Test
  public void testChildListingChecksAndSeedsOnlyTheRequestedPage()
      throws ExecutionException, InterruptedException {
    Urn parent = UrnUtils.getUrn("urn:li:glossaryNode:parent");
    List<Urn> children = glossaryTerms(2095);
    GlossaryChildListing listing =
        givenGlossaryChildren(parent, children, VIEW_AUTH_ALL_RESTRICTED, false);

    EntityRelationshipsResult result = listGlossaryChildren(10, 50);

    List<String> expectedPage =
        children.stream().skip(10).limit(50).map(Urn::toString).collect(Collectors.toList());
    assertEquals(result.getTotal(), 2095);
    assertEquals(relationshipUrns(result), expectedPage);
    assertEquals(authorizedResources(listing.authorizer()), Set.copyOf(expectedPage));
    assertEquals(
        listing.opContext().getAuthorizationContext().getSessionResourceFieldOverrides().size(),
        50);
  }

  @Test
  public void testChildListingReturnsShortPageWithUnfilteredTotalWhenChildrenAreHidden()
      throws ExecutionException, InterruptedException {
    Urn parent = UrnUtils.getUrn("urn:li:glossaryNode:parent");
    List<Urn> children = glossaryTerms(4);
    GlossaryChildListing listing =
        givenGlossaryChildren(parent, children, VIEW_AUTH_ALL_RESTRICTED, false);
    String hidden = children.get(1).toString();
    when(listing.authorizer().authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            inv -> {
              AuthorizationRequest request = inv.getArgument(0);
              boolean denied =
                  request
                      .getResourceSpec()
                      .map(spec -> hidden.equals(spec.getEntity()))
                      .orElse(false);
              return new AuthorizationResult(
                  request,
                  denied ? AuthorizationResult.Type.DENY : AuthorizationResult.Type.ALLOW,
                  "");
            });

    EntityRelationshipsResult result = listGlossaryChildren(0, 3);

    assertEquals(
        relationshipUrns(result), List.of(children.get(0).toString(), children.get(2).toString()));
    assertEquals(result.getCount(), 2);
    assertEquals(result.getTotal(), 4);
  }

  @Test
  public void testChildListingHonorsConfiguredPageLimits()
      throws ExecutionException, InterruptedException {
    Urn parent = UrnUtils.getUrn("urn:li:glossaryNode:parent");
    GlossaryChildListing listing = givenGlossaryChildren(parent, glossaryTerms(50), null, false);
    when(listing.queryContext().getDirectChildDefaultCount()).thenReturn(3);
    when(listing.queryContext().getDirectChildMaxCount()).thenReturn(5);

    assertEquals(listGlossaryChildren(0, null).getRelationships().size(), 3);
    assertEquals(listGlossaryChildren(0, 40).getRelationships().size(), 5);
  }

  @Test
  public void testChildListingAppliesDefaultAndMaximumCountAndClampsNegatives()
      throws ExecutionException, InterruptedException {
    Urn parent = UrnUtils.getUrn("urn:li:glossaryNode:parent");
    List<Urn> children = glossaryTerms(1200);
    givenGlossaryChildren(parent, children, null, false);

    EntityRelationshipsResult omittedCount = listGlossaryChildren(0, null);
    assertEquals(omittedCount.getRelationships().size(), 100);
    assertEquals(omittedCount.getTotal(), 1200);

    EntityRelationshipsResult oversizedCount = listGlossaryChildren(0, 5000);
    assertEquals(oversizedCount.getRelationships().size(), 1000);

    EntityRelationshipsResult negativeStart = listGlossaryChildren(-5, 3);
    assertEquals(negativeStart.getStart(), 0);
    assertEquals(
        relationshipUrns(negativeStart),
        children.subList(0, 3).stream().map(Urn::toString).collect(Collectors.toList()));

    EntityRelationshipsResult negativeCount = listGlossaryChildren(0, -1);
    assertTrue(negativeCount.getRelationships().isEmpty());
    assertEquals(negativeCount.getTotal(), 1200);
  }

  @Test
  public void testChildListingRunsNoViewChecksWhenViewAuthorizationDoesNotApply()
      throws ExecutionException, InterruptedException {
    Urn parent = UrnUtils.getUrn("urn:li:glossaryNode:parent");
    List<Urn> children = glossaryTerms(20);

    GlossaryChildListing disabled = givenGlossaryChildren(parent, children, null, false);
    assertEquals(listGlossaryChildren(0, 20).getRelationships().size(), 20);
    assertTrue(authorizedResources(disabled.authorizer()).isEmpty());

    GlossaryChildListing unrestrictedType =
        givenGlossaryChildren(
            parent,
            children,
            ViewAuthorizationConfiguration.builder()
                .enabled(true)
                .effectiveUnrestrictedEntityTypes(Set.of("glossaryTerm"))
                .build(),
            false);
    assertEquals(listGlossaryChildren(0, 20).getRelationships().size(), 20);
    assertTrue(authorizedResources(unrestrictedType.authorizer()).isEmpty());

    GlossaryChildListing systemActor =
        givenGlossaryChildren(parent, children, VIEW_AUTH_ALL_RESTRICTED, true);
    assertEquals(listGlossaryChildren(0, 20).getRelationships().size(), 20);
    assertTrue(authorizedResources(systemActor.authorizer()).isEmpty());
  }

  /** The child listing resolves the parent's ancestors once; return just the parent. */
  private static void stubParentAncestorWalk(EntityGraphCache entityGraphCache, String graphId) {
    when(entityGraphCache.expand(
            eq(graphId),
            any(),
            eq(TraversalDirection.FORWARD),
            anySet(),
            anyInt(),
            anyInt(),
            any()))
        .thenAnswer(inv -> GraphReadResult.fromVertices(inv.getArgument(3)));
  }

  private com.linkedin.datahub.graphql.generated.EntityRelationship resultRelationship(
      Entity entity) {
    return new com.linkedin.datahub.graphql.generated.EntityRelationship(
        "SomeType", RelationshipDirection.INCOMING, entity, null);
  }
}
