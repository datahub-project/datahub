package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.withSessionActorIdentity;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.SessionActorIdentity;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
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
  public void testContainerChildRelationshipsExcludeSoftDeleted()
      throws ExecutionException, InterruptedException, URISyntaxException {
    Urn parentContainer = Urn.createFromString("urn:li:container:parent");
    Urn activeChild = Urn.createFromString("urn:li:container:activeChild");
    Urn softDeletedChild = Urn.createFromString("urn:li:container:softDeletedChild");

    Container containerSource = new Container();
    containerSource.setUrn(parentContainer.toString());
    when(mockEnv.getSource()).thenReturn(containerSource);

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("container").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForPolicyField("CONTAINER")).thenReturn(Optional.of(binding));
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.CONTAINER))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("container"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(parentContainer.toString())),
            anyInt(),
            eq(1),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(
                    parentContainer.toString(),
                    activeChild.toString(),
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

    RelationshipsInput containerInput = new RelationshipsInput();
    containerInput.setTypes(List.of("IsPartOf"));
    containerInput.setDirection(RelationshipDirection.INCOMING);
    containerInput.setIncludeSoftDelete(false);
    containerInput.setStart(0);
    containerInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(containerInput);

    when(_entityService.exists(any(), eq(Set.of(activeChild, softDeletedChild)), eq(false)))
        .thenReturn(Set.of(activeChild));

    EntityRelationshipsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), activeChild.toString());
    verify(_graphClient, never()).getRelatedEntities(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testContainerChildRelationshipsFallsBackToGraphClientWhenNotDirectChildrenQuery()
      throws ExecutionException, InterruptedException {
    Container containerSource = new Container();
    containerSource.setUrn("urn:li:container:parent");
    when(mockEnv.getSource()).thenReturn(containerSource);

    RelationshipsInput nonContainerChildrenInput = new RelationshipsInput();
    nonContainerChildrenInput.setTypes(List.of("SomeType"));
    nonContainerChildrenInput.setDirection(RelationshipDirection.INCOMING);
    nonContainerChildrenInput.setStart(0);
    nonContainerChildrenInput.setCount(10);
    when(mockEnv.getArgument(eq("input"))).thenReturn(nonContainerChildrenInput);

    when(_graphClient.getRelatedEntities(
            eq("urn:li:container:parent"),
            eq(new HashSet<>(nonContainerChildrenInput.getTypes())),
            same(com.linkedin.metadata.query.filter.RelationshipDirection.INCOMING),
            eq(nonContainerChildrenInput.getStart()),
            eq(nonContainerChildrenInput.getCount()),
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

  private com.linkedin.datahub.graphql.generated.EntityRelationship resultRelationship(
      Entity entity) {
    return new com.linkedin.datahub.graphql.generated.EntityRelationship(
        "SomeType", RelationshipDirection.INCOMING, entity, null);
  }
}
