package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.UpdateParentNodeInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateParentNodeResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateParentNodeResolverTest {

  private static final String CONTAINER_URN = "urn:li:container:00005397daf94708a8822b8106cfd451";
  private static final String PARENT_NODE_URN =
      "urn:li:glossaryNode:00005397daf94708a8822b8106cfd451";
  private static final String TERM_URN = "urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451";
  private static final String NODE_URN = "urn:li:glossaryNode:22225397daf94708a8822b8106cfd451";
  private static final UpdateParentNodeInput INPUT =
      new UpdateParentNodeInput(PARENT_NODE_URN, TERM_URN);
  private static final UpdateParentNodeInput INPUT_WITH_NODE =
      new UpdateParentNodeInput(PARENT_NODE_URN, NODE_URN);
  private static final UpdateParentNodeInput INVALID_INPUT =
      new UpdateParentNodeInput(CONTAINER_URN, TERM_URN);
  private static final CorpuserUrn TEST_ACTOR_URN = new CorpuserUrn("test");

  private MetadataChangeProposal setupTests(
      DataFetchingEnvironment mockEnv, EntityService<?> mockService) throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final String name = "test name";
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(TERM_URN)),
                eq(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME),
                eq(0L)))
        .thenReturn(new GlossaryTermInfo().setName(name));

    GlossaryTermInfo info = new GlossaryTermInfo();
    info.setName(name);
    info.setParentNode(GlossaryNodeUrn.createFromString(PARENT_NODE_URN));
    return MutationUtils.buildMetadataChangeProposalWithUrn(
        Urn.createFromString(TERM_URN), GLOSSARY_TERM_INFO_ASPECT_NAME, info);
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TERM_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(OperationContext.class),
                eq(GlossaryNodeUrn.createFromString(PARENT_NODE_URN)),
                eq(true)))
        .thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService, mockClient);
    final MetadataChangeProposal proposal = setupTests(mockEnv, mockService);

    assertTrue(resolver.get(mockEnv).get());
    verifySingleIngestProposal(mockService, 1, proposal);
  }

  @Test
  public void testGetSuccessForNode() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(NODE_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(), eq(GlossaryNodeUrn.createFromString(PARENT_NODE_URN)), eq(true)))
        .thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT_WITH_NODE);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final String name = "test name";
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(NODE_URN)),
                eq(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME),
                eq(0L)))
        .thenReturn(new GlossaryNodeInfo().setName(name));

    GlossaryNodeInfo info = new GlossaryNodeInfo();
    info.setName(name);
    info.setParentNode(GlossaryNodeUrn.createFromString(PARENT_NODE_URN));
    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(NODE_URN), GLOSSARY_NODE_INFO_ASPECT_NAME, info);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService, mockClient);

    assertTrue(resolver.get(mockEnv).get());
    verifySingleIngestProposal(mockService, 1, proposal);
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TERM_URN)), eq(true)))
        .thenReturn(false);
    Mockito.when(
            mockService.exists(
                any(), eq(GlossaryNodeUrn.createFromString(PARENT_NODE_URN)), eq(true)))
        .thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService, mockClient);
    setupTests(mockEnv, mockService);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureNodeDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TERM_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(), eq(GlossaryNodeUrn.createFromString(PARENT_NODE_URN)), eq(true)))
        .thenReturn(false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService, mockClient);
    setupTests(mockEnv, mockService);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureParentIsNotNode() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TERM_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(), eq(GlossaryNodeUrn.createFromString(PARENT_NODE_URN)), eq(true)))
        .thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INVALID_INPUT);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService, mockClient);
    setupTests(mockEnv, mockService);

    assertThrows(URISyntaxException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureWouldCreateCycle() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(NODE_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(), eq(GlossaryNodeUrn.createFromString(PARENT_NODE_URN)), eq(true)))
        .thenReturn(true);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT_WITH_NODE);

    QueryContext mockContext = getMockAllowContext();
    mockContextWithGlossaryGraphCache(
        mockContext, GraphReadResult.fromVertices(Set.of(NODE_URN, PARENT_NODE_URN)));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService, mockClient);

    CompletionException completionException =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    assertTrue(completionException.getCause() instanceof DataHubGraphQLException);
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetSuccessWhenNewParentIsNotDescendant() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(NODE_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(), eq(GlossaryNodeUrn.createFromString(PARENT_NODE_URN)), eq(true)))
        .thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT_WITH_NODE);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    mockContextWithGlossaryGraphCache(
        mockContext, GraphReadResult.fromVertices(Set.of(NODE_URN, "urn:li:glossaryNode:other")));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final String name = "test name";
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(NODE_URN)),
                eq(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME),
                eq(0L)))
        .thenReturn(new GlossaryNodeInfo().setName(name));

    GlossaryNodeInfo info = new GlossaryNodeInfo();
    info.setName(name);
    info.setParentNode(GlossaryNodeUrn.createFromString(PARENT_NODE_URN));
    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(NODE_URN), GLOSSARY_NODE_INFO_ASPECT_NAME, info);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService, mockClient);

    assertTrue(resolver.get(mockEnv).get());
    verifySingleIngestProposal(mockService, 1, proposal);
  }

  private static void mockContextWithGlossaryGraphCache(
      QueryContext mockContext, GraphReadResult expandResult) {
    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("glossary").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.GLOSSARY))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("glossary"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.REVERSE),
            eq(Set.of(NODE_URN)),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(expandResult);

    OperationContext baseContext = mockContext.getOperationContext();
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
    when(mockContext.getOperationContext()).thenReturn(opContext);
  }
}
