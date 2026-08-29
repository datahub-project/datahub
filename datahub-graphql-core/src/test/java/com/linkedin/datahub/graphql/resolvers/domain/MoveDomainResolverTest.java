package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MoveDomainInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MoveDomainResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MoveDomainResolverTest {

  private static final String CONTAINER_URN = "urn:li:container:00005397daf94708a8822b8106cfd451";
  private static final String PARENT_DOMAIN_URN = "urn:li:domain:00005397daf94708a8822b8106cfd451";
  private static final String DOMAIN_URN = "urn:li:domain:11115397daf94708a8822b8106cfd451";
  private static final MoveDomainInput INPUT = new MoveDomainInput(PARENT_DOMAIN_URN, DOMAIN_URN);
  private static final MoveDomainInput INVALID_INPUT =
      new MoveDomainInput(CONTAINER_URN, DOMAIN_URN);
  private static final CorpuserUrn TEST_ACTOR_URN = new CorpuserUrn("test");

  private MetadataChangeProposal setupTests(
      DataFetchingEnvironment mockEnv, EntityService<?> mockService, EntityClient mockClient)
      throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final String name = "test name";
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(DOMAIN_URN)),
                eq(Constants.DOMAIN_PROPERTIES_ASPECT_NAME),
                eq(0L)))
        .thenReturn(new DomainProperties().setName(name));

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
                Mockito.eq(
                    DomainUtils.buildNameAndParentDomainFilter(
                        name, Urn.createFromString(PARENT_DOMAIN_URN))),
                Mockito.eq(null),
                Mockito.any(Integer.class),
                Mockito.any(Integer.class)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    DomainProperties properties = new DomainProperties();
    properties.setName(name);
    properties.setParentDomain(Urn.createFromString(PARENT_DOMAIN_URN));
    return MutationUtils.buildMetadataChangeProposalWithUrn(
        Urn.createFromString(DOMAIN_URN), DOMAIN_PROPERTIES_ASPECT_NAME, properties);
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockService.exists(
                any(OperationContext.class), eq(Urn.createFromString(PARENT_DOMAIN_URN)), eq(true)))
        .thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    MoveDomainResolver resolver = new MoveDomainResolver(mockService, mockClient);
    setupTests(mockEnv, mockService, mockClient);

    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(PARENT_DOMAIN_URN)), eq(true)))
        .thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(DOMAIN_URN)),
                eq(DOMAIN_PROPERTIES_ASPECT_NAME),
                eq(0)))
        .thenReturn(null);

    MoveDomainResolver resolver = new MoveDomainResolver(mockService, mockClient);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureParentDoesNotExist() throws Exception {
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(PARENT_DOMAIN_URN)), eq(true)))
        .thenReturn(false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    MoveDomainResolver resolver = new MoveDomainResolver(mockService, mockClient);
    setupTests(mockEnv, mockService, mockClient);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureMoveUnderDescendant() throws Exception {
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    String parentUrn = "urn:li:domain:parent";
    String childUrn = "urn:li:domain:child";
    MoveDomainInput cycleInput = new MoveDomainInput(childUrn, parentUrn);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(childUrn)), eq(true)))
        .thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(cycleInput);

    QueryContext mockContext = getMockAllowContext();
    OperationContext base = mockContext.getOperationContext();
    EntityGraphCache entityGraphCache = Mockito.mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        com.linkedin.metadata.graph.cache.EntityGraphBinding.builder()
            .graphId("domain")
            .source(com.linkedin.metadata.graph.cache.GraphSnapshotSource.SEARCH)
            .build();
    Mockito.when(entityGraphCache.bindingForPolicyField("DOMAIN"))
        .thenReturn(java.util.Optional.empty());
    Mockito.when(
            entityGraphCache.bindingForKnownGraph(
                com.linkedin.metadata.graph.cache.KnownEntityGraph.DOMAIN))
        .thenReturn(java.util.Optional.of(binding));
    Mockito.when(
            entityGraphCache.expand(
                eq("domain"),
                eq(com.linkedin.metadata.graph.cache.GraphSnapshotSource.SEARCH),
                eq(com.linkedin.metadata.graph.cache.TraversalDirection.REVERSE),
                eq(java.util.Set.of(parentUrn)),
                anyInt(),
                eq(com.linkedin.metadata.graph.cache.EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
                eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.fromVertices(java.util.Set.of(parentUrn, childUrn)));

    io.datahubproject.metadata.context.RetrieverContext retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .graphRetriever(base.getRetrieverContext().getGraphRetriever())
            .aspectRetriever(base.getRetrieverContext().getAspectRetriever())
            .cachingAspectRetriever(base.getRetrieverContext().getCachingAspectRetriever())
            .searchRetriever(base.getRetrieverContext().getSearchRetriever())
            .entityGraphCache(entityGraphCache)
            .build();
    OperationContext operationContext =
        base.toBuilder()
            .retrieverContext(retrieverContext)
            .build(base.getSessionAuthentication(), false);
    Mockito.when(mockContext.getOperationContext()).thenReturn(operationContext);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(parentUrn)),
                eq(DOMAIN_PROPERTIES_ASPECT_NAME),
                eq(0L)))
        .thenReturn(new DomainProperties().setName("parent"));

    MoveDomainResolver resolver = new MoveDomainResolver(mockService, mockClient);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }
}
