package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ContainerFieldResolverProviderTest
    extends EntityFieldResolverProviderBaseTest<ContainerFieldResolverProvider> {

  private static final String CONTAINER_URN = "urn:li:container:fooContainer";
  private static final String CONTAINER_PARENT_URN = "urn:li:container:fooContainer_parent";
  private static final String RESOURCE_URN =
      "urn:li:dataset:(urn:li:dataPlatform:s3,test-platform-instance.testDataset,PROD)";
  private static final EntitySpec RESOURCE_SPEC = new EntitySpec(DATASET_ENTITY_NAME, RESOURCE_URN);

  @Mock private SystemEntityClient entityClientMock;

  private OperationContext systemOperationContext;

  private ContainerFieldResolverProvider containerFieldResolverProvider;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
    containerFieldResolverProvider = buildFieldResolverProvider();
    systemOperationContext = operationContextWithContainerParents(Map.of());
  }

  @Override
  protected ContainerFieldResolverProvider buildFieldResolverProvider() {
    return new ContainerFieldResolverProvider(entityClientMock);
  }

  @Test
  public void shouldReturnContainerType() {
    assertEquals(EntityFieldType.CONTAINER, containerFieldResolverProvider.getFieldTypes().get(0));
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResponseIsNull()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(null);

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenResourceHasNoContainer()
      throws RemoteInvocationException, URISyntaxException {
    var entityResponseMock = mock(EntityResponse.class);
    when(entityResponseMock.getAspects()).thenReturn(new EnvelopedAspectMap());
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
  }

  @Test
  public void shouldReturnEmptyFieldValueWhenThereIsAnException()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenThrow(new RemoteInvocationException());

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertTrue(result.getFieldValuesFuture().join().getValues().isEmpty());
  }

  @Test
  public void shouldReturnFieldValueWithContainerOfTheResource()
      throws RemoteInvocationException, URISyntaxException {

    var container = new Container().setContainer(Urn.createFromString(CONTAINER_URN));
    var entityResponseMock = mock(EntityResponse.class);
    var envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        CONTAINER_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(container.data())));
    when(entityResponseMock.getAspects()).thenReturn(envelopedAspectMap);
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    Map<Urn, Urn> parentByContainer = new LinkedHashMap<>();
    parentByContainer.put(Urn.createFromString(CONTAINER_URN), null);
    systemOperationContext = operationContextWithContainerParents(parentByContainer);

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertEquals(result.getFieldValuesFuture().join().getValues(), Set.of(CONTAINER_URN));
  }

  @Test
  public void shouldReturnFieldValueWithAncestorContainersOfTheResource()
      throws RemoteInvocationException, URISyntaxException {

    var container = new Container().setContainer(Urn.createFromString(CONTAINER_URN));
    var entityResponseMock = mock(EntityResponse.class);
    var envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        CONTAINER_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(container.data())));
    when(entityResponseMock.getAspects()).thenReturn(envelopedAspectMap);
    when(entityClientMock.getV2(
            eq(systemOperationContext),
            eq(DATASET_ENTITY_NAME),
            eq(Urn.createFromString(RESOURCE_URN)),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(entityResponseMock);

    Map<Urn, Urn> parentByContainer = new LinkedHashMap<>();
    parentByContainer.put(
        Urn.createFromString(CONTAINER_URN), Urn.createFromString(CONTAINER_PARENT_URN));
    parentByContainer.put(Urn.createFromString(CONTAINER_PARENT_URN), null);
    systemOperationContext = operationContextWithContainerParents(parentByContainer);

    var result =
        containerFieldResolverProvider.getFieldResolver(systemOperationContext, RESOURCE_SPEC);

    assertEquals(
        result.getFieldValuesFuture().join().getValues(),
        Set.of(CONTAINER_URN, CONTAINER_PARENT_URN));
  }

  @Test
  public void shouldReturnFieldValueAsContainerItself()
      throws RemoteInvocationException, URISyntaxException {

    Map<Urn, Urn> parentByContainer = new LinkedHashMap<>();
    parentByContainer.put(Urn.createFromString(CONTAINER_URN), null);
    systemOperationContext = operationContextWithContainerParents(parentByContainer);

    var result =
        containerFieldResolverProvider.getFieldResolver(
            systemOperationContext, new EntitySpec(CONTAINER_ENTITY_NAME, CONTAINER_URN));

    assertEquals(result.getFieldValuesFuture().join().getValues(), Set.of(CONTAINER_URN));
  }

  @Test
  public void shouldUseEntityGraphCacheWhenExpandSucceeds()
      throws RemoteInvocationException, URISyntaxException {
    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("container").source(GraphSnapshotSource.GRAPH).build();
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.CONTAINER))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("container"),
            eq(GraphSnapshotSource.GRAPH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(CONTAINER_URN)),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(GraphReadResult.fromVertices(Set.of(CONTAINER_URN, CONTAINER_PARENT_URN)));

    systemOperationContext =
        operationContextWithContainerParents(
            Map.of(), entityGraphCache, mock(AspectRetriever.class));

    var result =
        containerFieldResolverProvider.getFieldResolver(
            systemOperationContext, new EntitySpec(CONTAINER_ENTITY_NAME, CONTAINER_URN));

    assertEquals(
        result.getFieldValuesFuture().join().getValues(),
        Set.of(CONTAINER_URN, CONTAINER_PARENT_URN));
    verify(entityClientMock, never()).getV2(any(), any(), any(), any());
  }

  private static OperationContext operationContextWithContainerParents(
      Map<Urn, Urn> parentByContainer) {
    return operationContextWithContainerParents(parentByContainer, EntityGraphCache.NO_OP, null);
  }

  private static OperationContext operationContextWithContainerParents(
      Map<Urn, Urn> parentByContainer,
      EntityGraphCache entityGraphCache,
      AspectRetriever aspectRetrieverOverride) {
    AspectRetriever aspectRetriever =
        aspectRetrieverOverride != null ? aspectRetrieverOverride : mock(AspectRetriever.class);
    if (aspectRetrieverOverride == null) {
      when(aspectRetriever.getLatestAspectObjects(any(), any(), eq(Set.of(CONTAINER_ASPECT_NAME))))
          .thenAnswer(
              invocation -> {
                Set<Urn> urns = invocation.getArgument(1);
                Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
                for (Urn urn : urns) {
                  Urn parent = parentByContainer.get(urn);
                  if (parent != null) {
                    result.put(
                        urn,
                        Map.of(
                            CONTAINER_ASPECT_NAME,
                            new Aspect(new Container().setContainer(parent).data())));
                  } else if (parentByContainer.containsKey(urn)) {
                    result.put(
                        urn, Map.of(CONTAINER_ASPECT_NAME, new Aspect(new Container().data())));
                  }
                }
                return result;
              });
    }

    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(aspectRetriever)
            .entityGraphCache(entityGraphCache)
            .build();
    return base.toBuilder()
        .retrieverContext(retrieverContext)
        .build(base.getSessionAuthentication(), false);
  }
}
