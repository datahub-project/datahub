package com.linkedin.datahub.graphql.resolvers.container;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CONTAINER_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMode;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ParentContainersResolverTest {
  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getMaxParentDepth()).thenReturn(50);

    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    Urn parentContainer1 = Urn.createFromString("urn:li:container:test-container");
    Urn parentContainer2 = Urn.createFromString("urn:li:container:test-container2");

    EntityGraphCache entityGraphCache = Mockito.mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("container").source(GraphSnapshotSource.GRAPH).build();
    Mockito.when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.CONTAINER))
        .thenReturn(Optional.of(binding));
    Mockito.when(
            entityGraphCache.walkOrderedForwardAncestors(
                eq("container"),
                eq(GraphSnapshotSource.GRAPH),
                eq(datasetUrn.toString()),
                eq(50),
                eq(ReadMode.CACHED)))
        .thenReturn(
            AncestorWalkResult.fromAncestors(
                List.of(parentContainer1.toString(), parentContainer2.toString())));

    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(
            aspectRetriever.getLatestAspectObjects(any(), any(), eq(Set.of(CONTAINER_ASPECT_NAME))))
        .thenAnswer(
            invocation -> {
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              result.put(
                  datasetUrn,
                  Map.of(
                      CONTAINER_ASPECT_NAME,
                      new Aspect(new Container().setContainer(parentContainer1).data())));
              result.put(
                  parentContainer1,
                  Map.of(
                      CONTAINER_ASPECT_NAME,
                      new Aspect(new Container().setContainer(parentContainer2).data())));
              result.put(
                  parentContainer2,
                  Map.of(CONTAINER_ASPECT_NAME, new Aspect(new Container().data())));
              return result;
            });

    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(aspectRetriever)
            .entityGraphCache(entityGraphCache)
            .build();
    OperationContext operationContext =
        base.toBuilder()
            .retrieverContext(retrieverContext)
            .build(base.getSessionAuthentication(), false);
    Mockito.when(mockContext.getOperationContext()).thenReturn(operationContext);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset datasetEntity = new Dataset();
    datasetEntity.setUrn(datasetUrn.toString());
    datasetEntity.setType(EntityType.DATASET);
    Mockito.when(mockEnv.getSource()).thenReturn(datasetEntity);

    final Container parentContainer2Aspect =
        new Container().setContainer(Urn.createFromString("urn:li:container:test-container2"));

    Map<String, EnvelopedAspect> parentContainer1Aspects = new HashMap<>();
    parentContainer1Aspects.put(
        CONTAINER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(new ContainerProperties().setName("test_schema").data())));
    parentContainer1Aspects.put(
        CONTAINER_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parentContainer2Aspect.data())));

    Map<String, EnvelopedAspect> parentContainer2Aspects = new HashMap<>();
    parentContainer2Aspects.put(
        CONTAINER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(new ContainerProperties().setName("test_database").data())));

    Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    batchResponse.put(
        parentContainer1,
        new EntityResponse()
            .setEntityName(CONTAINER_ENTITY_NAME)
            .setUrn(parentContainer1)
            .setAspects(new EnvelopedAspectMap(parentContainer1Aspects)));
    batchResponse.put(
        parentContainer2,
        new EntityResponse()
            .setEntityName(CONTAINER_ENTITY_NAME)
            .setUrn(parentContainer2)
            .setAspects(new EnvelopedAspectMap(parentContainer2Aspects)));

    Mockito.when(
            mockClient.batchGetV2(
                any(), Mockito.eq(CONTAINER_ENTITY_NAME), any(), Mockito.eq(null)))
        .thenReturn(batchResponse);

    ParentContainersResolver resolver = new ParentContainersResolver(mockClient);
    ParentContainersResult result = resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .batchGetV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    assertEquals(result.getCount(), 2);
    assertEquals(result.getContainers().get(0).getUrn(), parentContainer1.toString());
    assertEquals(result.getContainers().get(1).getUrn(), parentContainer2.toString());
  }
}
