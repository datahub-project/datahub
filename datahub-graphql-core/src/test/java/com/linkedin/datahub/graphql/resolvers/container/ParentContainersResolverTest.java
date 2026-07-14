package com.linkedin.datahub.graphql.resolvers.container;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.entity.Aspect;
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

    ParentContainersResolver resolver = new ParentContainersResolver();
    ParentContainersResult result = resolver.get(mockEnv).get();

    // The resolver now returns lightweight Container stubs (urn only); full hydration is
    // deferred to the batch loader wired on the ParentContainersResult type.
    assertEquals(result.getCount(), 2);
    assertEquals(result.getContainers().get(0).getUrn(), parentContainer1.toString());
    assertEquals(result.getContainers().get(1).getUrn(), parentContainer2.toString());
  }
}
