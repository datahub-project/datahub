package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.metadata.Constants.APPLICATION_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Authentication;
import com.linkedin.application.ApplicationProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Application;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ParentApplicationsResult;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ParentApplicationsResolverTest {

  @Test
  public void testWalksApplicationPartOfChain() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getMaxParentDepth()).thenReturn(50);

    Urn appUrn = Urn.createFromString("urn:li:application:child");
    Urn parentApp1 = Urn.createFromString("urn:li:application:parent1");
    Urn parentApp2 = Urn.createFromString("urn:li:application:parent2");

    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestAspectObjects(
            any(), any(), eq(Set.of(APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenAnswer(
            invocation -> {
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              result.put(
                  appUrn,
                  Map.of(
                      APPLICATION_PROPERTIES_ASPECT_NAME,
                      new Aspect(
                          new ApplicationProperties().setParentApplication(parentApp1).data())));
              result.put(
                  parentApp1,
                  Map.of(
                      APPLICATION_PROPERTIES_ASPECT_NAME,
                      new Aspect(
                          new ApplicationProperties().setParentApplication(parentApp2).data())));
              result.put(
                  parentApp2,
                  Map.of(
                      APPLICATION_PROPERTIES_ASPECT_NAME,
                      new Aspect(new ApplicationProperties().data())));
              return result;
            });

    Mockito.when(mockContext.getOperationContext()).thenReturn(operationContext(aspectRetriever));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Application appEntity = new Application();
    appEntity.setUrn(appUrn.toString());
    appEntity.setType(EntityType.APPLICATION);
    Mockito.when(mockEnv.getSource()).thenReturn(appEntity);

    ParentApplicationsResolver resolver = new ParentApplicationsResolver();
    ParentApplicationsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 2);
    assertEquals(result.getApplications().get(0).getUrn(), parentApp1.toString());
    assertEquals(result.getApplications().get(1).getUrn(), parentApp2.toString());
  }

  @Test
  public void testReturnsEmptyWhenNoParentApplication() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getMaxParentDepth()).thenReturn(50);

    Urn appUrn = Urn.createFromString("urn:li:application:standalone");

    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestAspectObjects(
            any(), any(), eq(Set.of(APPLICATION_PROPERTIES_ASPECT_NAME))))
        .thenAnswer(
            invocation -> {
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              result.put(
                  appUrn,
                  Map.of(
                      APPLICATION_PROPERTIES_ASPECT_NAME,
                      new Aspect(new ApplicationProperties().data())));
              return result;
            });

    Mockito.when(mockContext.getOperationContext()).thenReturn(operationContext(aspectRetriever));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Application appEntity = new Application();
    appEntity.setUrn(appUrn.toString());
    appEntity.setType(EntityType.APPLICATION);
    Mockito.when(mockEnv.getSource()).thenReturn(appEntity);

    ParentApplicationsResolver resolver = new ParentApplicationsResolver();
    ParentApplicationsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 0);
    assertTrue(result.getApplications().isEmpty());
  }

  /**
   * No known-graph cache exists for Application yet (see HierarchyBindings#applicationSpec), so
   * production always resolves this via the aspect walker, not the graph cache. NO_OP matches that
   * reality without needing to mock cache hits/misses.
   */
  private static OperationContext operationContext(AspectRetriever aspectRetriever) {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .aspectRetriever(aspectRetriever)
            .entityGraphCache(EntityGraphCache.NO_OP)
            .build();
    return base.toBuilder()
        .retrieverContext(retrieverContext)
        .build(base.getSessionAuthentication(), false);
  }
}
