package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ParentDomainsResult;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
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

public class ParentDomainsResolverTest {
  @Test
  public void testGetSuccessForDomain() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getMaxParentDepth()).thenReturn(50);

    Urn domainUrn = Urn.createFromString("urn:li:domain:00005397daf94708a8822b8106cfd451");
    Urn parentDomain1 = Urn.createFromString("urn:li:domain:11115397daf94708a8822b8106cfd451");
    Urn parentDomain2 = Urn.createFromString("urn:li:domain:22225397daf94708a8822b8106cfd451");

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("domain").source(GraphSnapshotSource.SEARCH).build();
    when(entityGraphCache.bindingForPolicyField("DOMAIN")).thenReturn(Optional.empty());
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.DOMAIN))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(domainUrn.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(
                Set.of(domainUrn.toString(), parentDomain1.toString(), parentDomain2.toString())));
    when(entityGraphCache.walkOrderedForwardAncestors(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(domainUrn.toString()),
            eq(50),
            eq(ReadMode.CACHED)))
        .thenReturn(
            AncestorWalkResult.fromAncestors(
                List.of(parentDomain1.toString(), parentDomain2.toString())));

    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestAspectObjects(
            any(), any(), eq(Set.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenAnswer(
            invocation -> {
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              result.put(
                  domainUrn,
                  Map.of(
                      DOMAIN_PROPERTIES_ASPECT_NAME,
                      new Aspect(new DomainProperties().setParentDomain(parentDomain1).data())));
              result.put(
                  parentDomain1,
                  Map.of(
                      DOMAIN_PROPERTIES_ASPECT_NAME,
                      new Aspect(new DomainProperties().setParentDomain(parentDomain2).data())));
              result.put(
                  parentDomain2,
                  Map.of(DOMAIN_PROPERTIES_ASPECT_NAME, new Aspect(new DomainProperties().data())));
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

    Domain domainEntity = new Domain();
    domainEntity.setUrn(domainUrn.toString());
    domainEntity.setType(EntityType.DOMAIN);
    Mockito.when(mockEnv.getSource()).thenReturn(domainEntity);

    ParentDomainsResolver resolver = new ParentDomainsResolver();
    ParentDomainsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 2);
    assertEquals(result.getDomains().get(0).getUrn(), parentDomain1.toString());
    assertEquals(result.getDomains().get(1).getUrn(), parentDomain2.toString());
  }

  @Test
  public void testFallsBackToAspectsWhenCacheWalkEmpty() throws Exception {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getMaxParentDepth()).thenReturn(50);

    Urn domainUrn = Urn.createFromString("urn:li:domain:00005397daf94708a8822b8106cfd451");
    Urn parentDomain1 = Urn.createFromString("urn:li:domain:11115397daf94708a8822b8106cfd451");

    EntityGraphCache entityGraphCache = mock(EntityGraphCache.class);
    EntityGraphBinding binding =
        EntityGraphBinding.builder().graphId("domain").source(GraphSnapshotSource.SEARCH).build();
    when(entityGraphCache.bindingForPolicyField("DOMAIN")).thenReturn(Optional.empty());
    when(entityGraphCache.bindingForKnownGraph(KnownEntityGraph.DOMAIN))
        .thenReturn(Optional.of(binding));
    when(entityGraphCache.expand(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(domainUrn.toString())),
            anyInt(),
            eq(EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
            eq(ReadMode.CACHED)))
        .thenReturn(
            GraphReadResult.fromVertices(Set.of(domainUrn.toString(), parentDomain1.toString())));
    when(entityGraphCache.walkOrderedForwardAncestors(
            eq("domain"),
            eq(GraphSnapshotSource.SEARCH),
            eq(domainUrn.toString()),
            eq(50),
            eq(ReadMode.CACHED)))
        .thenReturn(AncestorWalkResult.miss(ReadMissReason.ABSENT));

    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestAspectObjects(
            any(), any(), eq(Set.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenAnswer(
            invocation -> {
              Map<Urn, Map<String, Aspect>> result = new LinkedHashMap<>();
              result.put(
                  domainUrn,
                  Map.of(
                      DOMAIN_PROPERTIES_ASPECT_NAME,
                      new Aspect(new DomainProperties().setParentDomain(parentDomain1).data())));
              result.put(
                  parentDomain1,
                  Map.of(DOMAIN_PROPERTIES_ASPECT_NAME, new Aspect(new DomainProperties().data())));
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

    Domain domainEntity = new Domain();
    domainEntity.setUrn(domainUrn.toString());
    domainEntity.setType(EntityType.DOMAIN);
    Mockito.when(mockEnv.getSource()).thenReturn(domainEntity);

    ParentDomainsResolver resolver = new ParentDomainsResolver();
    ParentDomainsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCount(), 1);
    assertEquals(result.getDomains().get(0).getUrn(), parentDomain1.toString());
  }
}
