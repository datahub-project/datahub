package com.linkedin.datahub.graphql.resolvers;

import static com.linkedin.datahub.graphql.ViewAuthorizationTestContexts.queryContextHiding;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.MissingEntityResults;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityIncidentsResult;
import com.linkedin.datahub.graphql.generated.EntityRelationshipsResult;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.datahub.graphql.generated.HealthStatusType;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResults;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.browse.EntityBrowsePathsResolver;
import com.linkedin.datahub.graphql.resolvers.container.ParentContainersResolver;
import com.linkedin.datahub.graphql.resolvers.domain.DomainEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.health.EntityHealthResolver;
import com.linkedin.datahub.graphql.resolvers.incident.EntityIncidentsResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityRelationshipsResultResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchAcrossLineageResolver;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.datahub.graphql.types.view.DataHubViewType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Fields that read stores other than the aspect table answer for an entity the caller can't view as
 * for one that doesn't exist, and lookups give such an entity exactly the answer of a missing one.
 */
public class UnviewableSourceResolversTest {

  private static final Urn HIDDEN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,hidden,PROD)");
  private static final Urn MISSING =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,missing,PROD)");
  private static final Urn HIDDEN_VIEW = UrnUtils.getUrn("urn:li:dataHubView:hidden");
  private static final Urn MISSING_VIEW = UrnUtils.getUrn("urn:li:dataHubView:missing");

  @Test
  public void testRelationshipsOfHiddenSourceAreEmpty() throws Exception {
    GraphClient graphClient = mock(GraphClient.class);

    EntityRelationshipsResult result =
        new EntityRelationshipsResultResolver(graphClient)
            .get(
                environment(
                    datasetSource(HIDDEN),
                    ImmutableMap.of(
                        "input",
                        ImmutableMap.of(
                            "types",
                            List.of("DownstreamOf"),
                            "direction",
                            "INCOMING",
                            "start",
                            5,
                            "count",
                            10))))
            .get();

    assertEquals((int) result.getTotal(), 0);
    assertEquals((int) result.getStart(), 5);
    assertTrue(result.getRelationships().isEmpty());
    verifyNoInteractions(graphClient);
  }

  @Test
  public void testIncidentsOfHiddenSourceAreEmpty() throws Exception {
    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.batchGetV2(any(), anyString(), any(), any())).thenReturn(Map.of());

    EntityIncidentsResult result =
        new EntityIncidentsResolver(entityClient)
            .get(environment(datasetSource(HIDDEN), Map.of()))
            .get();

    assertEquals((int) result.getTotal(), 0);
    assertTrue(result.getIncidents().isEmpty());
    verify(entityClient, never()).filter(any(), anyString(), any(), any(), anyInt(), anyInt());
  }

  @Test
  public void testBrowsePathsAndParentContainersOfHiddenSourceAreEmpty() throws Exception {
    BrowsableEntityType<?, ?> browsableType = mock(BrowsableEntityType.class);
    EntityClient entityClient = mock(EntityClient.class);

    assertTrue(
        new EntityBrowsePathsResolver(browsableType)
            .get(environment(datasetSource(HIDDEN), Map.of()))
            .get()
            .isEmpty());
    ParentContainersResult parents =
        new ParentContainersResolver(entityClient, null)
            .get(environment(datasetSource(HIDDEN), Map.of()))
            .get();

    assertEquals((int) parents.getCount(), 0);
    verifyNoInteractions(browsableType, entityClient);
  }

  @Test
  public void testEntitiesOfHiddenDomainAreEmpty() throws Exception {
    EntityClient entityClient = mock(EntityClient.class);
    Urn hiddenDomain = UrnUtils.getUrn("urn:li:domain:hidden");
    Domain source = new Domain();
    source.setUrn(hiddenDomain.toString());

    SearchResults result =
        new DomainEntitiesResolver(entityClient, null)
            .get(environment(source, Map.of(), hiddenDomain))
            .get();

    assertEquals((int) result.getTotal(), 0);
    verifyNoInteractions(entityClient);
  }

  @Test
  public void testLineageSearchFromHiddenSourceIsEmpty() throws Exception {
    EntityClient entityClient = mock(EntityClient.class);
    EntityRegistry registry = TestOperationContexts.defaultEntityRegistry();

    SearchAcrossLineageResults result =
        new SearchAcrossLineageResolver(entityClient, registry, null)
            .get(
                environment(
                    null,
                    ImmutableMap.of(
                        "input",
                        ImmutableMap.of("urn", HIDDEN.toString(), "direction", "DOWNSTREAM"))))
            .get();

    assertEquals((int) result.getTotal(), 0);
    assertTrue(result.getSearchResults().isEmpty());
    verifyNoInteractions(entityClient);
  }

  @Test
  public void testHealthOfHiddenSourceIsAMissingEntitysHealth() throws Exception {
    EntityClient entityClient = mock(EntityClient.class);
    GraphClient graphClient = mock(GraphClient.class);
    TimeseriesAspectService timeseries = mock(TimeseriesAspectService.class);

    List<Health> health =
        new EntityHealthResolver(entityClient, graphClient, timeseries)
            .get(environment(datasetSource(HIDDEN), Map.of()))
            .get();

    assertEquals(health.size(), 1);
    assertEquals(health.get(0).getType(), HealthStatusType.INCIDENTS);
    assertEquals(health.get(0).getStatus(), HealthStatus.PASS);
    verifyNoInteractions(entityClient, graphClient, timeseries);
  }

  @Test
  public void testIncidentsOfVisibleSourceAreRead() throws Exception {
    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.filter(any(), anyString(), any(), anyList(), anyInt(), anyInt()))
        .thenReturn(MissingEntityResults.searchResult(0, 20));
    when(entityClient.batchGetV2(any(), anyString(), any(), any())).thenReturn(Map.of());

    new EntityIncidentsResolver(entityClient)
        .get(environment(datasetSource(MISSING), Map.of()))
        .get();

    verify(entityClient).filter(any(), eq("incident"), any(), anyList(), anyInt(), anyInt());
  }

  /** The gate answers both with only the key aspect; the GraphQL layer must not tell them apart. */
  @Test
  public void testHiddenDatasetLoadsLikeAMissingOne() throws Exception {
    EntityRegistry registry = TestOperationContexts.defaultEntityRegistry();
    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.batchGetV2(any(), eq("dataset"), any(), any()))
        .thenReturn(
            Map.of(
                HIDDEN, keyOnly(registry, HIDDEN),
                MISSING, keyOnly(registry, MISSING)));

    List<Dataset> loaded =
        new DatasetType(entityClient)
                .batchLoad(
                    List.of(HIDDEN.toString(), MISSING.toString()), queryContextHiding(HIDDEN))
                .stream()
                .map(result -> result.getData())
                .toList();

    assertEquals(loaded.get(0).getName(), "hidden");
    assertEquals(loaded.get(1).getName(), "missing");
    assertEquals(loaded.get(0).getPlatform().getUrn(), loaded.get(1).getPlatform().getUrn());
    assertNull(loaded.get(0).getProperties());
    assertNull(loaded.get(1).getProperties());
  }

  @Test
  public void testHiddenViewLoadsLikeAMissingOne() throws Exception {
    EntityRegistry registry = TestOperationContexts.defaultEntityRegistry();
    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.batchGetV2(any(), eq("dataHubView"), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(2);
              assertTrue(urns.contains(HIDDEN_VIEW), "A query loads the hidden view as missing");
              return Map.of(
                  HIDDEN_VIEW, keyOnly(registry, HIDDEN_VIEW),
                  MISSING_VIEW, keyOnly(registry, MISSING_VIEW));
            });

    List<DataHubView> loaded =
        new DataHubViewType(entityClient)
                .batchLoad(
                    List.of(HIDDEN_VIEW.toString(), MISSING_VIEW.toString()),
                    queryContextHiding(HIDDEN_VIEW))
                .stream()
                .map(result -> result == null ? null : result.getData())
                .toList();

    assertNotNull(loaded.get(0));
    assertNotNull(loaded.get(1));
    assertEquals(loaded.get(0).getName(), loaded.get(1).getName());
    assertEquals(loaded.get(0).getDefinition(), loaded.get(1).getDefinition());
  }

  private static EntityResponse keyOnly(EntityRegistry registry, Urn urn) {
    AspectSpec keySpec = registry.getEntitySpec(urn.getEntityType()).getKeyAspectSpec();
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(urn.getEntityType())
        .setAspects(
            new EnvelopedAspectMap(
                Map.of(
                    keySpec.getName(),
                    new EnvelopedAspect()
                        .setName(keySpec.getName())
                        .setValue(
                            new Aspect(
                                EntityKeyUtils.convertUrnToEntityKey(urn, keySpec).data())))));
  }

  private static Dataset datasetSource(Urn urn) {
    Dataset source = new Dataset();
    source.setUrn(urn.toString());
    return source;
  }

  private static DataFetchingEnvironment environment(Object source, Map<String, Object> arguments) {
    return environment(source, arguments, HIDDEN);
  }

  private static DataFetchingEnvironment environment(
      Object source, Map<String, Object> arguments, Urn hidden) {
    QueryContext context = queryContextHiding(hidden);
    DataFetchingEnvironment environment = mock(DataFetchingEnvironment.class);
    when(environment.getContext()).thenReturn(context);
    when(environment.getGraphQlContext()).thenReturn(null);
    when(environment.getSource()).thenReturn(source);
    arguments.forEach((name, value) -> when(environment.getArgument(name)).thenReturn(value));
    when(environment.getArgumentOrDefault(anyString(), any()))
        .thenAnswer(
            invocation ->
                arguments.getOrDefault(invocation.getArgument(0), invocation.getArgument(1)));
    return environment;
  }
}
