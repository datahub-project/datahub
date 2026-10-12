package com.linkedin.datahub.graphql.util;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DatasetKey;
import graphql.language.Field;
import graphql.language.SelectionSet;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Cross-dispatch regression: DataLoader caching is keyed by URN by default. A first dispatch that
 * hydrates URN X under {@link AspectLoadContext}{A} must not satisfy a later same-request dispatch
 * that needs disjoint {C}. The production loader from {@link GmsGraphQLEngine#loaderSuppliers} must
 * use a context-aware cache key so the second mapped field is populated.
 */
public class AspectLoadCrossDispatchCacheTest {

  private static final String URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.cross_dispatch,PROD)";

  private static final class AccumulatingContext implements QueryContext {
    private final ConcurrentHashMap<String, AspectLoadContext> aspectLoadContexts =
        new ConcurrentHashMap<>();
    private final QueryContext delegate;
    private final AspectMappingRegistry registry;

    AccumulatingContext(QueryContext delegate, AspectMappingRegistry registry) {
      this.delegate = delegate;
      this.registry = registry;
    }

    @Override
    public boolean isAuthenticated() {
      return delegate.isAuthenticated();
    }

    @Override
    public com.datahub.authentication.Authentication getAuthentication() {
      return delegate.getAuthentication();
    }

    @Override
    public com.datahub.plugins.auth.authorization.Authorizer getAuthorizer() {
      return delegate.getAuthorizer();
    }

    @Override
    public io.datahubproject.metadata.context.OperationContext getOperationContext() {
      return delegate.getOperationContext();
    }

    @Override
    public com.linkedin.metadata.config.DataHubAppConfiguration getDataHubAppConfig() {
      return delegate.getDataHubAppConfig();
    }

    @Override
    public int getMaxParentDepth() {
      return delegate.getMaxParentDepth();
    }

    @Override
    public AspectMappingRegistry getAspectMappingRegistry() {
      return registry;
    }

    @Override
    public void setAspectMappingRegistry(AspectMappingRegistry aspectMappingRegistry) {}

    @Override
    public void mergeAspectLoadContext(String entityTypeName, AspectLoadContext loadContext) {
      aspectLoadContexts.merge(entityTypeName, loadContext, AspectLoadContext::union);
    }

    @Override
    public AspectLoadContext getAspectLoadContext(String entityTypeName) {
      return aspectLoadContexts.get(entityTypeName);
    }
  }

  /**
   * BatchLoadUtils (entities(urns:), browse, autocomplete, siblings) widens the request union to
   * FETCH_ALL, but that alone is not enough: without a key context its loads use the key-only
   * DataLoader cache key, so a prior dispatch of the same URN under a narrower union would be
   * served from cache and skip batchLoad entirely. The FETCH_ALL key context must ride along so the
   * widened load misses the stale entry and re-dispatches with the full aspect set.
   */
  @Test
  public void testBatchLoadUtilsAfterNarrowDispatchDoesNotReuseStaleCache() throws Exception {
    AspectMappingRegistry mappingRegistry = mock(AspectMappingRegistry.class);
    when(mappingRegistry.getRequiredAspectsForFieldNames(eq("Dataset"), eq(Set.of("ownership"))))
        .thenReturn(ImmutableSet.of(Constants.OWNERSHIP_ASPECT_NAME));

    AccumulatingContext context = new AccumulatingContext(getMockAllowContext(), mappingRegistry);
    EntityClient entityClient = mock(EntityClient.class);
    Urn urn = Urn.createFromString(URN);

    // Return only the aspects that were requested for this batchGetV2 call.
    when(entityClient.batchGetV2(any(), eq(Constants.DATASET_ENTITY_NAME), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<String> requested = (Set<String>) invocation.getArgument(3);
              return Map.of(urn, datasetResponse(urn, requested));
            });

    DatasetType datasetType = new DatasetType(entityClient);
    Map<String, java.util.function.Function<QueryContext, DataLoader<?, ?>>> suppliers =
        com.linkedin.datahub.graphql.GmsGraphQLEngine.loaderSuppliers(List.of(datasetType));
    @SuppressWarnings("unchecked")
    DataLoader<String, ?> loader = (DataLoader<String, ?>) suppliers.get("Dataset").apply(context);
    org.dataloader.DataLoaderRegistry registry = new org.dataloader.DataLoaderRegistry();
    registry.register("Dataset", loader);

    // Dispatch 1: a typed selection needing only ownership hydrates and caches under a narrow key.
    LoadableTypeResolver<Dataset, String> resolver =
        new LoadableTypeResolver<>(datasetType, e -> URN);
    CompletableFuture<Dataset> narrowFuture =
        resolver.get(envWithSelection(context, registry, "ownership", "ownership"));
    loader.dispatch();
    assertNotNull(narrowFuture.get().getOwnership());

    // Dispatch 2: the opaque batch path loads the same URN and must not reuse the narrow result.
    Dataset stub = new Dataset();
    stub.setUrn(URN);
    stub.setType(com.linkedin.datahub.graphql.generated.EntityType.DATASET);
    CompletableFuture<List<com.linkedin.datahub.graphql.generated.Entity>> batchFuture =
        com.linkedin.datahub.graphql.resolvers.BatchLoadUtils.batchLoadEntitiesOfSameType(
            List.of(stub), List.of(datasetType), registry, context);
    loader.dispatch();
    Dataset fromBatch = (Dataset) unwrap(batchFuture.get().get(0));

    assertNotNull(
        fromBatch.getTags(),
        "batch path must re-dispatch with the widened union, not serve the ownership-only cached"
            + " result");
    assertTrue(
        Mockito.mockingDetails(entityClient).getInvocations().stream()
                .filter(i -> i.getMethod().getName().equals("batchGetV2"))
                .count()
            >= 2,
        "expected a second batchGetV2 for the FETCH_ALL key context");
  }

  private static Object unwrap(Object element) {
    if (element instanceof graphql.execution.DataFetcherResult) {
      return ((graphql.execution.DataFetcherResult<?>) element).getData();
    }
    return element;
  }

  @Test
  public void testSecondDispatchDisjointAspectUsesProductionLoaderNotStaleCache() throws Exception {
    AspectMappingRegistry mappingRegistry = mock(AspectMappingRegistry.class);
    when(mappingRegistry.getRequiredAspectsForFieldNames(eq("Dataset"), eq(Set.of("ownership"))))
        .thenReturn(ImmutableSet.of(Constants.OWNERSHIP_ASPECT_NAME));
    when(mappingRegistry.getRequiredAspectsForFieldNames(eq("Dataset"), eq(Set.of("tags"))))
        .thenReturn(ImmutableSet.of(Constants.GLOBAL_TAGS_ASPECT_NAME));

    AccumulatingContext context = new AccumulatingContext(getMockAllowContext(), mappingRegistry);
    EntityClient entityClient = mock(EntityClient.class);
    Urn urn = Urn.createFromString(URN);

    // Return only the aspects that were requested for this batchGetV2 call.
    when(entityClient.batchGetV2(any(), eq(Constants.DATASET_ENTITY_NAME), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<String> requested = (Set<String>) invocation.getArgument(3);
              return Map.of(urn, datasetResponse(urn, requested));
            });

    DatasetType datasetType = new DatasetType(entityClient);
    Map<String, Function<QueryContext, DataLoader<?, ?>>> suppliers =
        GmsGraphQLEngine.loaderSuppliers(List.of(datasetType));
    @SuppressWarnings("unchecked")
    DataLoader<String, ?> loader = (DataLoader<String, ?>) suppliers.get("Dataset").apply(context);

    DataLoaderRegistry registry = new DataLoaderRegistry();
    registry.register("Dataset", loader);

    LoadableTypeResolver<Dataset, String> resolver =
        new LoadableTypeResolver<>(datasetType, e -> URN);

    // Dispatch 1: ownership only.
    DataFetchingEnvironment envOwnership =
        envWithSelection(context, registry, "ownership", "ownership");
    CompletableFuture<Dataset> futureOwnership = resolver.get(envOwnership);
    loader.dispatch();
    Dataset first = futureOwnership.get();
    assertNotNull(first.getOwnership(), "first dispatch should hydrate ownership");
    assertNull(first.getTags(), "first dispatch must not include tags aspect");

    // Dispatch 2: disjoint globalTags for the same URN in the same request / loader.
    DataFetchingEnvironment envTags = envWithSelection(context, registry, "tags", "tags");
    CompletableFuture<Dataset> futureTags = resolver.get(envTags);
    loader.dispatch();
    Dataset second = futureTags.get();

    assertNotNull(
        second.getTags(),
        "second dispatch must not reuse the URN-cached under-hydrated result; tags should be"
            + " populated");
    assertTrue(
        Mockito.mockingDetails(entityClient).getInvocations().stream()
                .filter(i -> i.getMethod().getName().equals("batchGetV2"))
                .count()
            >= 2,
        "expected a second batchGetV2 after cache miss for disjoint AspectLoadContext");
  }

  /**
   * Builds an environment whose query AST selects {@code selectedFieldName} on the resolved entity.
   * Resolvers read immediate selections from the AST, so the selection must be expressed there.
   */
  private DataFetchingEnvironment envWithSelection(
      QueryContext context, DataLoaderRegistry registry, String selectedFieldName, String label) {
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(context);
    when(env.getDataLoaderRegistry()).thenReturn(registry);
    when(env.getField())
        .thenReturn(
            Field.newField()
                .name("entity")
                .selectionSet(
                    SelectionSet.newSelectionSet()
                        .selection(Field.newField().name(selectedFieldName).build())
                        .build())
                .build());
    when(env.toString()).thenReturn("DFE:" + label);
    return env;
  }

  private EntityResponse datasetResponse(Urn urn, Set<String> requested) {
    DatasetKey key =
        new DatasetKey()
            .setPlatform(Urn.createFromTuple("dataPlatform", "mysql"))
            .setName("my_db.cross_dispatch")
            .setOrigin(com.linkedin.common.FabricType.PROD);

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    // Key aspect is always included by DatasetType optimization.
    if (requested == null
        || requested.contains(Constants.DATASET_KEY_ASPECT_NAME)
        || requested.contains("datasetKey")) {
      aspects.put(
          Constants.DATASET_KEY_ASPECT_NAME,
          new EnvelopedAspect().setValue(new Aspect(key.data())));
    }
    if (requested == null || requested.contains(Constants.OWNERSHIP_ASPECT_NAME)) {
      Ownership ownership =
          new Ownership()
              .setOwners(new OwnerArray())
              .setLastModified(
                  new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn("urn:li:corpuser:test")));
      aspects.put(
          Constants.OWNERSHIP_ASPECT_NAME,
          new EnvelopedAspect().setValue(new Aspect(ownership.data())));
    }
    if (requested == null || requested.contains(Constants.GLOBAL_TAGS_ASPECT_NAME)) {
      GlobalTags tags = new GlobalTags().setTags(new TagAssociationArray());
      aspects.put(
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          new EnvelopedAspect().setValue(new Aspect(tags.data())));
    }

    return new EntityResponse()
        .setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(urn)
        .setAspects(new EnvelopedAspectMap(aspects));
  }
}
