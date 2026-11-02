package com.linkedin.datahub.graphql.util;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.resolvers.load.EntityTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DatasetKey;
import graphql.execution.DataFetcherResult;
import graphql.language.Field;
import graphql.language.SelectionSet;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;
import org.dataloader.DataLoaderRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Regression: aliased sibling / batched fragment loads must union aspect selections so both fields
 * populate. Request-scoped {@link AspectLoadContext} accumulation + DataLoader key contexts.
 */
public class AspectLoadBatchUnionTest {

  private static final String URN_A =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.table_a,PROD)";
  private static final String URN_B =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.table_b,PROD)";

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

  private EntityResponse datasetResponse(Urn urn) {
    DatasetKey key =
        new DatasetKey()
            .setPlatform(Urn.createFromTuple("dataPlatform", "mysql"))
            .setName(urn.getId())
            .setOrigin(com.linkedin.common.FabricType.PROD);
    return new EntityResponse()
        .setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(urn)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    Constants.DATASET_KEY_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(key.data())))));
  }

  /**
   * OwnerTypeResolver must contribute its selection to the request-scoped union and pass it as the
   * DataLoader key context, like LoadableTypeResolver. Before the fix it loaded by bare URN, so a
   * narrower CorpUser union accumulated by an earlier selection dictated the fetch and owner fields
   * (e.g. editableProperties.displayName) silently came back null.
   */
  @Test
  public void testOwnerResolverContributesSelectionToUnion() throws Exception {
    AspectMappingRegistry mappingRegistry = mock(AspectMappingRegistry.class);
    when(mappingRegistry.getRequiredAspectsForFieldNames(
            eq("CorpUser"), eq(Set.of("editableProperties"))))
        .thenReturn(ImmutableSet.of("corpUserEditableInfo"));

    AccumulatingContext context = new AccumulatingContext(getMockAllowContext(), mappingRegistry);
    // A prior selection left a narrow CorpUser union in the request.
    context.mergeAspectLoadContext("CorpUser", AspectLoadContext.of(ImmutableSet.of("status")));

    Urn userUrn = Urn.createFromString("urn:li:corpuser:owner_test_user");
    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.batchGetV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                userUrn,
                new EntityResponse()
                    .setEntityName(Constants.CORP_USER_ENTITY_NAME)
                    .setUrn(userUrn)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.CORP_USER_KEY_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(
                                        new Aspect(
                                            new com.linkedin.metadata.key.CorpUserKey()
                                                .setUsername("owner_test_user")
                                                .data())))))));

    com.linkedin.datahub.graphql.types.corpuser.CorpUserType corpUserType =
        new com.linkedin.datahub.graphql.types.corpuser.CorpUserType(entityClient, null);
    BatchLoaderContextProvider provider = () -> context;
    DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);
    DataLoader<String, Object> loader =
        DataLoader.newDataLoader(
            (keys, env) -> {
              AspectLoadContext fromKeys = AspectUtils.unionKeyContexts(env.getKeyContextsList());
              if (fromKeys != null) {
                context.mergeAspectLoadContext("CorpUser", fromKeys);
              }
              try {
                return CompletableFuture.completedFuture(
                    (List<Object>) (List<?>) corpUserType.batchLoad(keys, context));
              } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
              }
            },
            options);
    DataLoaderRegistry registry = new DataLoaderRegistry();
    registry.register("CorpUser", loader);

    com.linkedin.datahub.graphql.generated.CorpUser stub =
        new com.linkedin.datahub.graphql.generated.CorpUser();
    stub.setUrn(userUrn.toString());
    stub.setType(com.linkedin.datahub.graphql.generated.EntityType.CORP_USER);

    DataFetchingEnvironment env =
        envWithSelection(context, registry, "editableProperties", "owner");
    com.linkedin.datahub.graphql.resolvers.load.OwnerTypeResolver<Object> resolver =
        new com.linkedin.datahub.graphql.resolvers.load.OwnerTypeResolver<>(
            List.of(corpUserType), e -> stub);

    CompletableFuture<Object> future = resolver.get(env);
    loader.dispatch();
    future.get();

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(entityClient)
        .batchGetV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), aspectsCaptor.capture());
    Set<String> fetched = new HashSet<>(aspectsCaptor.getValue());

    assertTrue(
        fetched.contains("corpUserEditableInfo"),
        "owner resolution must widen the narrow {status} union with its own selection; got: "
            + fetched);
  }

  @Test
  public void testAliasedSiblingDisjointSelectionsUnionFetched() throws Exception {
    AspectMappingRegistry mappingRegistry = mock(AspectMappingRegistry.class);
    when(mappingRegistry.getRequiredAspectsForFieldNames(eq("Dataset"), eq(Set.of("ownership"))))
        .thenReturn(ImmutableSet.of("ownership"));
    when(mappingRegistry.getRequiredAspectsForFieldNames(eq("Dataset"), eq(Set.of("platform"))))
        .thenReturn(ImmutableSet.of("dataPlatformInstance"));

    AccumulatingContext context = new AccumulatingContext(getMockAllowContext(), mappingRegistry);
    EntityClient entityClient = mock(EntityClient.class);
    Urn urn = Urn.createFromString(URN_A);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    when(entityClient.batchGetV2(any(), eq(Constants.DATASET_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of(urn, datasetResponse(urn)));

    DatasetType datasetType = new DatasetType(entityClient);
    BatchLoaderContextProvider provider = () -> context;
    DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);
    DataLoader<String, DataFetcherResult<Dataset>> loader =
        DataLoader.newDataLoader(
            (keys, env) -> {
              AspectLoadContext fromKeys = AspectUtils.unionKeyContexts(env.getKeyContextsList());
              if (fromKeys != null) {
                context.mergeAspectLoadContext("Dataset", fromKeys);
              }
              try {
                return CompletableFuture.completedFuture(datasetType.batchLoad(keys, context));
              } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
              }
            },
            options);

    DataLoaderRegistry registry = new DataLoaderRegistry();
    registry.register("Dataset", loader);

    DataFetchingEnvironment envA = envWithSelection(context, registry, "ownership", "ownership");
    DataFetchingEnvironment envB = envWithSelection(context, registry, "platform", "platform");

    LoadableTypeResolver<Dataset, String> resolverA =
        new LoadableTypeResolver<>(datasetType, e -> URN_A);
    LoadableTypeResolver<Dataset, String> resolverB =
        new LoadableTypeResolver<>(datasetType, e -> URN_A);

    CompletableFuture<?> futureA = resolverA.get(envA);
    CompletableFuture<?> futureB = resolverB.get(envB);
    loader.dispatch();
    futureA.get();
    futureB.get();

    Mockito.verify(entityClient)
        .batchGetV2(any(), eq(Constants.DATASET_ENTITY_NAME), any(), aspectsCaptor.capture());
    Set<String> fetched = new HashSet<>(aspectsCaptor.getValue());

    assertTrue(fetched.contains("ownership"), "missing ownership: " + fetched);
    assertTrue(fetched.contains("dataPlatformInstance"), "missing platform: " + fetched);
    assertTrue(fetched.contains("datasetKey"), "missing key: " + fetched);
  }

  @Test
  public void testBatchedSearchFragmentSelectionsUnionFetched() throws Exception {
    AspectMappingRegistry mappingRegistry = mock(AspectMappingRegistry.class);
    when(mappingRegistry.getRequiredAspectsForFieldNames(eq("Dataset"), eq(Set.of("ownership"))))
        .thenReturn(ImmutableSet.of("ownership"));
    when(mappingRegistry.getRequiredAspectsForFieldNames(eq("Dataset"), eq(Set.of("tags"))))
        .thenReturn(ImmutableSet.of("globalTags"));

    AccumulatingContext context = new AccumulatingContext(getMockAllowContext(), mappingRegistry);
    EntityClient entityClient = mock(EntityClient.class);
    Urn urnA = Urn.createFromString(URN_A);
    Urn urnB = Urn.createFromString(URN_B);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    when(entityClient.batchGetV2(any(), eq(Constants.DATASET_ENTITY_NAME), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                urnA, datasetResponse(urnA),
                urnB, datasetResponse(urnB)));

    DatasetType datasetType = new DatasetType(entityClient);
    BatchLoaderContextProvider provider = () -> context;
    DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);
    DataLoader<String, DataFetcherResult<Dataset>> loader =
        DataLoader.newDataLoader(
            (keys, env) -> {
              AspectLoadContext fromKeys = AspectUtils.unionKeyContexts(env.getKeyContextsList());
              if (fromKeys != null) {
                context.mergeAspectLoadContext("Dataset", fromKeys);
              }
              try {
                return CompletableFuture.completedFuture(datasetType.batchLoad(keys, context));
              } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
              }
            },
            options);

    DataLoaderRegistry registry = new DataLoaderRegistry();
    registry.register("Dataset", loader);

    Dataset stubA = new Dataset();
    stubA.setUrn(URN_A);
    stubA.setType(com.linkedin.datahub.graphql.generated.EntityType.DATASET);
    Dataset stubB = new Dataset();
    stubB.setUrn(URN_B);
    stubB.setType(com.linkedin.datahub.graphql.generated.EntityType.DATASET);

    DataFetchingEnvironment envA = envWithSelection(context, registry, "ownership", "fragA");
    DataFetchingEnvironment envB = envWithSelection(context, registry, "tags", "fragB");

    EntityTypeResolver resolver =
        new EntityTypeResolver(List.of(datasetType), env -> env == envA ? stubA : stubB);

    CompletableFuture<?> futureA = resolver.get(envA);
    CompletableFuture<?> futureB = resolver.get(envB);
    loader.dispatch();
    futureA.get();
    futureB.get();

    Mockito.verify(entityClient)
        .batchGetV2(any(), eq(Constants.DATASET_ENTITY_NAME), any(), aspectsCaptor.capture());
    Set<String> fetched = new HashSet<>(aspectsCaptor.getValue());

    assertTrue(fetched.contains("ownership"), "missing ownership: " + fetched);
    assertTrue(fetched.contains("globalTags"), "missing globalTags: " + fetched);
    assertTrue(fetched.contains("datasetKey"), "missing key: " + fetched);
  }
}
