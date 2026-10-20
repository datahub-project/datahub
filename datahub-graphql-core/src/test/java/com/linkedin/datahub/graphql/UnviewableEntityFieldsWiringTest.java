package com.linkedin.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.EnumDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.util.EntityResponseUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.config.GraphQLConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLMetricsConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import graphql.GraphQLContext;
import graphql.execution.DataFetcherResult;
import graphql.execution.MergedField;
import graphql.language.BooleanValue;
import graphql.language.EnumValue;
import graphql.language.Field;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.SelectionSet;
import graphql.language.StringValue;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingEnvironmentImpl;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.PropertyDataFetcher;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.Answers;
import org.mockito.invocation.Invocation;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

/**
 * A lookup of an entity the caller can't view returns the placeholder a missing urn gets, which
 * still carries the real urn, and GraphQL then runs every resolver wired on that type with it as
 * the source. This walks the real wiring and fails for any field resolver that asks a backend about
 * that urn.
 */
public class UnviewableEntityFieldsWiringTest {

  private static final int MIN_CHECKED_FIELDS = 250;
  private static final int MIN_VIEWABLE_READS = 100;

  private static final Map<String, Object> SAMPLE_SCALARS =
      Map.of("Int", 10, "Long", 10L, "Float", 1.0, "Boolean", false);

  private static final Set<Class<?>> NON_MOCKED_ARG_TYPES =
      Set.of(String.class, Integer.class, Long.class, Double.class, Boolean.class);

  private final List<Object> backends = new ArrayList<>();
  private boolean buildingPlaceholders;

  @Test
  public void testNoFieldResolverReadsAnUnviewableSource() throws Exception {
    Walk walk = walk(false);

    assertTrue(
        walk.checked > MIN_CHECKED_FIELDS,
        "Checked only " + walk.checked + " fields; skipped types " + walk.skipped);
    assertTrue(
        walk.reads.isEmpty(),
        "Fields that read an unviewable source (guard them with AuthorizationUtils.canView):\n"
            + walk.reads.entrySet().stream()
                .map(leak -> leak.getKey() + " " + leak.getValue())
                .collect(Collectors.joining("\n"))
            + "\nFields that threw:\n"
            + String.join("\n", walk.failures));
  }

  /** Without this, a walk that never reaches a backend would pass the test above. */
  @Test
  public void testWalkSeesReadsOfAViewableSource() throws Exception {
    Walk walk = walk(true);

    assertTrue(walk.reads.size() > MIN_VIEWABLE_READS, "Only saw reads from " + walk.reads);
    assertTrue(walk.reads.containsKey("Dataset.health"), "Saw reads from " + walk.reads.keySet());
  }

  /** Calls every non-trivial field resolver of every entity type on a placeholder source. */
  private Walk walk(boolean viewable) throws Exception {
    EntityRegistry registry = TestOperationContexts.defaultEntityRegistry();
    QueryContext context = viewContext(viewable, registry);
    GmsGraphQLEngine engine = new GmsGraphQLEngine(args(registry));
    GraphQLSchema schema = engine.builder().build().getGraphQL().getGraphQLSchema();
    GraphQLCodeRegistry codeRegistry = schema.getCodeRegistry();
    Set<String> entityLoaders =
        engine.loadableTypes.stream()
            .map(LoadableType::name)
            .collect(Collectors.toCollection(HashSet::new));
    entityLoaders.add(WeaklyTypedAspectsResolver.LOADER_NAME);

    Walk walk = new Walk();
    for (GraphQLObjectType type :
        schema.getImplementations((GraphQLInterfaceType) schema.getType("Entity"))) {
      Entity source = placeholder(type.getName(), engine, registry, context);
      if (source == null) {
        walk.skipped.add(type.getName());
        continue;
      }
      for (GraphQLFieldDefinition field : type.getFieldDefinitions()) {
        FieldCoordinates coordinates = FieldCoordinates.coordinates(type, field);
        if (!codeRegistry.hasDataFetcher(coordinates)) {
          continue;
        }
        DataFetcher<?> fetcher = codeRegistry.getDataFetcher(coordinates, field);
        if (fetcher instanceof PropertyDataFetcher) {
          continue;
        }
        walk.checked++;
        List<String> reads =
            reads(fetcher, environment(source, context, type, field), source.getUrn(), walk);
        reads.removeIf(read -> isEntityLoad(read, entityLoaders));
        if (!reads.isEmpty()) {
          walk.reads.put(
              coordinates.getTypeName() + "." + coordinates.getFieldName(),
              "(" + fetcher.getClass().getName() + "): " + reads);
        }
      }
    }
    return walk;
  }

  /** Runs one field resolver and returns the backend calls and loads that name {@code urn}. */
  private List<String> reads(
      DataFetcher<?> fetcher, DataFetchingEnvironment environment, String urn, Walk walk) {
    backends.forEach(backend -> clearInvocations(backend));
    RecordingRegistry loaders = (RecordingRegistry) environment.getDataLoaderRegistry();
    String name =
        ((GraphQLObjectType) environment.getParentType()).getName()
            + "."
            + environment.getFieldDefinition().getName();
    try {
      Object result = fetcher.get(environment);
      if (result instanceof CompletionStage) {
        ((CompletionStage<?>) result).toCompletableFuture().get(10, TimeUnit.SECONDS);
      }
    } catch (TimeoutException e) {
      walk.failures.add(name + " timed out");
    } catch (Exception | LinkageError e) {
      walk.failures.add(name + " threw " + e);
    }

    List<String> reads = new ArrayList<>();
    for (Object backend : backends) {
      for (Invocation invocation : mockingDetails(backend).getInvocations()) {
        if (mentions(invocation.getArguments(), urn)) {
          reads.add(
              mockingDetails(backend).getMockCreationSettings().getTypeToMock().getSimpleName()
                  + "."
                  + invocation.getMethod().getName());
        }
      }
    }
    loaders.keys.forEach(
        (loader, keys) -> {
          if (mentions(keys.toArray(), urn)) {
            reads.add("loader:" + loader);
          }
        });
    return reads;
  }

  /**
   * Entity loaders and the aspects loader answer an entity the actor cannot view as a missing one,
   * so loading the source through one is safe.
   */
  private static boolean isEntityLoad(String read, Set<String> entityLoaders) {
    return read.startsWith("loader:") && entityLoaders.contains(read.substring("loader:".length()));
  }

  private static boolean mentions(Object[] arguments, String urn) {
    try {
      return Arrays.deepToString(arguments).contains(urn);
    } catch (RuntimeException e) {
      return Arrays.stream(arguments).anyMatch(arg -> safeToString(arg).contains(urn));
    }
  }

  private static String safeToString(Object arg) {
    try {
      return String.valueOf(arg);
    } catch (RuntimeException e) {
      return "";
    }
  }

  /** What a lookup of the type returns for an unviewable urn, or null if it can't be built. */
  private Entity placeholder(
      String typeName, GmsGraphQLEngine engine, EntityRegistry registry, QueryContext context) {
    for (LoadableType<?, ?> loadable : engine.loadableTypes) {
      if (!loadable.objectClass().getSimpleName().equals(typeName)
          || !(loadable instanceof EntityType)) {
        continue;
      }
      Urn urn = sampleUrn(EntityTypeMapper.getName(((EntityType<?, ?>) loadable).type()), registry);
      if (urn == null) {
        continue;
      }
      Object key =
          typeName.startsWith("Versioned")
              ? new VersionedUrn().setUrn(urn).setVersionStamp("0")
              : urn.toString();
      Entity loaded = load(loadable, key, context);
      return loaded != null ? loaded : bareEntity((EntityType<?, ?>) loadable, urn);
    }
    return null;
  }

  /** The generated object with only its identity set, for types that can't be loaded here. */
  @Nullable
  private static Entity bareEntity(EntityType<?, ?> type, Urn urn) {
    try {
      Object entity = type.objectClass().getDeclaredConstructor().newInstance();
      entity.getClass().getMethod("setUrn", String.class).invoke(entity, urn.toString());
      entity
          .getClass()
          .getMethod("setType", com.linkedin.datahub.graphql.generated.EntityType.class)
          .invoke(entity, type.type());
      return (Entity) entity;
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  @Nullable
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Entity load(LoadableType type, Object key, QueryContext context) {
    buildingPlaceholders = true;
    try {
      List<DataFetcherResult<?>> results = type.batchLoad(ImmutableList.of(key), context);
      Object data = results.isEmpty() || results.get(0) == null ? null : results.get(0).getData();
      return data instanceof Entity ? (Entity) data : null;
    } catch (Exception e) {
      return null;
    } finally {
      buildingPlaceholders = false;
    }
  }

  private DataFetchingEnvironment environment(
      Entity source, QueryContext context, GraphQLObjectType type, GraphQLFieldDefinition field) {
    Map<String, Object> arguments = new HashMap<>();
    for (GraphQLArgument argument : field.getArguments()) {
      arguments.put(argument.getName(), defaultValue(argument));
    }
    return DataFetchingEnvironmentImpl.newDataFetchingEnvironment()
        .source(source)
        .context(context)
        .graphQLContext(GraphQLContext.newContext().build())
        .parentType(type)
        .fieldDefinition(field)
        .mergedField(
            MergedField.newMergedField(
                    Field.newField(field.getName())
                        .selectionSet(SelectionSet.newSelectionSet().build())
                        .build())
                .build())
        .fieldType(field.getType())
        .arguments(arguments)
        .selectionSet(mock(DataFetchingFieldSelectionSet.class))
        .dataLoaderRegistry(new RecordingRegistry())
        .build();
  }

  /** The schema default, else a sample value so resolvers get past reading their arguments. */
  private static Object defaultValue(GraphQLArgument argument) {
    if (argument.hasSetDefaultValue() && argument.getArgumentDefaultValue().isLiteral()) {
      Object literal = argument.getArgumentDefaultValue().getValue();
      if (literal instanceof IntValue) {
        return ((IntValue) literal).getValue().intValue();
      }
      if (literal instanceof FloatValue) {
        return ((FloatValue) literal).getValue().doubleValue();
      }
      if (literal instanceof StringValue) {
        return ((StringValue) literal).getValue();
      }
      if (literal instanceof BooleanValue) {
        return ((BooleanValue) literal).isValue();
      }
      if (literal instanceof EnumValue) {
        return ((EnumValue) literal).getName();
      }
    } else if (argument.hasSetDefaultValue() && argument.getArgumentDefaultValue().isExternal()) {
      return argument.getArgumentDefaultValue().getValue();
    }
    GraphQLNamedType type = GraphQLTypeUtil.unwrapAll(argument.getType());
    if (type instanceof GraphQLInputObjectType) {
      return new HashMap<String, Object>();
    }
    if (type instanceof GraphQLEnumType) {
      return ((GraphQLEnumType) type).getValues().get(0).getValue();
    }
    return SAMPLE_SCALARS.get(type.getName());
  }

  /** A user session with view authorization on, allowed everything or nothing. */
  private static QueryContext viewContext(boolean viewable, EntityRegistry registry) {
    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0),
                    viewable ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                    ""));
    Authentication authentication = new Authentication(new Actor(ActorType.USER, "test"), "");
    OperationContext opContext =
        TestOperationContexts.systemContext(
                () ->
                    OperationContextConfig.builder()
                        .viewAuthorizationConfiguration(
                            ViewAuthorizationConfiguration.builder().enabled(true).build())
                        .build(),
                null,
                null,
                () -> registry,
                null,
                null,
                null,
                null)
            .asSession(RequestContext.TEST, authorizer, authentication);

    QueryContext context = mock(QueryContext.class);
    when(context.getOperationContext()).thenReturn(opContext);
    when(context.getAuthentication()).thenReturn(authentication);
    when(context.getActorUrn()).thenReturn("urn:li:corpuser:test");
    when(context.getAuthorizer()).thenReturn(authorizer);
    return context;
  }

  /** Engine args as production builds them, with every service a recording mock. */
  private GmsGraphQLEngineArgs args(EntityRegistry registry) throws IllegalAccessException {
    GmsGraphQLEngineArgs args = new GmsGraphQLEngineArgs();
    AuthorizationConfiguration authorizationConfiguration = new AuthorizationConfiguration();
    authorizationConfiguration.setView(
        ViewAuthorizationConfiguration.builder().enabled(true).build());
    GraphQLQueryConfiguration query = new GraphQLQueryConfiguration();
    query.setComplexityLimit(10000);
    query.setDepthLimit(50);
    GraphQLConfiguration graphQLConfiguration = new GraphQLConfiguration();
    graphQLConfiguration.setQuery(query);
    graphQLConfiguration.setMetrics(new GraphQLMetricsConfiguration());

    args.setEntityRegistry(registry);
    args.setSystemOperationContext(
        TestOperationContexts.systemContextNoSearchAuthorization(registry));
    args.setFeatureFlags(new FeatureFlags());
    args.setIngestionConfiguration(new IngestionConfiguration());
    args.setAuthenticationConfiguration(new AuthenticationConfiguration());
    args.setAuthorizationConfiguration(authorizationConfiguration);
    args.setGraphQLConfiguration(graphQLConfiguration);

    for (var field : GmsGraphQLEngineArgs.class.getDeclaredFields()) {
      Class<?> type = field.getType();
      if (Modifier.isStatic(field.getModifiers())
          || type.isPrimitive()
          || NON_MOCKED_ARG_TYPES.contains(type)) {
        continue;
      }
      field.setAccessible(true);
      if (field.get(args) != null) {
        continue;
      }
      if (isConfiguration(type)) {
        field.set(args, mock(type, RETURNS_DEEP_STUBS));
      } else {
        Object backend = mock(type, backendAnswer(registry));
        backends.add(backend);
        field.set(args, backend);
      }
    }
    return args;
  }

  private static boolean isConfiguration(Class<?> type) {
    return type.getName().startsWith("com.linkedin.metadata.config.")
        || type.getSimpleName().endsWith("Configuration")
        || type.getSimpleName().endsWith("Config")
        || type == FeatureFlags.class
        || type.getSimpleName().equals("GitVersion");
  }

  /** Answers batch gets with a missing urn's response while placeholders are built. */
  private Answer<Object> backendAnswer(EntityRegistry registry) {
    return invocation -> {
      String method = invocation.getMethod().getName();
      if (!buildingPlaceholders
          || (!method.equals("batchGetV2") && !method.equals("batchGetVersionedV2"))) {
        return Answers.RETURNS_DEFAULTS.answer(invocation);
      }
      Map<Urn, EntityResponse> responses = new HashMap<>();
      for (Object requested : (Set<?>) invocation.getArgument(2)) {
        Urn urn =
            requested instanceof VersionedUrn
                ? ((VersionedUrn) requested).getUrn()
                : (Urn) requested;
        responses.put(urn, EntityResponseUtils.missingEntityResponse(registry, urn));
      }
      return responses;
    };
  }

  /** A urn whose key parts parse into the entity's key aspect. */
  @Nullable
  private static Urn sampleUrn(String entityName, EntityRegistry registry) {
    if (!registry.getEntitySpecs().containsKey(entityName.toLowerCase())) {
      return null;
    }
    RecordDataSchema keySchema =
        registry.getEntitySpec(entityName).getKeyAspectSpec().getPegasusSchema();
    List<String> parts =
        keySchema.getFields().stream()
            .map(UnviewableEntityFieldsWiringTest::sampleKeyPart)
            .collect(Collectors.toList());
    return UrnUtils.getUrn(
        "urn:li:"
            + entityName
            + ":"
            + (parts.size() == 1 ? parts.get(0) : "(" + String.join(",", parts) + ")"));
  }

  private static String sampleKeyPart(RecordDataSchema.Field field) {
    DataSchema type = field.getType();
    if (type.getDereferencedType() == DataSchema.Type.ENUM) {
      return ((EnumDataSchema) type.getDereferencedDataSchema()).getSymbols().get(0);
    }
    if (type.getType() == DataSchema.Type.TYPEREF) {
      return "urn:li:dataPlatform:hiddenplatform";
    }
    return "hiddensample";
  }

  private static class Walk {
    private final Map<String, String> reads = new TreeMap<>();
    private final List<String> failures = new ArrayList<>();
    private final List<String> skipped = new ArrayList<>();
    private int checked;
  }

  /** Hands out loaders that record the keys they are asked for and load nothing. */
  private static class RecordingRegistry extends DataLoaderRegistry {
    private final Map<String, List<Object>> keys = new HashMap<>();
    private final Map<String, DataLoader<?, ?>> loaders = new HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> DataLoader<K, V> getDataLoader(String name) {
      return (DataLoader<K, V>) loaders.computeIfAbsent(name, this::recordingLoader);
    }

    @Override
    public Set<String> getKeys() {
      return new HashSet<>(loaders.keySet());
    }

    private DataLoader<?, ?> recordingLoader(String name) {
      List<Object> recorded = keys.computeIfAbsent(name, n -> new ArrayList<>());
      return mock(
          DataLoader.class,
          invocation -> {
            String method = invocation.getMethod().getName();
            if (method.equals("load")) {
              recorded.add(invocation.getArgument(0));
              return CompletableFuture.completedFuture(null);
            }
            if (method.equals("loadMany")) {
              Object requested = invocation.getArgument(0);
              recorded.add(requested);
              int size = requested instanceof List ? ((List<?>) requested).size() : 0;
              return CompletableFuture.completedFuture(Collections.nCopies(size, null));
            }
            return Answers.RETURNS_DEFAULTS.answer(invocation);
          });
    }
  }
}
