package com.linkedin.datahub.graphql;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.RelatedDataset;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserType;
import com.linkedin.datahub.graphql.types.dataplatform.DataPlatformType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.datahub.graphql.generated.CorpUserInfo;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.datahub.graphql.types.dataset.DownstreamLineageType;
import com.linkedin.datahub.graphql.resolvers.AuthenticatedResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowsePathsResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowseResolver;
import com.linkedin.datahub.graphql.resolvers.search.AutoCompleteResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchResolver;
import com.linkedin.datahub.graphql.resolvers.type.EntityInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.PlatformSchemaUnionTypeResolver;
import graphql.schema.idl.RuntimeWiring;
import org.apache.commons.io.IOUtils;
import org.dataloader.DataLoader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.Constants.*;
import static graphql.Scalars.GraphQLLong;

/**
 * A {@link GraphQLEngine} configured to provide access to the entities and aspects on the the GMS graph.
 */
public class GmsGraphQLEngine {

    private static GraphQLEngine _engine;

    public static final DatasetType DATASET_TYPE = new DatasetType(GmsClientFactory.getDatasetsClient());
    public static final CorpUserType CORP_USER_TYPE = new CorpUserType(GmsClientFactory.getCorpUsersClient());
    public static final DataPlatformType DATA_PLATFORM_TYPE = new DataPlatformType(GmsClientFactory.getDataPlatformsClient());
    public static final DownstreamLineageType DOWNSTREAM_LINEAGE_TYPE = new DownstreamLineageType(GmsClientFactory.getLineagesClient());

    /**
     * Configures the graph objects that can be fetched primary key.
     */
    public static final List<LoadableType<?>> LOADABLE_TYPES = ImmutableList.of(
            DATASET_TYPE,
            CORP_USER_TYPE,
            DATA_PLATFORM_TYPE,
            DOWNSTREAM_LINEAGE_TYPE
    );

    /**
     * Configures the graph objects that can be searched.
     */
    public static final List<SearchableEntityType<?>> SEARCHABLE_TYPES = LOADABLE_TYPES.stream()
            .filter(type -> (type instanceof SearchableEntityType<?>))
            .map(type -> (SearchableEntityType<?>) type)
            .collect(Collectors.toList());

    /**
     * Configures the graph objects that can be browsed.
     */
    public static final List<BrowsableEntityType<?>> BROWSABLE_TYPES = LOADABLE_TYPES.stream()
            .filter(type -> (type instanceof BrowsableEntityType<?>))
            .map(type -> (BrowsableEntityType<?>) type)
            .collect(Collectors.toList());

    public static String schema() {
        String defaultSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(GMS_SCHEMA_FILE);
            defaultSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + GMS_SCHEMA_FILE, e);
        }
        return defaultSchemaString;
    }

    /**
     * Returns a {@link Supplier} responsible for creating a new {@link DataLoader} from
     * a {@link LoadableType}.
     */
    public static Map<String, Supplier<DataLoader<?, ?>>> loaderSuppliers(final List<LoadableType<?>> loadableTypes) {
        return loadableTypes
            .stream()
            .collect(Collectors.toMap(
                    LoadableType::name,
                    (graphType) -> () -> createDataLoader(graphType)
            ));
    }

    public static void configureRuntimeWiring(final RuntimeWiring.Builder builder) {
        configureQueryResolvers(builder);
        configureDatasetResolvers(builder);
        configureCorpUserResolvers(builder);
        configureTypeResolvers(builder);
        configureTypeExtensions(builder);
    }

    public static GraphQLEngine.Builder builder() {
        return GraphQLEngine.builder()
                .addSchema(schema())
                .addDataLoaders(loaderSuppliers(LOADABLE_TYPES))
                .configureRuntimeWiring(GmsGraphQLEngine::configureRuntimeWiring);
    }

    public static GraphQLEngine get() {
        if (_engine == null) {
            synchronized (GmsGraphQLEngine.class) {
                if (_engine == null) {
                    _engine = builder().build();
                }
            }
        }
        return _engine;
    }

    private static void configureQueryResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Query", typeWiring -> typeWiring
                .dataFetcher("search", new AuthenticatedResolver<>(
                        new SearchResolver(SEARCHABLE_TYPES)))
                .dataFetcher("autoComplete", new AuthenticatedResolver<>(
                        new AutoCompleteResolver(SEARCHABLE_TYPES)))
                .dataFetcher("browse", new AuthenticatedResolver<>(
                        new BrowseResolver(BROWSABLE_TYPES)))
                .dataFetcher("browsePaths", new AuthenticatedResolver<>(
                        new BrowsePathsResolver(BROWSABLE_TYPES)))
                .dataFetcher("dataset", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(
                                DATASET_TYPE,
                                (env) -> env.getArgument(URN_FIELD_NAME))))
                .dataFetcher("corpUser", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(
                                CORP_USER_TYPE,
                                (env) -> env.getArgument(URN_FIELD_NAME))))
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Dataset} type.
     */
    private static void configureDatasetResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("Dataset", typeWiring -> typeWiring
                    .dataFetcher("platform", new AuthenticatedResolver<>(
                            new LoadableTypeResolver<>(
                                    DATA_PLATFORM_TYPE,
                                    (env) -> ((Dataset) env.getSource()).getPlatform().getUrn()))
                    )
                    .dataFetcher("downstreamLineage", new AuthenticatedResolver<>(
                            new LoadableTypeResolver<>(
                                    DOWNSTREAM_LINEAGE_TYPE,
                                    (env) -> ((Dataset) env.getSource()).getUrn()))
                    )
            )
            .type("Owner", typeWiring -> typeWiring
                    .dataFetcher("owner", new AuthenticatedResolver<>(
                            new LoadableTypeResolver<>(
                                    CORP_USER_TYPE,
                                    (env) -> ((Owner) env.getSource()).getOwner().getUrn()))
                    )
            )
            .type("RelatedDataset", typeWiring -> typeWiring
                    .dataFetcher("dataset", new AuthenticatedResolver<>(
                            new LoadableTypeResolver<>(
                                    DATASET_TYPE,
                                    (env) -> ((RelatedDataset) env.getSource()).getDataset().getUrn()))
                    )
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.CorpUser} type.
     */
    private static void configureCorpUserResolvers(final RuntimeWiring.Builder builder) {
        builder.type("CorpUserInfo", typeWiring -> typeWiring
            .dataFetcher("manager", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(
                            CORP_USER_TYPE,
                            (env) -> ((CorpUserInfo) env.getSource()).getManager().getUrn()))
            )
        );
    }

    /**
     * Configures {@link graphql.schema.TypeResolver}s for any GQL 'union' or 'interface' types.
     */
    private static void configureTypeResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("Entity", typeWiring -> typeWiring
                    .typeResolver(new EntityInterfaceTypeResolver(LOADABLE_TYPES.stream()
                            .filter(graphType -> graphType instanceof EntityType)
                            .map(graphType -> (EntityType<?>) graphType)
                            .collect(Collectors.toList())
                    )))
            .type("PlatformSchema", typeWiring -> typeWiring
                    .typeResolver(new PlatformSchemaUnionTypeResolver())
            );
    }

    /**
     * Configures custom type extensions leveraged within our GraphQL schema.
     */
    private static void configureTypeExtensions(final RuntimeWiring.Builder builder) {
        builder.scalar(GraphQLLong);
    }


    private static <T> DataLoader<String, T> createDataLoader(final LoadableType<T> graphType) {
        return DataLoader.newDataLoader(keys -> CompletableFuture.supplyAsync(() -> {
            try {
                return graphType.batchLoad(keys);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to retrieve entities of type %s", graphType.objectClass().getName()), e);
            }
        }));
    }

    private GmsGraphQLEngine() { }

}
