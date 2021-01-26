package com.linkedin.datahub.graphql;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.loaders.CorpUserLoader;
import com.linkedin.datahub.graphql.loaders.DatasetLoader;
import com.linkedin.datahub.graphql.loaders.GmsClientFactory;
import com.linkedin.datahub.graphql.resolvers.AuthenticatedResolver;
import com.linkedin.datahub.graphql.resolvers.corpuser.ManagerResolver;
import com.linkedin.datahub.graphql.resolvers.ownership.OwnerResolver;
import com.linkedin.datahub.graphql.resolvers.query.DatasetResolver;
import graphql.schema.idl.RuntimeWiring;
import org.apache.commons.io.IOUtils;
import org.dataloader.DataLoader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.datahub.graphql.Constants.*;
import static graphql.Scalars.GraphQLLong;

/**
 * A {@link GraphQLEngine} configured to provide access to the entities and aspects on the the GMS graph.
 */
public class GmsGraphQLEngine {

    private static GraphQLEngine _engine;

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

    public static Map<String, Supplier<DataLoader<?, ?>>> loaderSuppliers() {
        return ImmutableMap.of(
                DatasetLoader.NAME, () -> DataLoader.newDataLoader(new DatasetLoader(GmsClientFactory.getDatasetsClient())),
                CorpUserLoader.NAME, () -> DataLoader.newDataLoader(new CorpUserLoader(GmsClientFactory.getCorpUsersClient()))
        );
    }

    public static void configureRuntimeWiring(final RuntimeWiring.Builder builder) {
        builder
            .type(QUERY_TYPE_NAME, typeWiring -> typeWiring
                    .dataFetcher(DATASETS_FIELD_NAME, new AuthenticatedResolver<>(new DatasetResolver()))
            )
            .type(OWNER_TYPE_NAME, typeWiring -> typeWiring
                    .dataFetcher(OWNER_FIELD_NAME, new AuthenticatedResolver<>(new OwnerResolver()))
            )
            .type(CORP_USER_TYPE_NAME, typeWiring -> typeWiring
                    .dataFetcher(MANAGER_FIELD_NAME, new AuthenticatedResolver<>(new ManagerResolver()))
            )
            .scalar(GraphQLLong);
    }

    public static GraphQLEngine.Builder builder() {
        return GraphQLEngine.builder()
                .addSchema(schema())
                .addDataLoaders(loaderSuppliers())
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

    private GmsGraphQLEngine() { }

}
