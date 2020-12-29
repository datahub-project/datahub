package controllers.api.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.Ownership;
import com.linkedin.datahub.dao.DaoFactory;
import com.typesafe.config.Config;
import graphql.ExecutionResult;
import graphql.ExecutionInput;
import graphql.QueryContext;
import graphql.resolvers.corpuser.ManagerResolver;
import graphql.resolvers.mutation.LogInResolver;
import graphql.resolvers.mutation.UpdateDatasetResolver;
import graphql.resolvers.query.*;
import graphql.resolvers.ownership.OwnerResolver;
import graphql.GraphQL;
import graphql.resolvers.type.SearchResultTypeResolver;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nonnull;
import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import play.api.Environment;
import play.mvc.Controller;
import play.mvc.Result;

import static graphql.Constants.*;
import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;

/**
 * Responsible for executing GraphQL queries.
 */
public class GraphQLController extends Controller {

    private final GraphQL _engine;
    private final Config _config;

    @Inject
    public GraphQLController(@Nonnull Environment environment, @Nonnull Config config) {
        /*
         * Fetch path to GraphQL Type System Definition
         */
        String schemaString;
        try {
            InputStream is = environment.resourceAsStream(GRAPH_SCHEMA_FILE).get();
            schemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + GRAPH_SCHEMA_FILE, e);
        }

        /*
         * Parse type system
         */
        SchemaParser schemaParser = new SchemaParser();
        TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schemaString);

        /*
         * Configure resolvers (data fetchers)
         */
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, configureResolvers());

        /*
         * Instantiate engine
         */
        _engine = GraphQL.newGraphQL(graphQLSchema).build();
        _config = config;

    }

    @Nonnull
    public Result execute() {

        JsonNode bodyJson = request().body().asJson();
        if (bodyJson == null) {
            return badRequest();
        }

        /*
         * Extract "query" field
         */
        JsonNode queryJson = bodyJson.get("query");
        if (queryJson == null) {
            return badRequest();
        }

        /*
         * Extract "variables" map
         */
        JsonNode variablesJson = bodyJson.get("variables");
        Map<String, Object> variables = null;
        if (variablesJson != null) {
            variables = new ObjectMapper().convertValue(variablesJson, new TypeReference<Map<String, Object>>(){ });
        }

        /*
         * Init DataLoaderRegistry - should be created for each request.
         */
        DataLoaderRegistry register = createDataLoaderRegistry();

        /*
         * Init QueryContext
         */
        QueryContext context = new QueryContext(session(), _config);

        /*
         * Construct execution input
         */
        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
                .query(queryJson.asText())
                .variables(variables)
                .dataLoaderRegistry(register)
                .context(context)
                .build();

        /*
         * Execute GraphQL Query
         */
        ExecutionResult executionResult = _engine.execute(executionInput);

        /*
         * Format & Return Response
         */
        try {
            return ok(new ObjectMapper().writeValueAsString(executionResult.toSpecification()));
        } catch (JsonProcessingException e) {
            return internalServerError();
        }
    }

    /**
     * Registers the resolvers required to execute a GraphQL Query against the type system.
     */
    private static RuntimeWiring configureResolvers() {
        /*
         * Register GraphQL field Resolvers.
         */
        return newRuntimeWiring()
            /*
             * Query Type
             */
            .type(QUERY_TYPE_NAME, typeWiring -> typeWiring
                    .dataFetcher(DATASETS_FIELD_NAME, new DatasetResolver())
                    .dataFetcher(SEARCH_FIELD_NAME, new SearchResolver())
                    .dataFetcher(AUTO_COMPLETE_FIELD_NAME, new AutoCompleteResolver())
                    .dataFetcher(BROWSE_FIELD_NAME, new BrowseResolver())
                    .dataFetcher(BROWSE_PATHS_FIELD_NAME, new BrowsePathsResolver())
            )
            /*
             * Mutation Type
             */
            .type(MUTATION_TYPE_NAME, typeWiring -> typeWiring
                    .dataFetcher(UPDATE_DATASET_FIELD_NAME, new UpdateDatasetResolver())
                    .dataFetcher(LOG_IN_FIELD_NAME, new LogInResolver())
            )
            /*
             * Owner Type
             */
            .type(OWNER_TYPE_NAME, typeWiring -> typeWiring
                    .dataFetcher(OWNER_FIELD_NAME, new OwnerResolver())
            )
            /*
             * CorpUser Type
             */
            .type(CORP_USER_TYPE_NAME, typeWiring -> typeWiring
                    .dataFetcher(MANAGER_FIELD_NAME, new ManagerResolver())
            )
            /*
             * Search Result Union Type
             */
            .type(SEARCH_RESULT_TYPE_NAME, typeWiring -> typeWiring
                    .typeResolver(new SearchResultTypeResolver())
            )
            .build();
    }

    private DataLoaderRegistry createDataLoaderRegistry() {
        DataLoader<String, com.linkedin.dataset.Dataset> datasetLoader = createDatasetLoader();
        DataLoader<String, Ownership> ownershipLoader = createOwnershipLoader();
        DataLoader<String, com.linkedin.identity.CorpUser> corpUserLoader = createCorpUserLoader();

        DataLoaderRegistry registry = new DataLoaderRegistry();
        registry.register(DATASET_LOADER_NAME, datasetLoader);
        registry.register(OWNERSHIP_LOADER_NAME, ownershipLoader);
        registry.register(CORP_USER_LOADER_NAME, corpUserLoader);
        return registry;
    }

    private DataLoader<String, com.linkedin.identity.CorpUser> createCorpUserLoader() {
        BatchLoader<String, com.linkedin.identity.CorpUser> corpUserBatchLoader = new BatchLoader<String, com.linkedin.identity.CorpUser>() {
            @Override
            public CompletionStage<List<com.linkedin.identity.CorpUser>> load(List<String> keys) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        return DaoFactory.getCorpUsersDao().getCorpUsers(keys);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to batch load CorpUsers", e);
                    }
                });
            }
        };
        return DataLoader.newDataLoader(corpUserBatchLoader);
    }

    private DataLoader<String, com.linkedin.dataset.Dataset> createDatasetLoader() {
        BatchLoader<String, com.linkedin.dataset.Dataset> datasetBatchLoader = new BatchLoader<String, com.linkedin.dataset.Dataset>() {
            @Override
            public CompletionStage<List<com.linkedin.dataset.Dataset>> load(List<String> keys) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        return DaoFactory.getDatasetsDao().getDatasets(keys);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to batch load Datasets", e);
                    }
                });
            }
        };
        return DataLoader.newDataLoader(datasetBatchLoader);
    }

    private DataLoader<String, Ownership> createOwnershipLoader() {
        BatchLoader<String, Ownership> ownershipBatchLoader = new BatchLoader<String, Ownership>() {
            @Override
            public CompletionStage<List<Ownership>> load(List<String> datasetUrns) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        // Typically should not be requested in batch.
                        List<Ownership> ownerships = new ArrayList<>();
                        for (String datasetUrn : datasetUrns) {
                            ownerships.add(DaoFactory.getDatasetsDao().getOwnership(datasetUrn));
                        }
                        return ownerships;
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to load Ownerships", e);
                    }
                });
            }
        };
        return DataLoader.newDataLoader(ownershipBatchLoader);
    }
}