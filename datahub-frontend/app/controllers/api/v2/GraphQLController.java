package controllers.api.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.typesafe.config.Config;
import graphql.ExecutionResult;
import graphql.PlayQueryContext;
import graphql.resolvers.mutation.LogInResolver;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import play.api.Environment;
import play.mvc.Controller;
import play.mvc.Result;

public class GraphQLController extends Controller {

    private static final String FRONTEND_SCHEMA_NAME = "datahub-frontend.graphql";
    private static final String MUTATION_TYPE = "Mutation";
    private static final String LOG_IN_MUTATION = "logIn";

    private static final String QUERY = "query";
    private static final String VARIABLES = "variables";

    private final GraphQLEngine _engine;
    private final Config _config;

    @Inject
    public GraphQLController(@Nonnull Environment environment, @Nonnull Config config) {
        /*
         * Fetch path to custom GraphQL Type System Definition
         */
        String schemaString;
        try {
            final InputStream is = environment.resourceAsStream(FRONTEND_SCHEMA_NAME).get();
            schemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + FRONTEND_SCHEMA_NAME, e);
        }

        /*
         * Instantiate engine
         */
        _engine = GmsGraphQLEngine.builder()
            .addSchema(schemaString)
            .configureRuntimeWiring(builder ->
                    builder.type(MUTATION_TYPE,
                            typeWiring -> typeWiring.dataFetcher(
                                    LOG_IN_MUTATION,
                                    new LogInResolver(GmsGraphQLEngine.CORP_USER_TYPE))))
            .build();
        _config = config;

    }

    @Nonnull
    public Result execute() throws Exception {

        JsonNode bodyJson = request().body().asJson();
        if (bodyJson == null) {
            return badRequest();
        }

        /*
         * Extract "query" field
         */
        JsonNode queryJson = bodyJson.get(QUERY);
        if (queryJson == null) {
            return badRequest();
        }

        /*
         * Extract "variables" map
         */
        JsonNode variablesJson = bodyJson.get(VARIABLES);
        Map<String, Object> variables = null;
        if (variablesJson != null) {
            variables = new ObjectMapper().convertValue(variablesJson, new TypeReference<Map<String, Object>>(){ });
        }

        /*
         * Init QueryContext
         */
        PlayQueryContext context = new PlayQueryContext(session(), _config);

        /*
         * Execute GraphQL Query
         */
        ExecutionResult executionResult = _engine.execute(queryJson.asText(), variables, context);

        /*
         * Format & Return Response
         */
        return ok(new ObjectMapper().writeValueAsString(executionResult.toSpecification()));
    }
}
