package controllers.api.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
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

import static com.linkedin.datahub.graphql.Constants.*;

/**
 * Responsible for executing GraphQL queries.
 */
public class GraphQLController extends Controller {

    private final GraphQLEngine _engine;
    private final Config _config;

    @Inject
    public GraphQLController(@Nonnull Environment environment, @Nonnull Config config) {
        /*
         * Fetch path to custom GraphQL Type System Definition
         */
        String schemaString;
        try {
            InputStream is = environment.resourceAsStream("datahub-frontend.graphql").get();
            schemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + GMS_SCHEMA_FILE, e);
        }

        /*
         * Instantiate engine
         */
        _engine = GmsGraphQLEngine.builder()
            .addSchema(schemaString)
            .configureRuntimeWiring(builder ->
                    builder.type("Mutation",
                            typeWiring -> typeWiring.dataFetcher("logIn", new LogInResolver())))
            .build();
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
        try {
            return ok(new ObjectMapper().writeValueAsString(executionResult.toSpecification()));
        } catch (JsonProcessingException e) {
            return internalServerError();
        }
    }
}