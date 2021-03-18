package react.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.typesafe.config.Config;
import react.auth.Authenticator;
import graphql.ExecutionResult;
import react.graphql.PlayQueryContext;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import play.api.Environment;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;

public class GraphQLController extends Controller {

    private static final String QUERY = "query";
    private static final String VARIABLES = "variables";

    private final GraphQLEngine _engine;
    private final Config _config;

    @Inject
    public GraphQLController(@Nonnull Environment environment, @Nonnull Config config) {
        /*
         * Instantiate engine
         */
        _engine = GmsGraphQLEngine.get();
        _config = config;
    }

    @Security.Authenticated(Authenticator.class)
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
        PlayQueryContext context = new PlayQueryContext(ctx(), _config);

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
