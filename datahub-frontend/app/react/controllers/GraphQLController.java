package react.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.typesafe.config.Config;
import org.apache.commons.io.IOUtils;
import react.analytics.AnalyticsService;
import react.auth.Authenticator;
import graphql.ExecutionResult;
import react.graphql.PlayQueryContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import play.api.Environment;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security;
import react.resolver.AnalyticsChartTypeResolver;
import react.resolver.GetChartsResolver;
import react.resolver.GetHighlightsResolver;
import react.resolver.IsAnalyticsEnabledResolver;

public class GraphQLController extends Controller {

    private static final String FRONTEND_SCHEMA_NAME = "datahub-frontend.graphql";

    private static final String QUERY_TYPE = "Query";
    private static final String ANALYTICS_CHART_TYPE = "AnalyticsChart";

    private static final String IS_ANALYTICS_ENABLED_QUERY = "isAnalyticsEnabled";
    private static final String GET_ANALYTICS_CHARTS_QUERY = "getAnalyticsCharts";
    private static final String GET_HIGHLIGHTS_QUERY = "getHighlights";

    private static final String QUERY = "query";
    private static final String VARIABLES = "variables";

    private final GraphQLEngine _engine;
    private final Config _config;

    @Inject
    public GraphQLController(@Nonnull Environment environment,
                             @Nonnull Config config,
                             @Nonnull AnalyticsService analyticsService) {
        /*
         * Initialize GraphQL Engine
         */
        _config = config;
        _engine = buildExtendedEngine(environment, analyticsService);
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
        Map<String, Object> variables = Collections.emptyMap();
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

    private GraphQLEngine buildExtendedEngine(@Nonnull Environment environment,
                                              @Nonnull AnalyticsService analyticsService) {
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
        return GmsGraphQLEngine.builder()
                .addSchema(schemaString)
                .configureRuntimeWiring(builder -> builder
                        .type(QUERY_TYPE, typeWiring -> typeWiring
                                .dataFetcher(IS_ANALYTICS_ENABLED_QUERY, new IsAnalyticsEnabledResolver(isAnalyticsEnabled(_config)))
                                .dataFetcher(GET_ANALYTICS_CHARTS_QUERY, new GetChartsResolver(analyticsService))
                                .dataFetcher(GET_HIGHLIGHTS_QUERY, new GetHighlightsResolver(analyticsService)))
                        .type(ANALYTICS_CHART_TYPE, typeWiring -> typeWiring
                                .typeResolver(new AnalyticsChartTypeResolver())
                        ))
                .build();
    }

    private boolean isAnalyticsEnabled(final Config config) {
        return !config.hasPath("analytics.enabled") || config.getBoolean("analytics.enabled");
    }
}
