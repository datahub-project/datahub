//package react.controllers;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.linkedin.datahub.graphql.GmsGraphQLEngine;
//import com.linkedin.datahub.graphql.GraphQLEngine;
//import com.typesafe.config.Config;
//import org.apache.commons.io.IOUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import react.analytics.AnalyticsService;
//import react.auth.Authenticator;
//import graphql.ExecutionResult;
//import react.graphql.PlayQueryContext;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.nio.charset.StandardCharsets;
//import java.util.Collections;
//import java.util.Map;
//import javax.annotation.Nonnull;
//import javax.inject.Inject;
//import play.api.Environment;
//import play.mvc.Controller;
//import play.mvc.Result;
//import play.mvc.Security;
//import react.resolver.AnalyticsChartTypeResolver;
//import react.resolver.GetChartsResolver;
//import react.resolver.GetHighlightsResolver;
//import react.resolver.IsAnalyticsEnabledResolver;
//
//public class GraphQLController extends Controller {
//
//    private final Logger _logger = LoggerFactory.getLogger(GraphQLController.class.getName());
//
//    private static final String FRONTEND_SCHEMA_NAME = "datahub-frontend.graphql";
//
//    private static final String QUERY_TYPE = "Query";
//    private static final String ANALYTICS_CHART_TYPE = "AnalyticsChart";
//
//    private static final String IS_ANALYTICS_ENABLED_QUERY = "isAnalyticsEnabled";
//    private static final String GET_ANALYTICS_CHARTS_QUERY = "getAnalyticsCharts";
//    private static final String GET_HIGHLIGHTS_QUERY = "getHighlights";
//
//    private static final String QUERY = "query";
//    private static final String VARIABLES = "variables";
//
//    private final GraphQLEngine _engine;
//    private final Config _config;
//
//    @Inject
//    public GraphQLController(@Nonnull Environment environment,
//                             @Nonnull Config config,
//                             @Nonnull AnalyticsService analyticsService) {
//        /*
//         * Initialize GraphQL Engine
//         */
//        _config = config;
//        _engine = buildExtendedEngine(environment, analyticsService);
//    }
//
//    @Security.Authenticated(Authenticator.class)
//    @Nonnull
//    public Result execute() throws Exception {
//
//        JsonNode bodyJson = request().body().asJson();
//        if (bodyJson == null) {
//            return badRequest();
//        }
//
//        /*
//         * Extract "query" field
//         */
//        JsonNode queryJson = bodyJson.get(QUERY);
//        if (queryJson == null) {
//            return badRequest();
//        }
//
//        /*
//         * Extract "variables" map
//         */
//        JsonNode variablesJson = bodyJson.get(VARIABLES);
//        Map<String, Object> variables = Collections.emptyMap();
//        if (variablesJson != null) {
//            variables = new ObjectMapper().convertValue(variablesJson, new TypeReference<Map<String, Object>>(){ });
//        }
//
//        _logger.debug(String.format("Executing graphQL query: %s, variables: %s", queryJson, variablesJson));
//
//        /*
//         * Init QueryContext
//         */
//        PlayQueryContext context = new PlayQueryContext(ctx(), _config);
//
//        /*
//         * Execute GraphQL Query
//         */
//        ExecutionResult executionResult = _engine.execute(queryJson.asText(), variables, context);
//
//        if (executionResult.getErrors().size() != 0) {
//            // There were GraphQL errors. Report in error logs.
//            _logger.error(String.format("Errors while executing graphQL query: %s, result: %s, errors: %s",
//                queryJson,
//                executionResult.toSpecification(),
//                executionResult.getErrors()));
//        } else {
//            _logger.debug(String.format("Executed graphQL query: %s, result: %s",
//                queryJson,
//                executionResult.toSpecification()));
//        }
//
//
//        /*
//         * Format & Return Response
//         */
//        return ok(new ObjectMapper().writeValueAsString(executionResult.toSpecification()));
//    }
//}
