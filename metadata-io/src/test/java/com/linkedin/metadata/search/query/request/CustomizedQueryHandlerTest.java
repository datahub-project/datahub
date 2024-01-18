package com.linkedin.metadata.search.query.request;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.BoolQueryConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.testng.annotations.Test;

public class CustomizedQueryHandlerTest {
  public static final ObjectMapper TEST_MAPPER = new YAMLMapper();
  private static final CustomSearchConfiguration TEST_CONFIG;

  static {
    try {
      CustomConfiguration customConfiguration = new CustomConfiguration();
      customConfiguration.setEnabled(true);
      customConfiguration.setFile("search_config_test.yml");
      TEST_CONFIG = customConfiguration.resolve(TEST_MAPPER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static final SearchQueryBuilder SEARCH_QUERY_BUILDER;

  static {
    SEARCH_QUERY_BUILDER = new SearchQueryBuilder(new SearchConfiguration(), TEST_CONFIG);
  }

  private static final List<QueryConfiguration> EXPECTED_CONFIGURATION =
      List.of(
          QueryConfiguration.builder()
              .queryRegex("[*]|")
              .simpleQuery(false)
              .exactMatchQuery(false)
              .prefixMatchQuery(false)
              .functionScore(
                  Map.of(
                      "score_mode",
                      "avg",
                      "boost_mode",
                      "multiply",
                      "functions",
                      List.of(
                          Map.of(
                              "weight",
                              1,
                              "filter",
                              Map.<String, Object>of("match_all", Map.<String, Object>of())),
                          Map.of(
                              "weight",
                              0.5,
                              "filter",
                              Map.<String, Object>of(
                                  "term", Map.of("materialized", Map.of("value", true)))),
                          Map.of(
                              "weight",
                              0.5,
                              "filter",
                              Map.<String, Object>of(
                                  "term",
                                  Map.<String, Object>of("deprecated", Map.of("value", true)))))))
              .build(),
          QueryConfiguration.builder()
              .queryRegex(".*")
              .simpleQuery(true)
              .exactMatchQuery(true)
              .prefixMatchQuery(true)
              .boolQuery(
                  BoolQueryConfiguration.builder()
                      .must(List.of(Map.of("term", Map.of("name", "{{query_string}}"))))
                      .build())
              .functionScore(
                  Map.of(
                      "score_mode",
                      "avg",
                      "boost_mode",
                      "multiply",
                      "functions",
                      List.of(
                          Map.of(
                              "weight",
                              1,
                              "filter",
                              Map.<String, Object>of("match_all", Map.<String, Object>of())),
                          Map.of(
                              "weight",
                              0.5,
                              "filter",
                              Map.<String, Object>of(
                                  "term", Map.of("materialized", Map.of("value", true)))),
                          Map.of(
                              "weight",
                              1.5,
                              "filter",
                              Map.<String, Object>of(
                                  "term",
                                  Map.<String, Object>of("deprecated", Map.of("value", false)))))))
              .build());

  @Test
  public void configParsingTest() {
    assertNotNull(TEST_CONFIG);
    assertEquals(TEST_CONFIG.getQueryConfigurations(), EXPECTED_CONFIGURATION);
  }

  @Test
  public void customizedQueryHandlerInitTest() {
    CustomizedQueryHandler test = CustomizedQueryHandler.builder(TEST_CONFIG).build();

    assertEquals(
        test.getQueryConfigurations().stream()
            .map(e -> e.getKey().toString())
            .collect(Collectors.toList()),
        List.of("[*]|", ".*"));

    assertEquals(
        test.getQueryConfigurations().stream()
            .map(e -> Map.entry(e.getKey().toString(), e.getValue()))
            .collect(Collectors.toList()),
        EXPECTED_CONFIGURATION.stream()
            .map(cfg -> Map.entry(cfg.getQueryRegex(), cfg))
            .collect(Collectors.toList()));
  }

  @Test
  public void patternMatchTest() {
    CustomizedQueryHandler test = CustomizedQueryHandler.builder(TEST_CONFIG).build();

    for (String selectAllQuery : List.of("*", "")) {
      QueryConfiguration actual = test.lookupQueryConfig(selectAllQuery).get();
      assertEquals(
          actual,
          EXPECTED_CONFIGURATION.get(0),
          String.format("Failed to match: `%s`", selectAllQuery));
    }

    for (String otherQuery : List.of("foo", "bar")) {
      QueryConfiguration actual = test.lookupQueryConfig(otherQuery).get();
      assertEquals(actual, EXPECTED_CONFIGURATION.get(1));
    }
  }

  @Test
  public void functionScoreQueryBuilderTest() {
    CustomizedQueryHandler test = CustomizedQueryHandler.builder(TEST_CONFIG).build();
    MatchAllQueryBuilder inputQuery = QueryBuilders.matchAllQuery();

    /*
     * Test select star
     */
    FunctionScoreQueryBuilder selectStarTest =
        SEARCH_QUERY_BUILDER.functionScoreQueryBuilder(
            test.lookupQueryConfig("*").get(), inputQuery);

    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedSelectStarScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.weightFactorFunction(1f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("materialized", true),
          ScoreFunctionBuilders.weightFactorFunction(0.5f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("deprecated", true),
          ScoreFunctionBuilders.weightFactorFunction(0.5f))
    };
    FunctionScoreQueryBuilder expectedSelectStar =
        new FunctionScoreQueryBuilder(expectedSelectStarScoreFunctions)
            .scoreMode(FunctionScoreQuery.ScoreMode.AVG)
            .boostMode(CombineFunction.MULTIPLY);

    assertEquals(selectStarTest, expectedSelectStar);

    /*
     * Test default (non-select start)
     */
    FunctionScoreQueryBuilder defaultTest =
        SEARCH_QUERY_BUILDER.functionScoreQueryBuilder(
            test.lookupQueryConfig("foobar").get(), inputQuery);

    FunctionScoreQueryBuilder.FilterFunctionBuilder[] expectedDefaultScoreFunctions = {
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          ScoreFunctionBuilders.weightFactorFunction(1f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("materialized", true),
          ScoreFunctionBuilders.weightFactorFunction(0.5f)),
      new FunctionScoreQueryBuilder.FilterFunctionBuilder(
          QueryBuilders.termQuery("deprecated", false),
          ScoreFunctionBuilders.weightFactorFunction(1.5f))
    };
    FunctionScoreQueryBuilder expectedDefault =
        new FunctionScoreQueryBuilder(expectedDefaultScoreFunctions)
            .scoreMode(FunctionScoreQuery.ScoreMode.AVG)
            .boostMode(CombineFunction.MULTIPLY);

    assertEquals(defaultTest, expectedDefault);
  }
}
