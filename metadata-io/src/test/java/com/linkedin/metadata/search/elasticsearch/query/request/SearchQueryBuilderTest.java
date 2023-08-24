package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.TestEntitySpecBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.linkedin.util.Pair;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.testng.annotations.Test;

import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.TEXT_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.URN_SEARCH_ANALYZER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class SearchQueryBuilderTest {
  public static SearchConfiguration testQueryConfig;
  static {
    testQueryConfig = new SearchConfiguration();
    testQueryConfig.setMaxTermBucketSize(20);

    ExactMatchConfiguration exactMatchConfiguration = new ExactMatchConfiguration();
    exactMatchConfiguration.setExclusive(false);
    exactMatchConfiguration.setExactFactor(10.0f);
    exactMatchConfiguration.setWithPrefix(true);
    exactMatchConfiguration.setPrefixFactor(6.0f);
    exactMatchConfiguration.setCaseSensitivityFactor(0.7f);
    exactMatchConfiguration.setEnableStructured(true);

    WordGramConfiguration wordGramConfiguration = new WordGramConfiguration();
    wordGramConfiguration.setTwoGramFactor(1.2f);
    wordGramConfiguration.setThreeGramFactor(1.5f);
    wordGramConfiguration.setFourGramFactor(1.8f);

    PartialConfiguration partialConfiguration = new PartialConfiguration();
    partialConfiguration.setFactor(0.4f);
    partialConfiguration.setUrnFactor(0.7f);

    testQueryConfig.setExactMatch(exactMatchConfiguration);
    testQueryConfig.setWordGram(wordGramConfiguration);
    testQueryConfig.setPartial(partialConfiguration);
  }
  public static final SearchQueryBuilder TEST_BUILDER = new SearchQueryBuilder(testQueryConfig, null);

  @Test
  public void testQueryBuilderFulltext() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder) TEST_BUILDER.buildQuery(ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery",
                true);
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 2);

    BoolQueryBuilder analyzerGroupQuery = (BoolQueryBuilder) shouldQueries.get(0);

    SimpleQueryStringBuilder keywordQuery = (SimpleQueryStringBuilder) analyzerGroupQuery.should().get(0);
    assertEquals(keywordQuery.value(), "testQuery");
    assertEquals(keywordQuery.analyzer(), "keyword");
    Map<String, Float> keywordFields = keywordQuery.fields();
    assertEquals(keywordFields.size(), 9);
    assertEquals(keywordFields, Map.of(
        "urn", 10.f,
        "textArrayField", 1.0f,
        "customProperties", 1.0f,
        "wordGramField", 1.0f,
        "nestedArrayArrayField", 1.0f,
        "textFieldOverride", 1.0f,
        "nestedArrayStringField", 1.0f,
        "keyPart1", 10.0f,
        "esObjectField", 1.0f
    ));

    SimpleQueryStringBuilder urnComponentQuery = (SimpleQueryStringBuilder) analyzerGroupQuery.should().get(1);
    assertEquals(urnComponentQuery.value(), "testQuery");
    assertEquals(urnComponentQuery.analyzer(), URN_SEARCH_ANALYZER);
    assertEquals(urnComponentQuery.fields(), Map.of(
            "nestedForeignKey", 1.0f,
            "foreignKey", 1.0f
    ));

    SimpleQueryStringBuilder fulltextQuery = (SimpleQueryStringBuilder) analyzerGroupQuery.should().get(2);
    assertEquals(fulltextQuery.value(), "testQuery");
    assertEquals(fulltextQuery.analyzer(), TEXT_SEARCH_ANALYZER);
    assertEquals(fulltextQuery.fields(), Map.of(
            "textFieldOverride.delimited", 0.4f,
            "keyPart1.delimited", 4.0f,
            "nestedArrayArrayField.delimited", 0.4f,
            "urn.delimited", 7.0f,
            "textArrayField.delimited", 0.4f,
            "nestedArrayStringField.delimited", 0.4f,
            "wordGramField.delimited", 0.4f
    ));

    BoolQueryBuilder boolPrefixQuery = (BoolQueryBuilder) shouldQueries.get(1);
    assertTrue(boolPrefixQuery.should().size() > 0);

    List<Pair<String, Float>> prefixFieldWeights = boolPrefixQuery.should().stream().map(prefixQuery -> {
      if (prefixQuery instanceof MatchPhrasePrefixQueryBuilder) {
        MatchPhrasePrefixQueryBuilder builder = (MatchPhrasePrefixQueryBuilder) prefixQuery;
        return Pair.of(builder.fieldName(), builder.boost());
      } else if (prefixQuery instanceof TermQueryBuilder) {
        // exact
        TermQueryBuilder builder = (TermQueryBuilder) prefixQuery;
        return Pair.of(builder.fieldName(), builder.boost());
      } else { // if (prefixQuery instanceof MatchPhraseQueryBuilder) {
        // ngram
        MatchPhraseQueryBuilder builder = (MatchPhraseQueryBuilder) prefixQuery;
        return Pair.of(builder.fieldName(), builder.boost());
      }
    }).collect(Collectors.toList());

    assertEquals(prefixFieldWeights.size(), 28);

    List.of(
            Pair.of("urn", 100.0f),
            Pair.of("urn", 70.0f),
            Pair.of("keyPart1.delimited", 16.8f),
            Pair.of("keyPart1.keyword", 100.0f),
            Pair.of("keyPart1.keyword", 70.0f),
            Pair.of("wordGramField.wordGrams2", 1.44f),
            Pair.of("wordGramField.wordGrams3", 2.25f),
            Pair.of("wordGramField.wordGrams4", 3.2399998f),
            Pair.of("wordGramField.keyword", 10.0f),
            Pair.of("wordGramField.keyword", 7.0f)
    ).forEach(p -> assertTrue(prefixFieldWeights.contains(p), "Missing: " + p));

    // Validate scorer
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] scoringFunctions = result.filterFunctionBuilders();
    assertEquals(scoringFunctions.length, 3);
  }

  @Test
  public void testQueryBuilderStructured() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder) TEST_BUILDER.buildQuery(ImmutableList.of(TestEntitySpecBuilder.getSpec()),
            "testQuery", false);
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 2);

    QueryStringQueryBuilder keywordQuery = (QueryStringQueryBuilder) shouldQueries.get(0);
    assertEquals(keywordQuery.queryString(), "testQuery");
    assertNull(keywordQuery.analyzer());
    Map<String, Float> keywordFields = keywordQuery.fields();
    assertEquals(keywordFields.size(), 21);
    assertEquals(keywordFields.get("keyPart1").floatValue(), 10.0f);
    assertFalse(keywordFields.containsKey("keyPart3"));
    assertEquals(keywordFields.get("textFieldOverride").floatValue(), 1.0f);
    assertEquals(keywordFields.get("customProperties").floatValue(), 1.0f);
    assertEquals(keywordFields.get("esObjectField").floatValue(), 1.0f);

    // Validate scorer
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] scoringFunctions = result.filterFunctionBuilders();
    assertEquals(scoringFunctions.length, 3);
  }

  private static final SearchQueryBuilder TEST_CUSTOM_BUILDER;
  static {
    try {
      CustomConfiguration customConfiguration = new CustomConfiguration();
      customConfiguration.setEnabled(true);
      customConfiguration.setFile("search_config_builder_test.yml");
      CustomSearchConfiguration customSearchConfiguration = customConfiguration.resolve(new YAMLMapper());
      TEST_CUSTOM_BUILDER = new SearchQueryBuilder(testQueryConfig, customSearchConfiguration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testCustomSelectAll() {
    for (String triggerQuery : List.of("*", "")) {
      FunctionScoreQueryBuilder result = (FunctionScoreQueryBuilder) TEST_CUSTOM_BUILDER
              .buildQuery(ImmutableList.of(TestEntitySpecBuilder.getSpec()), triggerQuery, true);

      BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
      List<QueryBuilder> shouldQueries = mainQuery.should();
      assertEquals(shouldQueries.size(), 0);
    }
  }

  @Test
  public void testCustomExactMatch() {
    for (String triggerQuery : List.of("test_table", "'single quoted'", "\"double quoted\"")) {
      FunctionScoreQueryBuilder result = (FunctionScoreQueryBuilder) TEST_CUSTOM_BUILDER
              .buildQuery(ImmutableList.of(TestEntitySpecBuilder.getSpec()), triggerQuery, true);

      BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
      List<QueryBuilder> shouldQueries = mainQuery.should();
      assertEquals(shouldQueries.size(), 1, String.format("Expected query for `%s`", triggerQuery));

      BoolQueryBuilder boolPrefixQuery = (BoolQueryBuilder) shouldQueries.get(0);
      assertTrue(boolPrefixQuery.should().size() > 0);

      List<QueryBuilder> queries = boolPrefixQuery.should().stream().map(prefixQuery -> {
        if (prefixQuery instanceof MatchPhrasePrefixQueryBuilder) {
          // prefix
          return (MatchPhrasePrefixQueryBuilder) prefixQuery;
        } else if (prefixQuery instanceof TermQueryBuilder) {
          // exact
          return (TermQueryBuilder) prefixQuery;
        } else { // if (prefixQuery instanceof MatchPhraseQueryBuilder) {
          // ngram
          return (MatchPhraseQueryBuilder) prefixQuery;
        }
      }).collect(Collectors.toList());

      assertFalse(queries.isEmpty(), "Expected queries with specific types");
    }
  }

  @Test
  public void testCustomDefault() {
    for (String triggerQuery : List.of("foo", "bar", "foo\"bar", "foo:bar")) {
      FunctionScoreQueryBuilder result = (FunctionScoreQueryBuilder) TEST_CUSTOM_BUILDER
              .buildQuery(ImmutableList.of(TestEntitySpecBuilder.getSpec()), triggerQuery, true);

      BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
      List<QueryBuilder> shouldQueries = mainQuery.should();
      assertEquals(shouldQueries.size(), 3);

      List<QueryBuilder> queries = mainQuery.should().stream().map(query -> {
        if (query instanceof SimpleQueryStringBuilder) {
          return (SimpleQueryStringBuilder) query;
        } else if (query instanceof MatchAllQueryBuilder) {
          // custom
          return (MatchAllQueryBuilder) query;
        } else {
          // exact
          return (BoolQueryBuilder) query;
        }
      }).collect(Collectors.toList());

      assertEquals(queries.size(), 3, "Expected queries with specific types");

      // validate query injection
      List<QueryBuilder> mustQueries = mainQuery.must();
      assertEquals(mustQueries.size(), 1);
      TermQueryBuilder termQueryBuilder = (TermQueryBuilder) mainQuery.must().get(0);

      assertEquals(termQueryBuilder.fieldName(), "fieldName");
      assertEquals(termQueryBuilder.value().toString(), triggerQuery);
    }
  }
}
