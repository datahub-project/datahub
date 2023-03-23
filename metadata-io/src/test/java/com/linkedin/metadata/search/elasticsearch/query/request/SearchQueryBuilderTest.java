package com.linkedin.metadata.search.elasticsearch.query.request;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.TestEntitySpecBuilder;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.util.Pair;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
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

    PartialConfiguration partialConfiguration = new PartialConfiguration();
    partialConfiguration.setFactor(0.4f);
    partialConfiguration.setUrnFactor(0.7f);

    testQueryConfig.setExactMatch(exactMatchConfiguration);
    testQueryConfig.setPartial(partialConfiguration);
  }
  public static final SearchQueryBuilder TEST_BUILDER = new SearchQueryBuilder(testQueryConfig);

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
    assertEquals(keywordFields.size(), 8);
    assertEquals(keywordFields, Map.of(
       "urn", 10.f,
       "textArrayField", 1.0f,
       "customProperties", 1.0f,
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
            "nestedArrayStringField.delimited", 0.4f
    ));

    BoolQueryBuilder boolPrefixQuery = (BoolQueryBuilder) shouldQueries.get(1);
    assertTrue(boolPrefixQuery.should().size() > 0);

    List<Pair<String, Float>> prefixFieldWeights = boolPrefixQuery.should().stream().map(prefixQuery -> {
      if (prefixQuery instanceof MatchPhrasePrefixQueryBuilder) {
        MatchPhrasePrefixQueryBuilder builder = (MatchPhrasePrefixQueryBuilder) prefixQuery;
        return Pair.of(builder.fieldName(), builder.boost());
      } else {
        // exact
        TermQueryBuilder builder = (TermQueryBuilder) prefixQuery;
        return Pair.of(builder.fieldName(), builder.boost());
      }
    }).collect(Collectors.toList());

    assertEquals(prefixFieldWeights, List.of(
            Pair.of("urn", 100.0f),
            Pair.of("urn", 70.0f),
            Pair.of("keyPart1.delimited", 7.0f),
            Pair.of("keyPart1.keyword", 100.0f),
            Pair.of("keyPart1.keyword", 70.0f)
    ));

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
    assertEquals(keywordFields.size(), 16);
    assertEquals(keywordFields.get("keyPart1").floatValue(), 10.0f);
    assertFalse(keywordFields.containsKey("keyPart3"));
    assertEquals(keywordFields.get("textFieldOverride").floatValue(), 1.0f);
    assertEquals(keywordFields.get("customProperties").floatValue(), 1.0f);
    assertEquals(keywordFields.get("esObjectField").floatValue(), 1.0f);

    // Validate scorer
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] scoringFunctions = result.filterFunctionBuilders();
    assertEquals(scoringFunctions.length, 3);
  }
}
