package com.linkedin.metadata.search.elasticsearch.query.request;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.TestEntitySpecBuilder;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class SearchQueryBuilderTest {

  @Test
  public void testQueryBuilderFulltext() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder) SearchQueryBuilder.buildQuery(ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery",
                true);
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 2);

    BoolQueryBuilder analyzerGroupQuery = (BoolQueryBuilder) shouldQueries.get(0);

    SimpleQueryStringBuilder keywordQuery = (SimpleQueryStringBuilder) analyzerGroupQuery.should().get(0);
    assertEquals(keywordQuery.value(), "testQuery");
    assertEquals(keywordQuery.analyzer(), "keyword");
    Map<String, Float> keywordFields = keywordQuery.fields();
    assertEquals(keywordFields.size(), 1);
    assertEquals(keywordFields.get("urn").floatValue(), 10.0f);

    SimpleQueryStringBuilder fulltextQuery = (SimpleQueryStringBuilder) analyzerGroupQuery.should().get(1);
    assertEquals(fulltextQuery.value(), "testQuery");
    assertEquals(fulltextQuery.analyzer(), TEXT_SEARCH_ANALYZER);
    Map<String, Float> fulltextFields = fulltextQuery.fields();
    assertEquals(fulltextFields.size(), 19);
    assertEquals(fulltextFields.get("keyPart1").floatValue(), 10.0f);
    assertFalse(fulltextFields.containsKey("keyPart3"));
    assertEquals(fulltextFields.get("textFieldOverride").floatValue(), 1.0f);
    assertEquals(fulltextFields.get("customProperties").floatValue(), 1.0f);
    assertEquals(fulltextFields.get("esObjectField").floatValue(), 1.0f);
    assertEquals(fulltextFields.get("keyPart1.delimited").floatValue(), 4.0f);
    assertFalse(fulltextFields.containsKey("keyPart3"));
    assertEquals(fulltextFields.get("textFieldOverride.delimited").floatValue(), 0.4f);

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
            Pair.of("keyPart1.delimited", 10.0f),
            Pair.of("keyPart1.keyword", 100.0f)
    ));

    // Validate scorer
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] scoringFunctions = result.filterFunctionBuilders();
    assertEquals(scoringFunctions.length, 3);
  }

  @Test
  public void testQueryBuilderStructured() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder) SearchQueryBuilder.buildQuery(ImmutableList.of(TestEntitySpecBuilder.getSpec()),
            "testQuery", false);
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 2);

    QueryStringQueryBuilder keywordQuery = (QueryStringQueryBuilder) shouldQueries.get(0);
    assertEquals(keywordQuery.queryString(), "testQuery");
    assertNull(keywordQuery.analyzer());
    Map<String, Float> keywordFields = keywordQuery.fields();
    assertEquals(keywordFields.size(), 20);
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
