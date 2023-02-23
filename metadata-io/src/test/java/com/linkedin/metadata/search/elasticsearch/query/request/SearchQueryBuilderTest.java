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

import static org.testng.Assert.*;

public class SearchQueryBuilderTest {

  @Test
  public void testQueryBuilderFulltext() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder) SearchQueryBuilder.buildQuery(ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery",
                true);
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 2);

    SimpleQueryStringBuilder simpleQuery = (SimpleQueryStringBuilder) shouldQueries.get(0);
    assertEquals(simpleQuery.value(), "testQuery");
    assertNull(simpleQuery.analyzer());
    Map<String, Float> keywordFields = simpleQuery.fields();
    assertEquals(keywordFields.size(), 20);
    assertEquals(keywordFields.get("keyPart1").floatValue(), 10.0f);
    assertFalse(keywordFields.containsKey("keyPart3"));
    assertEquals(keywordFields.get("textFieldOverride").floatValue(), 1.0f);
    assertEquals(keywordFields.get("customProperties").floatValue(), 1.0f);
    assertEquals(keywordFields.get("esObjectField").floatValue(), 1.0f);

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
