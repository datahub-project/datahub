package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.TestEntitySpecBuilder;
import java.util.List;
import java.util.Map;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class SearchQueryBuilderTest {

  @Test
  public void testQueryBuilderFulltext() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder) SearchQueryBuilder.buildQuery(TestEntitySpecBuilder.getSpec(), "testQuery",
                true);
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 2);

    SimpleQueryStringBuilder simpleQuery = (SimpleQueryStringBuilder) shouldQueries.get(0);
    assertEquals(simpleQuery.value(), "testQuery");
    assertNull(simpleQuery.analyzer());
    Map<String, Float> keywordFields = simpleQuery.fields();
    assertEquals(keywordFields.size(), 23);
    assertEquals(keywordFields.get("keyPart1").floatValue(), 10.0f);
    assertFalse(keywordFields.containsKey("keyPart3"));
    assertEquals(keywordFields.get("textFieldOverride").floatValue(), 1.0f);
    assertEquals(keywordFields.get("customProperties").floatValue(), 1.0f);
    assertEquals(keywordFields.get("esObjectField").floatValue(), 1.0f);

    QueryStringQueryBuilder escapedQuery = (QueryStringQueryBuilder) shouldQueries.get(1);
    assertEquals(escapedQuery.queryString(), "testQuery");
    assertNull(escapedQuery.analyzer());
    Map<String, Float> textFields = escapedQuery.fields();
    assertEquals(textFields.size(), 23);
    assertEquals(textFields.get("keyPart1.delimited").floatValue(), 4.0f);
    assertTrue(textFields.containsKey("keyPart1.ngram"));
    assertEquals(textFields.get("textFieldOverride.delimited").floatValue(), 0.4f);
    assertFalse(textFields.containsKey("textFieldOverride.ngram"));
    assertEquals(textFields.get("textArrayField.delimited").floatValue(), 0.4f);
    assertEquals(textFields.get("textArrayField.ngram").floatValue(), 0.1f);

    // Validate scorer
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] scoringFunctions = result.filterFunctionBuilders();
    assertEquals(scoringFunctions.length, 3);
  }

  @Test
  public void testQueryBuilderStructured() {
    FunctionScoreQueryBuilder result =
            (FunctionScoreQueryBuilder) SearchQueryBuilder.buildQuery(TestEntitySpecBuilder.getSpec(), "testQuery",
                    false);
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 1);

    QueryStringQueryBuilder keywordQuery = (QueryStringQueryBuilder) shouldQueries.get(0);
    assertEquals(keywordQuery.queryString(), "testQuery");
    assertEquals(keywordQuery.analyzer(), "custom_keyword");
    Map<String, Float> keywordFields = keywordQuery.fields();
    assertEquals(keywordFields.size(), 23);
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
