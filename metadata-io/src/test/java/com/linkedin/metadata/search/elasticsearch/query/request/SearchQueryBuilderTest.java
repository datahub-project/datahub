package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.TestEntitySpecBuilder;
import java.util.List;
import java.util.Map;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class SearchQueryBuilderTest {

  @Test
  public void testQueryBuilder() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder) SearchQueryBuilder.buildQuery(TestEntitySpecBuilder.getSpec(), "testQuery");
    BoolQueryBuilder mainQuery = (BoolQueryBuilder) result.query();
    List<QueryBuilder> shouldQueries = mainQuery.should();
    assertEquals(shouldQueries.size(), 2);
    QueryStringQueryBuilder keywordQuery = (QueryStringQueryBuilder) shouldQueries.get(0);
    assertEquals(keywordQuery.queryString(), "testQuery");
    assertEquals(keywordQuery.analyzer(), "custom_keyword");
    Map<String, Float> keywordFields = keywordQuery.fields();
    assertEquals(keywordFields.size(), 9);
    assertEquals(keywordFields.get("keyPart1").floatValue(), 10.0f);
    assertFalse(keywordFields.containsKey("keyPart3"));
    assertEquals(keywordFields.get("textFieldOverride").floatValue(), 1.0f);
    assertEquals(keywordFields.get("customProperties").floatValue(), 1.0f);
    assertEquals(keywordFields.get("esObjectField").floatValue(), 1.0f);
    QueryStringQueryBuilder textQuery = (QueryStringQueryBuilder) shouldQueries.get(1);
    assertEquals(textQuery.queryString(), "testQuery");
    assertEquals(textQuery.analyzer(), "word_delimited");
    Map<String, Float> textFields = textQuery.fields();
    assertEquals(textFields.size(), 7);
    assertEquals(textFields.get("keyPart1.delimited").floatValue(), 4.0f);
    assertFalse(textFields.containsKey("keyPart1.ngram"));
    assertEquals(textFields.get("textFieldOverride.delimited").floatValue(), 0.4f);
    assertFalse(textFields.containsKey("textFieldOverride.ngram"));
    assertEquals(textFields.get("textArrayField.delimited").floatValue(), 0.4f);
    assertEquals(textFields.get("textArrayField.ngram").floatValue(), 0.1f);

    // Validate scorer
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] scoringFunctions = result.filterFunctionBuilders();
    assertEquals(scoringFunctions.length, 3);
  }
}
