package com.linkedin.metadata.configs;

import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.CorpGroupDocument;
import com.linkedin.metadata.utils.ESTestUtils;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.Filter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class CorpGroupSearchTest {

  private ESSearchDAO _corpGroupSearchDAO;
  private CorpGroupSearchConfig _corpGroupSearchConfig;

  @BeforeMethod
  public void setup() {
    _corpGroupSearchConfig = new CorpGroupSearchConfig();
    _corpGroupSearchDAO = new ESSearchDAO(null, CorpGroupDocument.class, _corpGroupSearchConfig);
  }

  @Test
  public void testConstructAutoCompleteQuery() throws Exception {

    Map<String, String> requestMap = new HashMap<>();
    String input = "jira";
    String field = "email";

    String queryOnly = _corpGroupSearchConfig.getAutocompleteQueryTemplate();
    queryOnly = queryOnly.replace("$INPUT", input).replace("$FIELD", field);
    // convert using base64 encoding
    String encodedQuery = Base64.getEncoder().encodeToString(queryOnly.getBytes("UTF-8"));

    String expectedSearchRequest = loadJsonFromResource("corpGroupESAutocompleteRequest.json");
    expectedSearchRequest = expectedSearchRequest.replace("$ENCODED_QUERY", encodedQuery);

    Filter filter = QueryUtils.newFilter(requestMap);

    SearchRequest searchRequest = _corpGroupSearchDAO.constructAutoCompleteQuery(input, field, filter);
    assertEquals(searchRequest.source().toString(), expectedSearchRequest);
  }

  @Test
  public void testAutoCompleteResult() throws Exception {

    String input = "jira";
    String field = "email";
    int limit = 5;
    String esSearchResponseString = loadJsonFromResource("corpGroupESAutocompleteResponse.json");
    SearchResponse searchResponse = ESTestUtils.getSearchResponseFromJSON(esSearchResponseString);

    AutoCompleteResult autoCompleteResult =
        _corpGroupSearchDAO.extractAutoCompleteResult(searchResponse, input, field, limit);

    assertNotNull(autoCompleteResult);
    assertEquals(autoCompleteResult.getQuery(), input);
    assertNotNull(autoCompleteResult.getSuggestions());
    assertEquals(autoCompleteResult.getSuggestions().get(0), "rap-jira@linkedin.com");
  }
}