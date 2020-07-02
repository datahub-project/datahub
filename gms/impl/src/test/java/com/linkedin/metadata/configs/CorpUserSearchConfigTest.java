package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.search.CorpUserInfoDocument;
import com.linkedin.metadata.utils.ESTestUtils;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class CorpUserSearchConfigTest {

  private ESSearchDAO _corpUserInfoSearchDAO;
  private CorpUserSearchConfig _corpUserInfoSearchConfig;
  private SearchResponse _autoCompleteResponse;

  @BeforeMethod
  public void setup() throws Exception {
    _corpUserInfoSearchConfig = new CorpUserSearchConfig();
    _corpUserInfoSearchDAO = new ESSearchDAO(null, CorpUserInfoDocument.class, _corpUserInfoSearchConfig);
    String esSearchResponseString =   loadJsonFromResource("corpUserESAutocompleteResponse.json");
    _autoCompleteResponse = ESTestUtils.getSearchResponseFromJSON(esSearchResponseString);
  }

  @Test
  public void testConstructAutoCompleteQuery() throws Exception {

    Map<String, String> requestMap = new HashMap<>();
    String input = "fbar";
    String field = "ldap";

    String queryOnly = _corpUserInfoSearchConfig.getAutocompleteQueryTemplate();
    queryOnly = queryOnly.replace("$INPUT", input).replace("$FIELD", field);
    // convert using base64 encoding
    String encodedQuery = Base64.getEncoder().encodeToString(queryOnly.getBytes("UTF-8"));

    String expectedSearchRequest = loadJsonFromResource("corpUserESAutocompleteRequest.json");
    expectedSearchRequest = expectedSearchRequest.replace("$ENCODED_QUERY", encodedQuery);

    Filter filter = QueryUtils.newFilter(requestMap);

    SearchRequest searchRequest = _corpUserInfoSearchDAO.constructAutoCompleteQuery(input, field, filter);
    assertEquals(searchRequest.source().toString(), expectedSearchRequest);
  }

  @Test
  public void testConstructSearchQuery() throws Exception {

    String expectedSearchRequest = loadJsonFromResource("corpUserESSearchRequest.json");

    int from = 0;
    int size = 10;

    String input = "fbar";

    Map<String, String> requestMap = new HashMap<>();
    requestMap.put("title", "MockUser");
    Filter filter = QueryUtils.newFilter(requestMap);

    String queryOnly = _corpUserInfoSearchConfig.getSearchQueryTemplate();
    queryOnly = queryOnly.replace("$INPUT", input);

    // convert using base64 encoding
    String encodedQuery = Base64.getEncoder().encodeToString(queryOnly.getBytes("UTF-8"));
    expectedSearchRequest = expectedSearchRequest.replace("$ENCODED_QUERY", encodedQuery);

    SearchRequest searchRequest = _corpUserInfoSearchDAO.constructSearchQuery(input, filter, null, from, size);

    assertEquals(searchRequest.source().toString().trim(), expectedSearchRequest.trim());
  }

  @Test
  public void testOwnersAutoCompleteResult() {

    String input = "fbar";
    String field = "ldap";
    int limit = 5;

    AutoCompleteResult autoCompleteResult =
        _corpUserInfoSearchDAO.extractAutoCompleteResult(_autoCompleteResponse, input, field, limit);
    assertNotNull(autoCompleteResult);
    assertEquals(autoCompleteResult.getQuery(), input);
    assertNotNull(autoCompleteResult.getSuggestions());
    assertEquals(autoCompleteResult.getSuggestions().size(), 5);
  }
}