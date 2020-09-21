package com.linkedin.metadata.configs;

import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.search.ChartDocument;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class ChartSearchConfigTest {
  private ESSearchDAO _esSearchDAO;
  private ChartSearchConfig _searchConfig;

  @BeforeMethod
  public void setup() {
    _searchConfig = new ChartSearchConfig();
    _esSearchDAO = new ESSearchDAO(null, ChartDocument.class, _searchConfig);
  }

  @Test
  public void testConstructAutoCompleteQuery() throws Exception {
    Map<String, String> requestMap = new HashMap<>();
    String input = "fooChart";
    String field = "title";

    String queryOnly = _searchConfig.getAutocompleteQueryTemplate();
    queryOnly = queryOnly.replace("$INPUT", input).replace("$FIELD", field);
    // convert using base64 encoding
    String encodedQuery = Base64.getEncoder().encodeToString(queryOnly.getBytes("UTF-8"));

    String expectedSearchRequest = loadJsonFromResource("chartESAutocompleteRequest.json");
    expectedSearchRequest = expectedSearchRequest.replace("$ENCODED_QUERY", encodedQuery);

    Filter filter = QueryUtils.newFilter(requestMap);

    SearchRequest searchRequest = _esSearchDAO.constructAutoCompleteQuery(input, field, filter);
    assertEquals(searchRequest.source().toString(), expectedSearchRequest);
  }

  @Test
  public void testConstructSearchQuery() throws Exception {
    String expectedSearchRequest = loadJsonFromResource("chartESSearchRequest.json");

    int from = 0;
    int size = 10;

    String input = "fooChart";

    Map<String, String> requestMap = new HashMap<>();
    requestMap.put("tool", "Looker");
    Filter filter = QueryUtils.newFilter(requestMap);

    String queryOnly = _searchConfig.getSearchQueryTemplate();
    queryOnly = queryOnly.replace("$INPUT", input);

    // convert using base64 encoding
    String encodedQuery = Base64.getEncoder().encodeToString(queryOnly.getBytes("UTF-8"));
    expectedSearchRequest = expectedSearchRequest.replace("$ENCODED_QUERY", encodedQuery);

    SearchRequest searchRequest = _esSearchDAO.constructSearchQuery(input, filter, null, from, size);

    assertEquals(searchRequest.source().toString().trim(), expectedSearchRequest.trim());
  }
}
