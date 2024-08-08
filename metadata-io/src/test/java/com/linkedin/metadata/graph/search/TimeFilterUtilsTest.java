package com.linkedin.metadata.graph.search;

import com.google.common.io.Resources;
import com.linkedin.metadata.graph.elastic.TimeFilterUtils;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.opensearch.index.query.QueryBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TimeFilterUtilsTest {

  private static final String TEST_QUERY_FILE =
      "elasticsearch/sample_filters/lineage_time_query_filters_1.json";

  @Test
  private static void testGetEdgeTimeFilterQuery() throws Exception {
    URL url = Resources.getResource(TEST_QUERY_FILE);
    String expectedQuery = Resources.toString(url, StandardCharsets.UTF_8);
    long startTime = 1L;
    long endTime = 2L;
    QueryBuilder result = TimeFilterUtils.getEdgeTimeFilterQuery(startTime, endTime);
    Assert.assertEquals(result.toString(), expectedQuery);
  }
}
