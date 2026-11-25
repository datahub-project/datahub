package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateEngineSupport.Support;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SearchTemplateEngineSupportTest {

  @DataProvider
  public Object[][] engines() {
    return new Object[][] {
      {SearchClientShim.SearchEngineType.ELASTICSEARCH_8, "8.18.0", Support.SUPPORTED},
      {SearchClientShim.SearchEngineType.OPENSEARCH_3, "3.5.0", Support.SUPPORTED},
      {SearchClientShim.SearchEngineType.OPENSEARCH_2, "2.19.3", Support.UNSUPPORTED},
      {SearchClientShim.SearchEngineType.OPENSEARCH_3, "3.4.0", Support.UNSUPPORTED},
      // Restricted cluster info hides the version, and compatibility mode reports 7.10.2 whatever
      // the cluster runs
      {SearchClientShim.SearchEngineType.OPENSEARCH_3, "unknown", Support.UNKNOWN},
      {SearchClientShim.SearchEngineType.OPENSEARCH_2, "7.10.2", Support.UNKNOWN},
      {SearchClientShim.SearchEngineType.UNKNOWN, "3.7.0", Support.UNKNOWN}
    };
  }

  @Test(dataProvider = "engines")
  public void testTheEngineReadsAsSupportedUnsupportedOrUnknown(
      SearchClientShim.SearchEngineType engine, String version, Support expected)
      throws IOException {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(engine);
    when(client.getEngineVersion()).thenReturn(version);
    OperationContext opContext =
        TestOperationContexts.withFixedSearchClient(
            TestOperationContexts.systemContextNoSearchAuthorization(), client);

    assertEquals(new SearchTemplateEngineSupport("{}", "{}", () -> 0L).read(opContext), expected);
  }
}
