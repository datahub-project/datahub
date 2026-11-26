package com.linkedin.metadata.recommendation.candidatesource;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UsageEventIndexCheckerTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final SearchClientShim<?> searchClient = mock(SearchClientShim.class);
  private final IndexConvention indexConvention = mock(IndexConvention.class);

  @BeforeMethod
  public void setup() {
    reset(searchClient, indexConvention);
    when(indexConvention.getIndexName(opContext, "datahub_usage_event"))
        .thenReturn("prefix_datahub_usage_event");
  }

  @AfterMethod
  public void resetCache() throws Exception {
    Field field = UsageEventIndexChecker.class.getDeclaredField("USAGE_INDEX_EXISTS_CACHE");
    field.setAccessible(true);
    ((Map<?, ?>) field.get(null)).clear();
  }

  @Test
  public void testReturnsIndexExistsResult() throws IOException {
    when(searchClient.indexExists(any(), any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);

    assertTrue(UsageEventIndexChecker.usageIndexExists(opContext, searchClient, indexConvention));
  }

  @Test
  public void testCachesResultAcrossCalls() throws IOException {
    when(searchClient.indexExists(any(), any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);

    UsageEventIndexChecker.usageIndexExists(opContext, searchClient, indexConvention);
    UsageEventIndexChecker.usageIndexExists(opContext, searchClient, indexConvention);
    UsageEventIndexChecker.usageIndexExists(opContext, searchClient, indexConvention);

    verify(searchClient, times(1))
        .indexExists(any(), any(GetIndexRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testDoesNotCacheOnFailureAndRetriesOnNextCall() throws IOException {
    when(searchClient.indexExists(any(), any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("simulated failure"))
        .thenReturn(true);

    assertFalse(UsageEventIndexChecker.usageIndexExists(opContext, searchClient, indexConvention));
    assertTrue(UsageEventIndexChecker.usageIndexExists(opContext, searchClient, indexConvention));

    verify(searchClient, times(2))
        .indexExists(any(), any(GetIndexRequest.class), any(RequestOptions.class));
  }
}
