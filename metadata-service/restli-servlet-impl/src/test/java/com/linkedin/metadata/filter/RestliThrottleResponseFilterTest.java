package com.linkedin.metadata.filter;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.util.exception.DatabaseTransactionConflictException;
import com.linkedin.metadata.dao.throttle.DatabaseTransactionConflictRestLiServiceException;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import com.linkedin.restli.server.RestLiResponseData;
import com.linkedin.restli.server.filter.FilterRequestContext;
import com.linkedin.restli.server.filter.FilterResponseContext;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RestliThrottleResponseFilterTest {

  private final RestliThrottleResponseFilter filter = new RestliThrottleResponseFilter();

  @Test
  public void testOnErrorAddsRetryAfterForDatabaseTransactionConflict() throws Exception {
    Map<String, String> headers = new HashMap<>();
    FilterResponseContext responseContext = mockResponseContext(headers);
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001");
    DatabaseTransactionConflictRestLiServiceException error =
        new DatabaseTransactionConflictRestLiServiceException(conflict);

    filter.onError(error, Mockito.mock(FilterRequestContext.class), responseContext).get();

    assertEquals(headers.get(ThrottleResponseHeaders.RETRY_AFTER), "1");
  }

  @Test
  public void testOnErrorAddsRetryAfterForNestedDatabaseTransactionConflict() throws Exception {
    Map<String, String> headers = new HashMap<>();
    FilterResponseContext responseContext = mockResponseContext(headers);
    DatabaseTransactionConflictException conflict =
        new DatabaseTransactionConflictException(
            "Failed to add after 3 retries due to transaction conflict", "40001");
    DatabaseTransactionConflictRestLiServiceException nested =
        new DatabaseTransactionConflictRestLiServiceException(conflict);
    RuntimeException error = new RuntimeException("outer", new RuntimeException("mid", nested));

    filter.onError(error, Mockito.mock(FilterRequestContext.class), responseContext).get();

    assertEquals(headers.get(ThrottleResponseHeaders.RETRY_AFTER), "1");
  }

  @Test
  public void testOnErrorIgnoresNonThrottleExceptions() throws Exception {
    Map<String, String> headers = new HashMap<>();
    FilterResponseContext responseContext = mockResponseContext(headers);

    filter
        .onError(
            new IllegalStateException("boom"),
            Mockito.mock(FilterRequestContext.class),
            responseContext)
        .get();

    assertTrue(headers.isEmpty());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static FilterResponseContext mockResponseContext(Map<String, String> headers) {
    FilterResponseContext responseContext = Mockito.mock(FilterResponseContext.class);
    RestLiResponseData responseData = Mockito.mock(RestLiResponseData.class);
    Mockito.when(responseContext.getResponseData()).thenReturn(responseData);
    Mockito.when(responseData.getHeaders()).thenReturn(headers);
    return responseContext;
  }
}
