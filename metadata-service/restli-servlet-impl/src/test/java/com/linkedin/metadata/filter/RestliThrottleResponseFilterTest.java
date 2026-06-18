package com.linkedin.metadata.filter;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.dao.throttle.APIThrottleException;
import com.linkedin.metadata.dao.throttle.ThrottledRestLiServiceException;
import com.linkedin.metadata.throttle.ThrottleMechanismType;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import com.linkedin.metadata.throttle.ThrottleResponseSource;
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
  public void testOnErrorAddsHeadersForThrottledRestLiServiceException() throws Exception {
    Map<String, String> headers = new HashMap<>();
    FilterResponseContext responseContext = mockResponseContext(headers);
    APIThrottleException throttleException =
        new APIThrottleException(
            30_000L,
            "Throttled",
            "mcl-versioned-lag",
            ThrottleMechanismType.INGEST,
            ThrottleResponseSource.METADATA_WRITE);
    ThrottledRestLiServiceException error = new ThrottledRestLiServiceException(throttleException);

    filter.onError(error, Mockito.mock(FilterRequestContext.class), responseContext).get();

    assertEquals(headers.get(ThrottleResponseHeaders.RULE), "mcl-versioned-lag");
    assertEquals(headers.get(ThrottleResponseHeaders.TYPE), "ingest");
    assertEquals(headers.get(ThrottleResponseHeaders.SOURCE), "metadata-write");
    assertEquals(headers.get(ThrottleResponseHeaders.RETRY_AFTER), "30");
  }

  @Test
  public void testOnErrorAddsHeadersForDirectApiThrottleException() throws Exception {
    Map<String, String> headers = new HashMap<>();
    FilterResponseContext responseContext = mockResponseContext(headers);
    APIThrottleException error =
        new APIThrottleException(
            5_000L,
            "Throttled",
            "manual",
            ThrottleMechanismType.INGEST,
            ThrottleResponseSource.METADATA_WRITE);

    filter.onError(error, Mockito.mock(FilterRequestContext.class), responseContext).get();

    assertEquals(headers.get(ThrottleResponseHeaders.RULE), "manual");
    assertEquals(headers.get(ThrottleResponseHeaders.RETRY_AFTER), "5");
  }

  @Test
  public void testOnErrorAddsHeadersForWrappedApiThrottleException() throws Exception {
    Map<String, String> headers = new HashMap<>();
    FilterResponseContext responseContext = mockResponseContext(headers);
    APIThrottleException throttleException =
        new APIThrottleException(
            1_000L,
            "Throttled",
            "wrapped-rule",
            ThrottleMechanismType.INGEST,
            ThrottleResponseSource.METADATA_WRITE);
    RuntimeException error = new RuntimeException("wrapper", throttleException);

    filter.onError(error, Mockito.mock(FilterRequestContext.class), responseContext).get();

    assertEquals(headers.get(ThrottleResponseHeaders.RULE), "wrapped-rule");
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
