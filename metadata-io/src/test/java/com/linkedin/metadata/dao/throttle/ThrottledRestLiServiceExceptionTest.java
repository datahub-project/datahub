package com.linkedin.metadata.dao.throttle;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.linkedin.metadata.throttle.ThrottleMechanismType;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import com.linkedin.metadata.throttle.ThrottleResponseSource;
import com.linkedin.restli.common.HttpStatus;
import java.util.Map;
import org.testng.annotations.Test;

public class ThrottledRestLiServiceExceptionTest {

  @Test
  public void testWrapsThrottleExceptionAndBuildsHeaders() {
    APIThrottleException throttleException =
        new APIThrottleException(
            5000L,
            "Throttled due to lag",
            "MCL_VERSIONED_LAG",
            ThrottleMechanismType.INGEST,
            ThrottleResponseSource.METADATA_WRITE);

    ThrottledRestLiServiceException exception =
        new ThrottledRestLiServiceException(throttleException);

    assertSame(exception.getThrottleException(), throttleException);
    assertEquals(exception.getStatus(), HttpStatus.S_429_TOO_MANY_REQUESTS);
    assertEquals(exception.getMessage(), "Throttled due to lag");

    Map<String, String> headers = exception.getThrottleResponseHeaders();
    assertEquals(headers.get(ThrottleResponseHeaders.RULE), "MCL_VERSIONED_LAG");
    assertEquals(headers.get(ThrottleResponseHeaders.TYPE), "ingest");
    assertEquals(headers.get(ThrottleResponseHeaders.SOURCE), "metadata-write");
    assertEquals(headers.get(ThrottleResponseHeaders.RETRY_AFTER), "5");
  }
}
