package com.linkedin.metadata.dao.throttle;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.metadata.throttle.ThrottleMechanismType;
import com.linkedin.metadata.throttle.ThrottleResponseSource;
import org.testng.annotations.Test;

public class APIThrottleExceptionTest {

  @Test
  public void testLegacyConstructorUsesIngestDefaults() {
    APIThrottleException exception = new APIThrottleException(5000L, "Throttled");

    assertEquals(exception.getDurationMs(), 5000L);
    assertEquals(exception.getDurationSeconds(), 5L);
    assertNull(exception.getRuleId());
    assertEquals(exception.getMechanismType(), ThrottleMechanismType.INGEST);
    assertEquals(exception.getSource(), ThrottleResponseSource.METADATA_WRITE);
    assertEquals(exception.getMessage(), "Throttled");
  }

  @Test
  public void testFullConstructorExposesMetadata() {
    APIThrottleException exception =
        new APIThrottleException(
            12_000L,
            "Throttled due to lag",
            "MCL_TIMESERIES_LAG",
            ThrottleMechanismType.INGEST,
            ThrottleResponseSource.METADATA_WRITE);

    assertEquals(exception.getDurationMs(), 12_000L);
    assertEquals(exception.getRuleId(), "MCL_TIMESERIES_LAG");
    assertEquals(exception.getMechanismType(), ThrottleMechanismType.INGEST);
    assertEquals(exception.getSource(), ThrottleResponseSource.METADATA_WRITE);
  }
}
