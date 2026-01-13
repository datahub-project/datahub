package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.RateLimitInfo;
import com.linkedin.datahub.graphql.generated.RateLimitThrottledValue;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RateLimitInfoResolverTest {

  @Test
  public void testGetThrottledWhenSensorReturnsTrue() throws Exception {
    // Create mock sensor that returns true (throttled)
    ThrottleSensor mockSensor = mock(ThrottleSensor.class);
    when(mockSensor.getIsThrottled()).thenReturn(true);

    RateLimitInfoResolver resolver = new RateLimitInfoResolver(mockSensor);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RateLimitInfo result = resolver.get(mockEnv).get();

    // Verify result
    assertNotNull(result);
    assertEquals(result.getRateLimitThrottled(), RateLimitThrottledValue.THROTTLED);
    verify(mockSensor, times(1)).getIsThrottled();
  }

  @Test
  public void testGetNotThrottledWhenSensorReturnsFalse() throws Exception {
    // Create mock sensor that returns false (not throttled)
    ThrottleSensor mockSensor = mock(ThrottleSensor.class);
    when(mockSensor.getIsThrottled()).thenReturn(false);

    RateLimitInfoResolver resolver = new RateLimitInfoResolver(mockSensor);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RateLimitInfo result = resolver.get(mockEnv).get();

    // Verify result
    assertNotNull(result);
    assertEquals(result.getRateLimitThrottled(), RateLimitThrottledValue.NOT_THROTTLED);
    verify(mockSensor, times(1)).getIsThrottled();
  }

  @Test
  public void testGetNotThrottledWhenSensorIsNull() throws Exception {
    // Create resolver with null sensor
    RateLimitInfoResolver resolver = new RateLimitInfoResolver(null);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RateLimitInfo result = resolver.get(mockEnv).get();

    // Verify result
    assertNotNull(result);
    assertEquals(result.getRateLimitThrottled(), RateLimitThrottledValue.NOT_THROTTLED);
  }
}
