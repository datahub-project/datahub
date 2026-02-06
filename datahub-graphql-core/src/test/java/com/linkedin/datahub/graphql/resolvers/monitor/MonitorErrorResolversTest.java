package com.linkedin.datahub.graphql.resolvers.monitor;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.datahub.graphql.generated.MonitorError;
import com.linkedin.datahub.graphql.generated.MonitorErrorType;
import com.linkedin.datahub.graphql.resolvers.assertion.AssertionErrorMessageMapper;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MonitorErrorResolversTest {

  @Test
  public void testDisplayMessageResolverUsesMapper() throws Exception {
    MonitorError error = new MonitorError();
    error.setType(MonitorErrorType.INPUT_DATA_INVALID);

    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getSource()).thenReturn(error);

    MonitorErrorDisplayMessageResolver resolver = new MonitorErrorDisplayMessageResolver();
    String message = resolver.get(env).get();

    assertEquals(
        message,
        AssertionErrorMessageMapper.displayMessageForMonitorError(
            MonitorErrorType.INPUT_DATA_INVALID));
  }

  @Test
  public void testRecommendedActionResolverUsesMapper() throws Exception {
    MonitorError error = new MonitorError();
    error.setType(MonitorErrorType.INPUT_DATA_INVALID);

    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getSource()).thenReturn(error);

    MonitorErrorRecommendedActionResolver resolver = new MonitorErrorRecommendedActionResolver();
    String action = resolver.get(env).get();

    assertEquals(
        action,
        AssertionErrorMessageMapper.recommendedActionForMonitorError(
            MonitorErrorType.INPUT_DATA_INVALID));
  }

  @Test
  public void testResolversReturnNullWhenErrorMissing() throws Exception {
    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getSource()).thenReturn(null);

    MonitorErrorDisplayMessageResolver displayResolver = new MonitorErrorDisplayMessageResolver();
    MonitorErrorRecommendedActionResolver actionResolver =
        new MonitorErrorRecommendedActionResolver();

    assertNull(displayResolver.get(env).get());
    assertNull(actionResolver.get(env).get());
  }
}
