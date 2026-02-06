package com.linkedin.datahub.graphql.resolvers.assertion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.datahub.graphql.generated.AssertionResultError;
import com.linkedin.datahub.graphql.generated.AssertionResultErrorType;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AssertionResultErrorResolversTest {

  @Test
  public void testDisplayMessageResolverUsesMapper() throws Exception {
    AssertionResultError error = new AssertionResultError();
    error.setType(AssertionResultErrorType.INVALID_PARAMETERS);

    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getSource()).thenReturn(error);

    AssertionResultErrorDisplayMessageResolver resolver =
        new AssertionResultErrorDisplayMessageResolver();
    String message = resolver.get(env).get();

    assertEquals(
        message,
        AssertionErrorMessageMapper.displayMessageForEvaluationError(
            AssertionResultErrorType.INVALID_PARAMETERS));
  }

  @Test
  public void testRecommendedActionResolverUsesMapper() throws Exception {
    AssertionResultError error = new AssertionResultError();
    error.setType(AssertionResultErrorType.INVALID_PARAMETERS);

    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getSource()).thenReturn(error);

    AssertionResultErrorRecommendedActionResolver resolver =
        new AssertionResultErrorRecommendedActionResolver();
    String action = resolver.get(env).get();

    assertEquals(
        action,
        AssertionErrorMessageMapper.recommendedActionForEvaluationError(
            AssertionResultErrorType.INVALID_PARAMETERS));
  }

  @Test
  public void testResolversReturnNullWhenErrorMissing() throws Exception {
    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getSource()).thenReturn(null);

    AssertionResultErrorDisplayMessageResolver displayResolver =
        new AssertionResultErrorDisplayMessageResolver();
    AssertionResultErrorRecommendedActionResolver actionResolver =
        new AssertionResultErrorRecommendedActionResolver();

    assertNull(displayResolver.get(env).get());
    assertNull(actionResolver.get(env).get());
  }
}
