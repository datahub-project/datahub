package com.linkedin.test;

import com.linkedin.common.client.BaseClient;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RestliMetadataTestClient extends BaseClient implements MetadataTestClient {

  private static final TestRequestBuilders TEST_REQUEST_BUILDERS = new TestRequestBuilders();

  public RestliMetadataTestClient(@Nonnull Client restliClient) {
    super(
        restliClient,
        EntityClientConfig.builder()
            .backoffPolicy(new ExponentialBackoff(1))
            .retryCount(1)
            .build());
  }

  /**
   * Evaluate tests for the given urn and return the test results. If shouldPush is set to true,
   * ingest the test results into DataHub
   *
   * @param urn Urn of the entity being evaluated
   * @param tests List of tests being evaluated. If null, runs all tests
   * @param operationContext Authentication to access this functionality
   * @return
   */
  @Nonnull
  public TestResults evaluate(
      @Nonnull final Urn urn,
      @Nullable List<Urn> tests,
      boolean shouldPush,
      @Nonnull final OperationContext operationContext)
      throws RemoteInvocationException {
    TestDoEvaluateRequestBuilder requestBuilder =
        TEST_REQUEST_BUILDERS.actionEvaluate().urnParam(urn.toString()).pushParam(shouldPush);
    if (tests != null) {
      requestBuilder.testsParam(
          new StringArray(tests.stream().map(Urn::toString).collect(Collectors.toList())));
    }
    return sendClientRequest(requestBuilder, operationContext).getEntity();
  }

  @Nonnull
  public BatchedTestResults evaluateSingleTest(
      @Nonnull final Urn testUrn,
      boolean shouldPush,
      @Nonnull final OperationContext operationContext)
      throws RemoteInvocationException {
    TestDoTestEvaluateRequestBuilder requestBuilder =
        TEST_REQUEST_BUILDERS
            .actionTestEvaluate()
            .urnParam(testUrn.toString())
            .pushParam(shouldPush);
    return sendClientRequest(requestBuilder, operationContext).getEntity();
  }
}
