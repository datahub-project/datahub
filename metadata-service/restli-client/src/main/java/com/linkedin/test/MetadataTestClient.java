package com.linkedin.test;

import com.datahub.authentication.Authentication;
import com.linkedin.common.client.BaseClient;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class MetadataTestClient extends BaseClient {

  private static final TestRequestBuilders TEST_REQUEST_BUILDERS = new TestRequestBuilders();

  public MetadataTestClient(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Evaluate tests for the given urn and return the test results. If shouldPush is set to true, ingest the test results into DataHub
   *
   * @param urn Urn of the entity being evaluated
   * @param tests List of tests being evaluated. If null, runs all tests
   * @param authentication Authentication to access this functionality
   * @return
   */
  @Nonnull
  public TestResults evaluate(@Nonnull final Urn urn, @Nullable List<Urn> tests, boolean shouldPush,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {
    TestDoEvaluateRequestBuilder requestBuilder =
        TEST_REQUEST_BUILDERS.actionEvaluate().urnParam(urn.toString()).pushParam(shouldPush);
    if (tests != null) {
      requestBuilder.testsParam(new StringArray(tests.stream().map(Urn::toString).collect(Collectors.toList())));
    }
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }
}
