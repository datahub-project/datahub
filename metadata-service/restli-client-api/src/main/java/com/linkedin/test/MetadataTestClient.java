package com.linkedin.test;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.r2.RemoteInvocationException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface MetadataTestClient {
  @Nonnull
  TestResults evaluate(
      @Nonnull final Urn urn,
      @Nullable List<Urn> tests,
      boolean shouldPush,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException;

  @Nonnull
  BatchedTestResults evaluateSingleTest(
      @Nonnull final Urn testUrn, boolean shouldPush, @Nonnull final Authentication authentication)
      throws RemoteInvocationException;
}
