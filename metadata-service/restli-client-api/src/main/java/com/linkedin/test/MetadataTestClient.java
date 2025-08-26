package com.linkedin.test;

import com.linkedin.common.urn.Urn;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface MetadataTestClient {
  @Nonnull
  TestResults evaluate(
      @Nonnull final Urn urn,
      @Nullable List<Urn> tests,
      boolean shouldPush,
      @Nonnull final OperationContext operationContext)
      throws RemoteInvocationException;

  @Nonnull
  BatchedTestResults evaluateSingleTest(
      @Nonnull final Urn testUrn,
      boolean shouldPush,
      @Nonnull final OperationContext operationContext)
      throws RemoteInvocationException;
}
