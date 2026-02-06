package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionStatus;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.metadata.utils.AssertionStatusUtils;
import com.linkedin.monitor.MonitorError;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorStatus;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

public class AssertionStatusResolver implements DataFetcher<CompletableFuture<AssertionStatus>> {

  private final AssertionService _assertionService;
  private final MonitorService _monitorService;

  public AssertionStatusResolver(
      final AssertionService assertionService, final MonitorService monitorService) {
    _assertionService =
        Objects.requireNonNull(assertionService, "assertionService must not be null");
    _monitorService = Objects.requireNonNull(monitorService, "monitorService must not be null");
  }

  @Override
  public CompletableFuture<AssertionStatus> get(final DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final Assertion assertion = (Assertion) environment.getSource();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> resolveStatus(context, assertion), this.getClass().getSimpleName(), "get");
  }

  @Nullable
  private AssertionStatus resolveStatus(final QueryContext context, final Assertion assertion) {
    final io.datahubproject.metadata.context.OperationContext opContext =
        context.getOperationContext();
    final com.linkedin.common.urn.Urn assertionUrn = UrnUtils.getUrn(assertion.getUrn());
    final AssertionRunSummary runSummary =
        _assertionService.getAssertionRunSummary(opContext, assertionUrn);
    if (runSummary != null && runSummary.getAssertionStatus() != null) {
      return AssertionStatus.valueOf(runSummary.getAssertionStatus().name());
    }

    final MonitorError monitorError = fetchMonitorError(opContext, assertionUrn);
    final AssertionRunEvent latestRunEvent =
        _assertionService.getLatestAssertionRunEvent(opContext, assertionUrn);
    final com.linkedin.assertion.AssertionResultType resultType =
        latestRunEvent != null && latestRunEvent.getResult() != null
            ? latestRunEvent.getResult().getType()
            : null;
    final com.linkedin.assertion.AssertionStatus status =
        AssertionStatusUtils.resolveStatus(monitorError, resultType);
    return status != null ? AssertionStatus.valueOf(status.name()) : null;
  }

  @Nullable
  private MonitorError fetchMonitorError(
      final io.datahubproject.metadata.context.OperationContext opContext,
      final com.linkedin.common.urn.Urn assertionUrn) {
    final com.linkedin.common.urn.Urn monitorUrn =
        _assertionService.getMonitorUrnForAssertion(opContext, assertionUrn);
    if (monitorUrn == null) {
      return null;
    }
    final MonitorInfo monitorInfo = _monitorService.getMonitorInfo(opContext, monitorUrn);
    if (monitorInfo == null || !monitorInfo.hasStatus()) {
      return null;
    }
    final MonitorStatus status = monitorInfo.getStatus();
    return status.hasError() ? status.getError() : null;
  }
}
