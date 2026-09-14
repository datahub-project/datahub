package com.linkedin.metadata.resources.analytics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.analytics.GetTimeseriesAggregatedStatsResponse;
import com.linkedin.metadata.authorization.TimeseriesAuthUtil;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.GenericTable;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AnalyticsTest {

  private Analytics analytics;
  private TimeseriesAspectService timeseriesAspectService;

  @BeforeMethod
  public void setUp() {
    analytics = new Analytics();
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    analytics.setTimeseriesAspectService(timeseriesAspectService);
    analytics.setAuthorizer(mock(Authorizer.class));
    analytics.setSystemOperationContext(TestOperationContexts.systemContextNoSearchAuthorization());

    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "tester"));
    AuthenticationContext.setAuthentication(authentication);
  }

  @AfterMethod
  public void tearDown() {
    AuthenticationContext.remove();
  }

  @Test
  public void testGetTimeseriesStatsForbiddenDoesNotQuery() {
    try (MockedStatic<TimeseriesAuthUtil> auth = mockStatic(TimeseriesAuthUtil.class)) {
      auth.when(
              () ->
                  TimeseriesAuthUtil.canReadAggregatedStats(
                      any(OperationContext.class), anyString(), anyString(), nullable(Filter.class)))
          .thenReturn(false);

      RestLiServiceException thrown =
          expectThrows(
              RestLiServiceException.class,
              () ->
                  analytics.getTimeseriesStats(
                      "dataset",
                      "datasetProfile",
                      new AggregationSpec[] {aggregationSpec()},
                      null,
                      null));

      assertEquals(thrown.getStatus(), HttpStatus.S_403_FORBIDDEN);
      verify(timeseriesAspectService, never())
          .getAggregatedStats(any(), any(), any(), any(), any(), any());
    }
  }

  @Test
  public void testGetTimeseriesStatsAuthorizedQueries() {
    GenericTable table = new GenericTable();
    when(timeseriesAspectService.getAggregatedStats(any(), any(), any(), any(), any(), any()))
        .thenReturn(table);

    try (MockedStatic<TimeseriesAuthUtil> auth = mockStatic(TimeseriesAuthUtil.class)) {
      auth.when(
              () ->
                  TimeseriesAuthUtil.canReadAggregatedStats(
                      any(OperationContext.class),
                      eq("dataset"),
                      eq("datasetProfile"),
                      isNull()))
          .thenReturn(true);

      Task<GetTimeseriesAggregatedStatsResponse> task =
          analytics.getTimeseriesStats(
              "dataset",
              "datasetProfile",
              new AggregationSpec[] {aggregationSpec()},
              null,
              null);

      assertNotNull(task);
      verify(timeseriesAspectService)
          .getAggregatedStats(any(), eq("dataset"), eq("datasetProfile"), any(), isNull(), isNull());
    }
  }

  private static AggregationSpec aggregationSpec() {
    return new AggregationSpec().setFieldPath("rowCount").setAggregationType(AggregationType.LATEST);
  }
}
