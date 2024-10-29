package com.linkedin.metadata.resources.operations;

import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.util.Pair;
import java.util.List;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import mock.MockTimeseriesAspectService;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class OperationsResourceTest {
  private static final String TASK_ID = "taskId123";

  @Test
  public void testDryRun() {
    TimeseriesAspectService mockTimeseriesAspectService = new MockTimeseriesAspectService();
    String entityType = "dataset";
    String aspectName = "datasetusagestatistics";
    long endTimeMillis = 3000;
    try (MockedStatic<AuthenticationContext> utilities = Mockito.mockStatic(AuthenticationContext.class)) {
      utilities.when(AuthenticationContext::getAuthentication).thenReturn(new Authentication(new Actor(ActorType.USER, "urn:li:corpuser:test"), ""));

      OperationsResource testResource = new OperationsResource(TestOperationContexts.systemContextNoSearchAuthorization(), mockTimeseriesAspectService);
      String output =
              testResource.executeTruncateTimeseriesAspect(
                      entityType, aspectName, endTimeMillis, true, null, null, null, null);
      assertTrue(output.contains("This was a dry run"));
      output =
              testResource.executeTruncateTimeseriesAspect(
                      entityType, aspectName, endTimeMillis, false, null, null, null, null);
      assertEquals(TASK_ID, output);
    }
  }

  @Test
  public void testIsTaskIdValid() {
    assertFalse(OperationsResource.isTaskIdValid("hello"));
    assertTrue(OperationsResource.isTaskIdValid("aB1cdEf2GHIJKLMnoPQr3S:123456"));
    assertFalse(OperationsResource.isTaskIdValid("123456:aB1cdEf2GHIJKLMnoPQr3S"));
    assertFalse(OperationsResource.isTaskIdValid(":123"));
    // node can have a - in it
    assertTrue(OperationsResource.isTaskIdValid("qhxGdzytQS-pQek8CwBCZg:54654"));
    assertTrue(OperationsResource.isTaskIdValid("qhxGdzytQSpQek8CwBCZg_:54654"));
  }

  @Test
  public void testForceFlags() {
    final String reindexTaskId = "REINDEX_TASK_ID";
    TimeseriesAspectService mockTimeseriesAspectServiceWouldDeleteByQuery =
            new MockTimeseriesAspectService();
    TimeseriesAspectService mockTimeseriesAspectServiceWouldReindex =
            new MockTimeseriesAspectService(30, 20, reindexTaskId);
    String entityType = "dataset";
    String aspectName = "datasetusagestatistics";
    long endTimeMillis = 3000;

    try (MockedStatic<AuthenticationContext> utilities = Mockito.mockStatic(AuthenticationContext.class)) {
      utilities.when(AuthenticationContext::getAuthentication).thenReturn(new Authentication(new Actor(ActorType.USER, "urn:li:corpuser:test"), ""));

      OperationsResource testResourceWouldReindex =
              new OperationsResource(TestOperationContexts.systemContextNoSearchAuthorization(), mockTimeseriesAspectServiceWouldReindex);
      OperationsResource testResourceWouldDeleteByQuery =
              new OperationsResource(TestOperationContexts.systemContextNoSearchAuthorization(), mockTimeseriesAspectServiceWouldDeleteByQuery);

      String result =
              testResourceWouldReindex.executeTruncateTimeseriesAspect(
                      entityType, aspectName, endTimeMillis, true, null, null, true, true);
      String errorIfFlagsAreIncompatable = "please only set forceReindex OR forceDeleteByQuery flags";
      assertEquals(errorIfFlagsAreIncompatable, result);

      result =
              testResourceWouldReindex.executeTruncateTimeseriesAspect(
                      entityType, aspectName, endTimeMillis, true, null, null, false, false);
      assertEquals(errorIfFlagsAreIncompatable, result);

      List<Pair<Boolean, Boolean>> validOptionsNothingForced =
              List.of(Pair.of(null, null), Pair.of(null, false), Pair.of(false, null));
      for (Pair<Boolean, Boolean> values : validOptionsNothingForced) {
        String reindexResult =
                testResourceWouldReindex.executeTruncateTimeseriesAspect(
                        entityType,
                        aspectName,
                        endTimeMillis,
                        true,
                        null,
                        null,
                        values.getFirst(),
                        values.getSecond());
        assertNotSame(errorIfFlagsAreIncompatable, reindexResult);
        assertTrue(reindexResult.contains("Reindexing the aspect without the deleted records"));
        String deleteResult =
                testResourceWouldDeleteByQuery.executeTruncateTimeseriesAspect(
                        entityType,
                        aspectName,
                        endTimeMillis,
                        true,
                        null,
                        null,
                        values.getFirst(),
                        values.getSecond());
        assertNotSame(errorIfFlagsAreIncompatable, deleteResult);
        assertTrue(deleteResult.contains("Issuing a delete by query request. "));
      }

      List<Pair<Boolean, Boolean>> validOptionsForceDeleteByQuery =
              List.of(Pair.of(true, null), Pair.of(true, false));
      for (Pair<Boolean, Boolean> values : validOptionsForceDeleteByQuery) {
        String reindexResult =
                testResourceWouldReindex.executeTruncateTimeseriesAspect(
                        entityType,
                        aspectName,
                        endTimeMillis,
                        true,
                        null,
                        null,
                        values.getFirst(),
                        values.getSecond());
        String deleteResult =
                testResourceWouldDeleteByQuery.executeTruncateTimeseriesAspect(
                        entityType,
                        aspectName,
                        endTimeMillis,
                        true,
                        null,
                        null,
                        values.getFirst(),
                        values.getSecond());
        for (String res : List.of(reindexResult, deleteResult)) {
          assertNotSame(errorIfFlagsAreIncompatable, res);
          assertTrue(res.contains("Issuing a delete by query request. "));
        }
      }
      List<Pair<Boolean, Boolean>> validOptionsForceReindex =
              List.of(Pair.of(null, true), Pair.of(false, true));
      for (Pair<Boolean, Boolean> values : validOptionsForceReindex) {
        String reindexResult =
                testResourceWouldReindex.executeTruncateTimeseriesAspect(
                        entityType,
                        aspectName,
                        endTimeMillis,
                        true,
                        null,
                        null,
                        values.getFirst(),
                        values.getSecond());
        String deleteResult =
                testResourceWouldDeleteByQuery.executeTruncateTimeseriesAspect(
                        entityType,
                        aspectName,
                        endTimeMillis,
                        true,
                        null,
                        null,
                        values.getFirst(),
                        values.getSecond());
        for (String res : List.of(reindexResult, deleteResult)) {
          assertNotSame(errorIfFlagsAreIncompatable, res);
          assertTrue(res.contains("Reindexing the aspect without the deleted records"));
        }
      }
    }
  }
}
