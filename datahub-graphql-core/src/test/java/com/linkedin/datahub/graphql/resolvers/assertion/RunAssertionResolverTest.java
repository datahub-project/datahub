package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RunAssertionResolverTest {
  private final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test-guid");
  private final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");

  @Test
  public void testGetSuccess() throws Exception {
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo = new AssertionInfo();
    expectedAssertionInfo.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));
    expectedAssertionInfo.setType(AssertionType.VOLUME);
    expectedAssertionInfo.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(expectedAssertionInfo);

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(
            TEST_ASSERTION_URN, new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(TEST_ASSERTION_URN)),
                Mockito.eq(false),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    RunAssertionResolver resolver = new RunAssertionResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResult"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.AssertionResult result = resolver.get(mockEnv).get();
    Assert.assertEquals(
        result.getType(), com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
  }

  @Test
  public void testGetAssertionSuccessDoNotSaveResult() throws Exception {
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo = new AssertionInfo();
    expectedAssertionInfo.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));
    expectedAssertionInfo.setType(AssertionType.VOLUME);
    expectedAssertionInfo.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(expectedAssertionInfo);

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(
            TEST_ASSERTION_URN, new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(TEST_ASSERTION_URN)),
                Mockito.eq(true),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(expectedAssertionInfo);

    RunAssertionResolver resolver = new RunAssertionResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResult"), Mockito.eq(true)))
        .thenReturn(false);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    com.linkedin.datahub.graphql.generated.AssertionResult result = resolver.get(mockEnv).get();
    Assert.assertEquals(
        result.getType(), com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
  }

  @Test
  public void testGetAssertionDoesNotExistException() throws Exception {
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(null);

    RunAssertionResolver resolver = new RunAssertionResolver(monitorService, assertionService);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResult"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(
        ExecutionException.class,
        () -> {
          resolver.get(mockEnv).get();
        });
  }

  @Test
  public void testGetFailureExternalAssertion() throws Exception {
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo = new AssertionInfo();
    // NO assertion source.
    expectedAssertionInfo.setType(AssertionType.VOLUME);
    expectedAssertionInfo.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(expectedAssertionInfo);

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(
            TEST_ASSERTION_URN, new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(TEST_ASSERTION_URN)),
                Mockito.eq(false),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    RunAssertionResolver resolver = new RunAssertionResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResult"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(
        ExecutionException.class,
        () -> {
          resolver.get(mockEnv).get();
        });
  }

  @Test
  public void testGetSuccessWithParameters() throws Exception {
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo = new AssertionInfo();
    expectedAssertionInfo.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));
    expectedAssertionInfo.setType(AssertionType.VOLUME);
    expectedAssertionInfo.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(expectedAssertionInfo);

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(
            TEST_ASSERTION_URN, new AssertionResult().setType(AssertionResultType.SUCCESS));

    Map<String, String> expectedParams =
        ImmutableMap.of(
            "param1", "value1",
            "param2", "value2");

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(TEST_ASSERTION_URN)),
                Mockito.eq(true),
                Mockito.eq(expectedParams),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(expectedAssertionInfo);

    RunAssertionResolver resolver = new RunAssertionResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResult"), Mockito.eq(true)))
        .thenReturn(false);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(
            ImmutableList.of(
                ImmutableMap.of(
                    "key", "param1",
                    "value", "value1"),
                ImmutableMap.of(
                    "key", "param2",
                    "value", "value2")));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    com.linkedin.datahub.graphql.generated.AssertionResult result = resolver.get(mockEnv).get();
    Assert.assertEquals(
        result.getType(), com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
  }

  @Test
  public void testGetSuccessAsync() throws Exception {
    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo = new AssertionInfo();
    expectedAssertionInfo.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));
    expectedAssertionInfo.setType(AssertionType.VOLUME);
    expectedAssertionInfo.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(expectedAssertionInfo);

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(TEST_ASSERTION_URN)),
                Mockito.eq(true),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(null);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(expectedAssertionInfo);

    RunAssertionResolver resolver = new RunAssertionResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResult"), Mockito.eq(true)))
        .thenReturn(false);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(true);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    com.linkedin.datahub.graphql.generated.AssertionResult result = resolver.get(mockEnv).get();
    Assert.assertNull(result);
  }
}
