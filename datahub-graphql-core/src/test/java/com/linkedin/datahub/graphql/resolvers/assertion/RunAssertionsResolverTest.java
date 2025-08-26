package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
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

public class RunAssertionsResolverTest {
  private final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test-guid");
  private final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");

  @Test
  public void testGetSuccess() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo1 = new AssertionInfo();
    expectedAssertionInfo1.setType(AssertionType.VOLUME);
    expectedAssertionInfo1.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));
    expectedAssertionInfo1.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    AssertionInfo expectedAssertionInfo2 = new AssertionInfo();
    expectedAssertionInfo2.setType(AssertionType.SQL);
    expectedAssertionInfo2.setSqlAssertion(
        new SqlAssertionInfo().setType(SqlAssertionType.METRIC).setEntity(TEST_DATASET_URN));
    expectedAssertionInfo2.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(expectedAssertionInfo1);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(expectedAssertionInfo2);

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(
            assertionUrn1,
            new AssertionResult().setType(AssertionResultType.SUCCESS),
            assertionUrn2,
            new AssertionResult().setType(AssertionResultType.FAILURE));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(assertionUrn1, assertionUrn2)),
                Mockito.eq(false),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    RunAssertionsResolver resolver = new RunAssertionsResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(assertionUrn1.toString(), assertionUrn2.toString()));
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.RunAssertionsResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertEquals(result.getPassingCount(), 1);
    Assert.assertEquals(result.getFailingCount(), 1);
    Assert.assertEquals(result.getErrorCount(), 0);
    Assert.assertEquals(result.getResults().size(), 2);
    Assert.assertEquals(
        result.getResults().get(0).getAssertion().getUrn(), assertionUrn1.toString());
    Assert.assertEquals(
        result.getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
    Assert.assertEquals(
        result.getResults().get(1).getAssertion().getUrn(), assertionUrn2.toString());
    Assert.assertEquals(
        result.getResults().get(1).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.FAILURE);
  }

  @Test
  public void testGetAssertionSuccessDoNotSaveResult() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo1 = new AssertionInfo();
    expectedAssertionInfo1.setType(AssertionType.VOLUME);
    expectedAssertionInfo1.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));
    expectedAssertionInfo1.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    AssertionInfo expectedAssertionInfo2 = new AssertionInfo();
    expectedAssertionInfo2.setType(AssertionType.SQL);
    expectedAssertionInfo2.setSqlAssertion(
        new SqlAssertionInfo().setType(SqlAssertionType.METRIC).setEntity(TEST_DATASET_URN));
    expectedAssertionInfo2.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(expectedAssertionInfo1);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(expectedAssertionInfo2);

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(
            assertionUrn1,
            new AssertionResult().setType(AssertionResultType.SUCCESS),
            assertionUrn2,
            new AssertionResult().setType(AssertionResultType.FAILURE));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(assertionUrn1, assertionUrn2)),
                Mockito.eq(true),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    RunAssertionsResolver resolver = new RunAssertionsResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(assertionUrn1.toString(), assertionUrn2.toString()));
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(false);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.RunAssertionsResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertEquals(result.getPassingCount(), 1);
    Assert.assertEquals(result.getFailingCount(), 1);
    Assert.assertEquals(result.getErrorCount(), 0);
    Assert.assertEquals(result.getResults().size(), 2);
    Assert.assertEquals(
        result.getResults().get(0).getAssertion().getUrn(), assertionUrn1.toString());
    Assert.assertEquals(
        result.getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
    Assert.assertEquals(
        result.getResults().get(1).getAssertion().getUrn(), assertionUrn2.toString());
    Assert.assertEquals(
        result.getResults().get(1).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.FAILURE);
  }

  @Test
  public void testGetAssertionDoesNotExistException() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo1 = new AssertionInfo();
    expectedAssertionInfo1.setType(AssertionType.VOLUME);
    expectedAssertionInfo1.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));
    expectedAssertionInfo1.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    AssertionInfo expectedAssertionInfo2 = new AssertionInfo();
    expectedAssertionInfo2.setType(AssertionType.SQL);
    expectedAssertionInfo2.setSqlAssertion(
        new SqlAssertionInfo().setType(SqlAssertionType.METRIC).setEntity(TEST_DATASET_URN));
    expectedAssertionInfo2.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(expectedAssertionInfo1);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(null);

    RunAssertionsResolver resolver = new RunAssertionsResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(assertionUrn1.toString(), assertionUrn2.toString()));
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Verify throws - One Assertion Does not Exist!
    Assert.assertThrows(
        ExecutionException.class,
        () -> {
          resolver.get(mockEnv).get();
        });
  }

  @Test
  public void testGetFailureExternalAssertion() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo1 = new AssertionInfo();
    expectedAssertionInfo1.setType(AssertionType.VOLUME);
    expectedAssertionInfo1.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));
    expectedAssertionInfo1.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    AssertionInfo expectedAssertionInfo2 = new AssertionInfo();
    expectedAssertionInfo2.setType(AssertionType.SQL);
    expectedAssertionInfo2.setSqlAssertion(
        new SqlAssertionInfo().setType(SqlAssertionType.METRIC).setEntity(TEST_DATASET_URN));
    expectedAssertionInfo2.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(expectedAssertionInfo1);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(expectedAssertionInfo2);

    RunAssertionsResolver resolver = new RunAssertionsResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(assertionUrn1.toString(), assertionUrn2.toString()));
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(false);
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
  public void testGetSuccessWithParams() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo1 = new AssertionInfo();
    expectedAssertionInfo1.setType(AssertionType.VOLUME);
    expectedAssertionInfo1.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));
    expectedAssertionInfo1.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    AssertionInfo expectedAssertionInfo2 = new AssertionInfo();
    expectedAssertionInfo2.setType(AssertionType.SQL);
    expectedAssertionInfo2.setSqlAssertion(
        new SqlAssertionInfo().setType(SqlAssertionType.METRIC).setEntity(TEST_DATASET_URN));
    expectedAssertionInfo2.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(expectedAssertionInfo1);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(expectedAssertionInfo2);

    Map<String, String> expectedParams =
        ImmutableMap.of(
            "param1", "value1",
            "param2", "value2");

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(
            assertionUrn1,
            new AssertionResult().setType(AssertionResultType.SUCCESS),
            assertionUrn2,
            new AssertionResult().setType(AssertionResultType.FAILURE));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(assertionUrn1, assertionUrn2)),
                Mockito.eq(false),
                Mockito.eq(expectedParams),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    RunAssertionsResolver resolver = new RunAssertionsResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(assertionUrn1.toString(), assertionUrn2.toString()));
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(true);
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

    com.linkedin.datahub.graphql.generated.RunAssertionsResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertEquals(result.getPassingCount(), 1);
    Assert.assertEquals(result.getFailingCount(), 1);
    Assert.assertEquals(result.getErrorCount(), 0);
    Assert.assertEquals(result.getResults().size(), 2);
    Assert.assertEquals(
        result.getResults().get(0).getAssertion().getUrn(), assertionUrn1.toString());
    Assert.assertEquals(
        result.getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
    Assert.assertEquals(
        result.getResults().get(1).getAssertion().getUrn(), assertionUrn2.toString());
    Assert.assertEquals(
        result.getResults().get(1).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.FAILURE);
  }

  @Test
  public void testGetSuccessAsync() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo1 = new AssertionInfo();
    expectedAssertionInfo1.setType(AssertionType.VOLUME);
    expectedAssertionInfo1.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));
    expectedAssertionInfo1.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    AssertionInfo expectedAssertionInfo2 = new AssertionInfo();
    expectedAssertionInfo2.setType(AssertionType.SQL);
    expectedAssertionInfo2.setSqlAssertion(
        new SqlAssertionInfo().setType(SqlAssertionType.METRIC).setEntity(TEST_DATASET_URN));
    expectedAssertionInfo2.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(expectedAssertionInfo1);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(expectedAssertionInfo2);

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(assertionUrn1, assertionUrn2)),
                Mockito.eq(false),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(true)))
        .thenReturn(null);

    RunAssertionsResolver resolver = new RunAssertionsResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urns")))
        .thenReturn(ImmutableList.of(assertionUrn1.toString(), assertionUrn2.toString()));
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(true);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.RunAssertionsResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertNull(result);
  }
}
