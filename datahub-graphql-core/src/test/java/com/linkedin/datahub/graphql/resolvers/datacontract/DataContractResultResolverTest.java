package com.linkedin.datahub.graphql.resolvers.datacontract;

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
import com.linkedin.datahub.graphql.generated.DataContract;
import com.linkedin.datahub.graphql.generated.DataContractResultType;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.DataContractService;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataContractResultResolverTest {
  private final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private final Urn TEST_CONTRACT_URN = UrnUtils.getUrn("urn:li:dataContract:test");

  @Test
  public void testGetSuccessNoRefresh() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    DataContractService dataContractService = Mockito.mock(DataContractService.class);

    Mockito.when(
            dataContractService.getDataContractAssertionUrns(
                Mockito.any(), Mockito.eq(TEST_CONTRACT_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

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
            assertionService.getLatestAssertionRunResult(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.when(
            assertionService.getLatestAssertionRunResult(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(new AssertionResult().setType(AssertionResultType.FAILURE));

    DataContractResultResolver resolver =
        new DataContractResultResolver(monitorService, assertionService, dataContractService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    DataContract parentContract = new DataContract();
    parentContract.setUrn(TEST_CONTRACT_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentContract);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("refresh"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.DataContractResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertEquals(result.getType(), DataContractResultType.FAILING);
    Assert.assertEquals(result.getAssertionResults().getResults().size(), 2);
    Assert.assertEquals(result.getAssertionResults().getPassingCount(), 1);
    Assert.assertEquals(result.getAssertionResults().getFailingCount(), 1);
    Assert.assertEquals(result.getAssertionResults().getErrorCount(), 0);
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(0).getAssertion().getUrn(),
        assertionUrn1.toString());
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(1).getAssertion().getUrn(),
        assertionUrn2.toString());
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(1).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.FAILURE);
  }

  @Test
  public void testGetSuccessNoRefreshNoResults() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    DataContractService dataContractService = Mockito.mock(DataContractService.class);

    Mockito.when(
            dataContractService.getDataContractAssertionUrns(
                Mockito.any(), Mockito.eq(TEST_CONTRACT_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

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
            assertionService.getLatestAssertionRunResult(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(null);

    Mockito.when(
            assertionService.getLatestAssertionRunResult(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(null);

    DataContractResultResolver resolver =
        new DataContractResultResolver(monitorService, assertionService, dataContractService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    DataContract parentContract = new DataContract();
    parentContract.setUrn(TEST_CONTRACT_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentContract);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("refresh"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.DataContractResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertEquals(result.getType(), DataContractResultType.PASSING);
    Assert.assertEquals(result.getAssertionResults().getResults().size(), 0);
    Assert.assertEquals(result.getAssertionResults().getPassingCount(), 0);
    Assert.assertEquals(result.getAssertionResults().getFailingCount(), 0);
    Assert.assertEquals(result.getAssertionResults().getErrorCount(), 0);
  }

  @Test
  public void testGetSuccessNoRefreshUnrelatedResults() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    DataContractService dataContractService = Mockito.mock(DataContractService.class);

    Mockito.when(
            dataContractService.getDataContractAssertionUrns(
                Mockito.any(), Mockito.eq(TEST_CONTRACT_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

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
            assertionService.getLatestAssertionRunResult(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(new AssertionResult().setType(AssertionResultType.ERROR));

    Mockito.when(
            assertionService.getLatestAssertionRunResult(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(new AssertionResult().setType(AssertionResultType.INIT));

    DataContractResultResolver resolver =
        new DataContractResultResolver(monitorService, assertionService, dataContractService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    DataContract parentContract = new DataContract();
    parentContract.setUrn(TEST_CONTRACT_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentContract);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("refresh"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.DataContractResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertEquals(result.getType(), DataContractResultType.PASSING);
    Assert.assertEquals(result.getAssertionResults().getResults().size(), 2);
    Assert.assertEquals(result.getAssertionResults().getPassingCount(), 0);
    Assert.assertEquals(result.getAssertionResults().getFailingCount(), 0);
    Assert.assertEquals(result.getAssertionResults().getErrorCount(), 1);
  }

  @Test
  public void testGetAssertionSuccessRefreshAllNative() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    DataContractService dataContractService = Mockito.mock(DataContractService.class);

    Mockito.when(
            dataContractService.getDataContractAssertionUrns(
                Mockito.any(), Mockito.eq(TEST_CONTRACT_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

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

    DataContractResultResolver resolver =
        new DataContractResultResolver(monitorService, assertionService, dataContractService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    DataContract parentContract = new DataContract();
    parentContract.setUrn(TEST_CONTRACT_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentContract);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("refresh"), Mockito.eq(false)))
        .thenReturn(true);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.DataContractResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertEquals(result.getType(), DataContractResultType.FAILING);
    Assert.assertEquals(result.getAssertionResults().getResults().size(), 2);
    Assert.assertEquals(result.getAssertionResults().getPassingCount(), 1);
    Assert.assertEquals(result.getAssertionResults().getFailingCount(), 1);
    Assert.assertEquals(result.getAssertionResults().getErrorCount(), 0);
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(0).getAssertion().getUrn(),
        assertionUrn1.toString());
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(1).getAssertion().getUrn(),
        assertionUrn2.toString());
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(1).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.FAILURE);
  }

  @Test
  public void testGetAssertionSuccessRefreshMixed() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    DataContractService dataContractService = Mockito.mock(DataContractService.class);

    Mockito.when(
            dataContractService.getDataContractAssertionUrns(
                Mockito.any(), Mockito.eq(TEST_CONTRACT_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

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
    expectedAssertionInfo2.setSource(
        new AssertionSource()
            .setType(AssertionSourceType.EXTERNAL)); // External assertion means no native refresh!

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(expectedAssertionInfo1);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(expectedAssertionInfo2);

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(assertionUrn1, new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(assertionUrn1)),
                Mockito.eq(false),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    Mockito.when(
            assertionService.getLatestAssertionRunResult(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(new AssertionResult().setType(AssertionResultType.FAILURE));

    DataContractResultResolver resolver =
        new DataContractResultResolver(monitorService, assertionService, dataContractService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    DataContract parentContract = new DataContract();
    parentContract.setUrn(TEST_CONTRACT_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentContract);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("refresh"), Mockito.eq(false)))
        .thenReturn(true);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.DataContractResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertEquals(result.getType(), DataContractResultType.FAILING);
    Assert.assertEquals(result.getAssertionResults().getResults().size(), 2);
    Assert.assertEquals(result.getAssertionResults().getPassingCount(), 1);
    Assert.assertEquals(result.getAssertionResults().getFailingCount(), 1);
    Assert.assertEquals(result.getAssertionResults().getErrorCount(), 0);
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(0).getAssertion().getUrn(),
        assertionUrn2.toString());
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.FAILURE);
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(1).getAssertion().getUrn(),
        assertionUrn1.toString());
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(1).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
  }

  @Test
  public void testGetAssertionDoesNotExistFilteredOut() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    DataContractService dataContractService = Mockito.mock(DataContractService.class);

    Mockito.when(
            dataContractService.getDataContractAssertionUrns(
                Mockito.any(), Mockito.eq(TEST_CONTRACT_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

    AssertionInfo expectedAssertionInfo1 = new AssertionInfo();
    expectedAssertionInfo1.setType(AssertionType.VOLUME);
    expectedAssertionInfo1.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));
    expectedAssertionInfo1.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(expectedAssertionInfo1);

    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(null);

    Mockito.when(
            assertionService.getLatestAssertionRunResult(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(new AssertionResult().setType(AssertionResultType.SUCCESS));

    DataContractResultResolver resolver =
        new DataContractResultResolver(monitorService, assertionService, dataContractService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    DataContract parentContract = new DataContract();
    parentContract.setUrn(TEST_CONTRACT_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentContract);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("refresh"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.DataContractResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertEquals(result.getType(), DataContractResultType.PASSING);
    Assert.assertEquals(result.getAssertionResults().getResults().size(), 1);
    Assert.assertEquals(result.getAssertionResults().getPassingCount(), 1);
    Assert.assertEquals(result.getAssertionResults().getFailingCount(), 0);
    Assert.assertEquals(result.getAssertionResults().getErrorCount(), 0);
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(0).getAssertion().getUrn(),
        assertionUrn1.toString());
    Assert.assertEquals(
        result.getAssertionResults().getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
  }

  @Test
  public void testGetNoContractAssertions() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);
    DataContractService dataContractService = Mockito.mock(DataContractService.class);

    Mockito.when(
            dataContractService.getDataContractAssertionUrns(
                Mockito.any(), Mockito.eq(TEST_CONTRACT_URN)))
        .thenReturn(Collections.emptyList());

    DataContractResultResolver resolver =
        new DataContractResultResolver(monitorService, assertionService, dataContractService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    DataContract parentContract = new DataContract();
    parentContract.setUrn(TEST_CONTRACT_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentContract);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("refresh"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.DataContractResult result = resolver.get(mockEnv).get();

    // Verify Results. Contract is passing.
    Assert.assertEquals(result.getType(), DataContractResultType.PASSING);
    Assert.assertEquals(result.getAssertionResults().getResults().size(), 0);
    Assert.assertEquals(result.getAssertionResults().getPassingCount(), 0);
    Assert.assertEquals(result.getAssertionResults().getFailingCount(), 0);
    Assert.assertEquals(result.getAssertionResults().getErrorCount(), 0);
  }
}
