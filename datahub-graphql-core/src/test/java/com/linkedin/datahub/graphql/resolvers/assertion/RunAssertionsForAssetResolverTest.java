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
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RunAssertionsForAssetResolverTest {
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

    // Init mocks
    Mockito.when(
            assertionService.getAssertionUrnsForEntity(Mockito.any(), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo1));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo2));

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

    RunAssertionsForAssetResolver resolver =
        new RunAssertionsForAssetResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_DATASET_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("tagUrns"), Mockito.eq(Collections.emptyList())))
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

    // Init mocks
    Mockito.when(
            assertionService.getAssertionUrnsForEntity(Mockito.any(), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo1));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo2));

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

    RunAssertionsForAssetResolver resolver =
        new RunAssertionsForAssetResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_DATASET_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(false);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("tagUrns"), Mockito.eq(Collections.emptyList())))
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
  public void testGetSuccessFilterByTags() throws Exception {
    Urn assertionUrn1 = UrnUtils.getUrn("urn:li:assertion:test");
    Urn assertionUrn2 = UrnUtils.getUrn("urn:li:assertion:test2");

    Urn tagUrn1 = UrnUtils.getUrn("urn:li:tag:test1");
    Urn tagUrn2 = UrnUtils.getUrn("urn:li:tag:test2");

    MonitorService monitorService = Mockito.mock(MonitorService.class);
    AssertionService assertionService = Mockito.mock(AssertionService.class);

    AssertionInfo expectedAssertionInfo1 = new AssertionInfo();
    expectedAssertionInfo1.setType(AssertionType.VOLUME);
    expectedAssertionInfo1.setVolumeAssertion(
        new VolumeAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL));
    expectedAssertionInfo1.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    GlobalTags expectedTags1 =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn1)),
                        new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn2)))));

    AssertionInfo expectedAssertionInfo2 = new AssertionInfo();
    expectedAssertionInfo2.setType(AssertionType.SQL);
    expectedAssertionInfo2.setSqlAssertion(
        new SqlAssertionInfo().setType(SqlAssertionType.METRIC).setEntity(TEST_DATASET_URN));
    expectedAssertionInfo2.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));

    GlobalTags expectedTags2 =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(new TagAssociation().setTag(TagUrn.createFromUrn(tagUrn2)))));

    // Init mocks
    Mockito.when(
            assertionService.getAssertionUrnsForEntity(Mockito.any(), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo1, expectedTags1));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo2, expectedTags2));

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(assertionUrn1, new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(assertionUrn1)),
                Mockito.eq(false),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    RunAssertionsForAssetResolver resolver =
        new RunAssertionsForAssetResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_DATASET_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("tagUrns"), Mockito.eq(Collections.emptyList())))
        .thenReturn(ImmutableList.of(tagUrn1.toString())); // Only run assertions with tag1.
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.RunAssertionsResult result = resolver.get(mockEnv).get();

    // Verify Results - Only should run the first assertion based on tag match.
    Assert.assertEquals(result.getPassingCount(), 1);
    Assert.assertEquals(result.getFailingCount(), 0);
    Assert.assertEquals(result.getErrorCount(), 0);
    Assert.assertEquals(result.getResults().size(), 1);
    Assert.assertEquals(
        result.getResults().get(0).getAssertion().getUrn(), assertionUrn1.toString());
    Assert.assertEquals(
        result.getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
  }

  @Test
  public void testGetAssertionDoesNotExistFilteredOut() throws Exception {
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

    // Init mocks
    Mockito.when(
            assertionService.getAssertionUrnsForEntity(Mockito.any(), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo1));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(null);

    // ONLY RETURN ASSERTION 1 RESULT.
    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(assertionUrn1, new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(assertionUrn1)),
                Mockito.eq(false),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    RunAssertionsForAssetResolver resolver =
        new RunAssertionsForAssetResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_DATASET_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("tagUrns"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.RunAssertionsResult result = resolver.get(mockEnv).get();

    // Verify Null Assertion Was Simply Omitted.
    Assert.assertEquals(result.getPassingCount(), 1);
    Assert.assertEquals(result.getFailingCount(), 0);
    Assert.assertEquals(result.getErrorCount(), 0);
    Assert.assertEquals(result.getResults().size(), 1);
    Assert.assertEquals(
        result.getResults().get(0).getAssertion().getUrn(), assertionUrn1.toString());
    Assert.assertEquals(
        result.getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
  }

  @Test
  public void testGetFailureExternalAssertionFilteredOut() throws Exception {
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
    expectedAssertionInfo2.setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));

    // Init mocks
    Mockito.when(
            assertionService.getAssertionUrnsForEntity(Mockito.any(), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo1));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo2));

    Map<Urn, AssertionResult> resultMap =
        ImmutableMap.of(
            assertionUrn1,
            new AssertionResult().setType(AssertionResultType.SUCCESS),
            assertionUrn2,
            new AssertionResult().setType(AssertionResultType.FAILURE));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(assertionUrn1)),
                Mockito.eq(false),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(resultMap);

    RunAssertionsForAssetResolver resolver =
        new RunAssertionsForAssetResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_DATASET_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("tagUrns"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.RunAssertionsResult result = resolver.get(mockEnv).get();

    // Verify External Assertion Was Simply Omitted.
    Assert.assertEquals(result.getPassingCount(), 1);
    Assert.assertEquals(result.getFailingCount(), 0);
    Assert.assertEquals(result.getErrorCount(), 0);
    Assert.assertEquals(result.getResults().size(), 1);
    Assert.assertEquals(
        result.getResults().get(0).getAssertion().getUrn(), assertionUrn1.toString());
    Assert.assertEquals(
        result.getResults().get(0).getResult().getType(),
        com.linkedin.datahub.graphql.generated.AssertionResultType.SUCCESS);
  }

  @Test
  public void testGetSuccessWithParameters() throws Exception {
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

    // Init mocks
    Mockito.when(
            assertionService.getAssertionUrnsForEntity(Mockito.any(), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo1));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo2));

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

    RunAssertionsForAssetResolver resolver =
        new RunAssertionsForAssetResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_DATASET_URN.toString());
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
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("tagUrns"), Mockito.eq(Collections.emptyList())))
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

    // Init mocks
    Mockito.when(
            assertionService.getAssertionUrnsForEntity(Mockito.any(), Mockito.eq(TEST_DATASET_URN)))
        .thenReturn(ImmutableList.of(assertionUrn1, assertionUrn2));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn1)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo1));

    Mockito.when(
            assertionService.getAssertionEntityResponse(Mockito.any(), Mockito.eq(assertionUrn2)))
        .thenReturn(buildEntityResponse(expectedAssertionInfo2));

    Mockito.when(
            monitorService.runAssertions(
                Mockito.eq(ImmutableList.of(assertionUrn1, assertionUrn2)),
                Mockito.eq(false),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(false)))
        .thenReturn(null);

    RunAssertionsForAssetResolver resolver =
        new RunAssertionsForAssetResolver(monitorService, assertionService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();

    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_DATASET_URN.toString());
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("saveResults"), Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("async"), Mockito.eq(false)))
        .thenReturn(true);
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("parameters"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(
            mockEnv.getArgumentOrDefault(
                Mockito.eq("tagUrns"), Mockito.eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.RunAssertionsResult result = resolver.get(mockEnv).get();

    // Verify Results
    Assert.assertNull(result);
  }

  public static EntityResponse buildEntityResponse(@Nonnull AssertionInfo info) {
    return buildEntityResponse(info, null);
  }

  public static EntityResponse buildEntityResponse(
      @Nonnull AssertionInfo info, @Nullable GlobalTags maybeTags) {
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.ASSERTION_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(info.data())));
    if (maybeTags != null) {
      aspectMap.put(
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          new EnvelopedAspect().setValue(new Aspect(maybeTags.data())));
    }
    entityResponse.setAspects(aspectMap);
    return entityResponse;
  }
}
