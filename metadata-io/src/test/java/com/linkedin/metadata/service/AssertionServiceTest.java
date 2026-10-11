package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultError;
import com.linkedin.assertion.AssertionResultErrorType;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.patch.builder.AssertionRunSummaryPatchBuilder;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AssertionServiceTest {
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_FIELD_URN =
      UrnUtils.getUrn(
          "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD),field1)");
  private static final Urn TEST_PLATFORM_URN = UrnUtils.getUrn("urn:li:dataPlatform:hive");
  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn("urn:li:monitor:(urn:li:dataset:test,test)");

  private static final Urn TEST_PLATFORM_INSTANCE_URN =
      UrnUtils.getUrn("urn:li:dataPlatformInstance:(urn:li:dataPlatform:custom,instance1)");

  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.userContextNoSearchAuthorization(TEST_ACTOR_URN);
  }

  @Test
  public void testUpsertCustomAssertionRequiredFields()
      throws Exception, RemoteInvocationException {
    SystemEntityClient mockedEntityClient = mock(SystemEntityClient.class);
    AssertionService assertionService =
        new AssertionService(mockedEntityClient, mock(GraphClient.class));

    String descriptionOfCustomAssertion = "Description of custom assertion";
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);

              MetadataChangeProposal proposal1 = aspects.get(0);
              Assert.assertEquals(proposal1.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal1.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              AssertionInfo info =
                  GenericRecordUtils.deserializeAspect(
                      proposal1.getAspect().getValue(),
                      proposal1.getAspect().getContentType(),
                      AssertionInfo.class);

              Assert.assertEquals(info.getType(), AssertionType.CUSTOM);
              Assert.assertEquals(info.getDescription(), descriptionOfCustomAssertion);
              Assert.assertEquals(info.getSource().getType(), AssertionSourceType.EXTERNAL);
              CustomAssertionInfo customAssertionInfo = info.getCustomAssertion();
              Assert.assertEquals(customAssertionInfo.getEntity(), TEST_DATASET_URN);

              MetadataChangeProposal proposal2 = aspects.get(1);
              Assert.assertEquals(proposal2.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal2.getAspectName(), DATA_PLATFORM_INSTANCE_ASPECT_NAME);
              DataPlatformInstance dataPlatformInstance =
                  GenericRecordUtils.deserializeAspect(
                      proposal2.getAspect().getValue(),
                      proposal2.getAspect().getContentType(),
                      DataPlatformInstance.class);
              Assert.assertEquals(dataPlatformInstance.getPlatform(), TEST_PLATFORM_URN);
              return null;
            })
        .when(mockedEntityClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    Urn urn =
        assertionService.upsertCustomAssertion(
            opContext,
            TEST_ASSERTION_URN,
            TEST_DATASET_URN,
            descriptionOfCustomAssertion,
            null,
            new DataPlatformInstance().setPlatform(TEST_PLATFORM_URN),
            new CustomAssertionInfo().setEntity(TEST_DATASET_URN));
    Assert.assertEquals(urn.getEntityType(), "assertion");

    Mockito.verify(mockedEntityClient, Mockito.times(1))
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));
  }

  @Test
  public void testUpsertCustomAssertionAllFields() throws Exception, RemoteInvocationException {
    SystemEntityClient mockedEntityClient = mock(SystemEntityClient.class);
    AssertionService assertionService =
        new AssertionService(mockedEntityClient, mock(GraphClient.class));
    String descriptionOfCustomAssertion = "Description of custom assertion";
    String externalUrlOfCustomAssertion = "https://xyz.com/abc";
    String customCategory = "Custom category";
    String customLogic = "select percentile(field1, 0.66) from table";
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);

              MetadataChangeProposal proposal1 = aspects.get(0);
              Assert.assertEquals(proposal1.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal1.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              AssertionInfo info =
                  GenericRecordUtils.deserializeAspect(
                      proposal1.getAspect().getValue(),
                      proposal1.getAspect().getContentType(),
                      AssertionInfo.class);

              Assert.assertEquals(info.getType(), AssertionType.CUSTOM);
              Assert.assertEquals(info.getDescription(), descriptionOfCustomAssertion);
              Assert.assertEquals(info.getExternalUrl().toString(), externalUrlOfCustomAssertion);
              Assert.assertEquals(info.getSource().getType(), AssertionSourceType.EXTERNAL);
              CustomAssertionInfo customAssertionInfo = info.getCustomAssertion();
              Assert.assertEquals(customAssertionInfo.getEntity(), TEST_DATASET_URN);
              Assert.assertEquals(customAssertionInfo.getField(), TEST_FIELD_URN);
              Assert.assertEquals(customAssertionInfo.getType(), customCategory);
              Assert.assertEquals(customAssertionInfo.getLogic(), customLogic);

              MetadataChangeProposal proposal2 = aspects.get(1);
              Assert.assertEquals(proposal2.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal2.getAspectName(), DATA_PLATFORM_INSTANCE_ASPECT_NAME);
              DataPlatformInstance dataPlatformInstance =
                  GenericRecordUtils.deserializeAspect(
                      proposal2.getAspect().getValue(),
                      proposal2.getAspect().getContentType(),
                      DataPlatformInstance.class);
              Assert.assertEquals(dataPlatformInstance.getPlatform(), TEST_PLATFORM_URN);
              Assert.assertEquals(dataPlatformInstance.getInstance(), TEST_PLATFORM_INSTANCE_URN);
              return null;
            })
        .when(mockedEntityClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    Urn urn =
        assertionService.upsertCustomAssertion(
            opContext,
            TEST_ASSERTION_URN,
            TEST_DATASET_URN,
            descriptionOfCustomAssertion,
            externalUrlOfCustomAssertion,
            new DataPlatformInstance()
                .setPlatform(TEST_PLATFORM_URN)
                .setInstance(TEST_PLATFORM_INSTANCE_URN),
            new CustomAssertionInfo()
                .setEntity(TEST_DATASET_URN)
                .setField(TEST_FIELD_URN)
                .setType(customCategory)
                .setLogic(customLogic));
    Assert.assertEquals(urn.getEntityType(), "assertion");

    Mockito.verify(mockedEntityClient, Mockito.times(1))
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));
  }

  @Test
  public void testAddAssertionRunEventRequiredFields() throws Exception, RemoteInvocationException {
    SystemEntityClient mockedEntityClient = mock(SystemEntityClient.class);
    AssertionService assertionService =
        new AssertionService(mockedEntityClient, mock(GraphClient.class));
    Long eventtime = 1718619000000L;

    Mockito.doAnswer(
            invocation -> {
              MetadataChangeProposal proposal = invocation.getArgument(1);

              Assert.assertEquals(proposal.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_RUN_EVENT_ASPECT_NAME);
              AssertionRunEvent runEvent =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionRunEvent.class);

              Assert.assertEquals(runEvent.getAssertionUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(runEvent.getAsserteeUrn(), TEST_DATASET_URN);
              Assert.assertEquals(runEvent.getTimestampMillis(), eventtime);
              Assert.assertEquals(runEvent.getStatus(), AssertionRunStatus.COMPLETE);

              AssertionResult result = runEvent.getResult();
              Assert.assertEquals(result.getType(), AssertionResultType.SUCCESS);

              return null;
            })
        .when(mockedEntityClient)
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));

    assertionService.addAssertionRunEvent(
        opContext,
        TEST_ASSERTION_URN,
        TEST_DATASET_URN,
        eventtime,
        new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.verify(mockedEntityClient, Mockito.times(1))
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));
  }

  @Test
  public void testAddAssertionRunEventAllFields() throws Exception, RemoteInvocationException {
    SystemEntityClient mockedEntityClient = mock(SystemEntityClient.class);
    AssertionService assertionService =
        new AssertionService(mockedEntityClient, mock(GraphClient.class));
    Long eventtime = 1718619000000L;
    StringMap nativeResults = new StringMap(Map.of("prop-1", "value-1"));
    StringMap errorProps = new StringMap(Map.of("message", "errorMessage"));
    String externalUrlOfAssertion = "https://abc/xyz";

    Mockito.doAnswer(
            invocation -> {
              MetadataChangeProposal proposal = invocation.getArgument(1);

              Assert.assertEquals(proposal.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_RUN_EVENT_ASPECT_NAME);
              AssertionRunEvent runEvent =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionRunEvent.class);

              Assert.assertEquals(runEvent.getAssertionUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(runEvent.getAsserteeUrn(), TEST_DATASET_URN);
              Assert.assertEquals(runEvent.getTimestampMillis(), eventtime);
              Assert.assertEquals(runEvent.getStatus(), AssertionRunStatus.COMPLETE);
              Assert.assertEquals(runEvent.getResult().getNativeResults(), nativeResults);

              AssertionResult result = runEvent.getResult();
              Assert.assertEquals(result.getType(), AssertionResultType.ERROR);
              Assert.assertEquals(result.getExternalUrl(), externalUrlOfAssertion);
              Assert.assertEquals(
                  result.getError().getType(), AssertionResultErrorType.UNKNOWN_ERROR);
              Assert.assertEquals(result.getError().getProperties(), errorProps);

              return null;
            })
        .when(mockedEntityClient)
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));

    assertionService.addAssertionRunEvent(
        opContext,
        TEST_ASSERTION_URN,
        TEST_DATASET_URN,
        eventtime,
        new AssertionResult()
            .setType(AssertionResultType.ERROR)
            .setExternalUrl(externalUrlOfAssertion)
            .setNativeResults(nativeResults)
            .setError(
                new AssertionResultError()
                    .setType(AssertionResultErrorType.UNKNOWN_ERROR)
                    .setProperties(errorProps)));

    Mockito.verify(mockedEntityClient, Mockito.times(1))
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));
  }

  @Test
  public void testGetAssertionInfo() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    AssertionInfo assertionInfo =
        new AssertionInfo().setType(AssertionType.CUSTOM).setEntityUrn(TEST_DATASET_URN);
    EnvelopedAspectMap aspects = new EnvelopedAspectMap(new HashMap<>());
    aspects.put(
        ASSERTION_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(assertionInfo.data())));
    Mockito.when(
            mockClient.getV2(
                opContext,
                ASSERTION_ENTITY_NAME,
                TEST_ASSERTION_URN,
                ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME),
                false))
        .thenReturn(new EntityResponse().setUrn(TEST_ASSERTION_URN).setAspects(aspects));

    final AssertionService service = new AssertionService(mockClient, mock(GraphClient.class));

    Assert.assertEquals(service.getAssertionInfo(opContext, TEST_ASSERTION_URN), assertionInfo);
  }

  @Test
  public void testGetAssertionInfoReturnsNullWhenMissing() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.getV2(
                opContext,
                ASSERTION_ENTITY_NAME,
                TEST_ASSERTION_URN,
                ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME),
                false))
        .thenReturn(null);

    final AssertionService service = new AssertionService(mockClient, mock(GraphClient.class));

    Assert.assertNull(service.getAssertionInfo(opContext, TEST_ASSERTION_URN));
  }

  @Test
  public void testGetEntityUrnForAssertion() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    AssertionInfo assertionInfo =
        new AssertionInfo().setType(AssertionType.CUSTOM).setEntityUrn(TEST_DATASET_URN);
    EnvelopedAspectMap aspects = new EnvelopedAspectMap(new HashMap<>());
    aspects.put(
        ASSERTION_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(assertionInfo.data())));
    Mockito.when(
            mockClient.getV2(
                opContext,
                ASSERTION_ENTITY_NAME,
                TEST_ASSERTION_URN,
                ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME),
                false))
        .thenReturn(new EntityResponse().setUrn(TEST_ASSERTION_URN).setAspects(aspects));

    final AssertionService service = new AssertionService(mockClient, mock(GraphClient.class));

    Assert.assertEquals(
        service.getEntityUrnForAssertion(opContext, TEST_ASSERTION_URN), TEST_DATASET_URN);
  }

  @Test
  public void testGetEntityUrnForAssertionFallsBackToTypeSpecificEntity() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.CUSTOM)
            .setCustomAssertion(
                new CustomAssertionInfo().setEntity(TEST_DATASET_URN).setField(TEST_FIELD_URN));
    EnvelopedAspectMap aspects = new EnvelopedAspectMap(new HashMap<>());
    aspects.put(
        ASSERTION_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(assertionInfo.data())));
    Mockito.when(
            mockClient.getV2(
                opContext,
                ASSERTION_ENTITY_NAME,
                TEST_ASSERTION_URN,
                ImmutableSet.of(ASSERTION_INFO_ASPECT_NAME),
                false))
        .thenReturn(new EntityResponse().setUrn(TEST_ASSERTION_URN).setAspects(aspects));

    final AssertionService service = new AssertionService(mockClient, mock(GraphClient.class));

    Assert.assertEquals(
        service.getEntityUrnForAssertion(opContext, TEST_ASSERTION_URN), TEST_DATASET_URN);
  }

  @Test
  public void testGetMonitorUrnForAssertion() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);
    Mockito.when(
            mockGraphClient.getRelatedEntities(
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.eq(ImmutableSet.of("Evaluates")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(1),
                Mockito.anyString()))
        .thenReturn(
            new EntityRelationships()
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(new EntityRelationship().setEntity(TEST_MONITOR_URN)))));

    AssertionService service = new AssertionService(mockClient, mockGraphClient);

    Assert.assertEquals(
        service.getMonitorUrnForAssertion(opContext, TEST_ASSERTION_URN), TEST_MONITOR_URN);
  }

  @Test
  public void testGetMonitorUrnForAssertionReturnsNullWithoutRelationships() throws Exception {
    GraphClient mockGraphClient = mock(GraphClient.class);
    Mockito.when(
            mockGraphClient.getRelatedEntities(
                anyString(),
                anySet(),
                any(RelationshipDirection.class),
                anyInt(),
                anyInt(),
                anyString()))
        .thenReturn(new EntityRelationships().setTotal(0));

    AssertionService service =
        new AssertionService(mock(SystemEntityClient.class), mockGraphClient);

    Assert.assertNull(service.getMonitorUrnForAssertion(opContext, TEST_ASSERTION_URN));
  }

  @Test
  public void testGetAssertionRunSummary() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    AssertionRunSummary summary =
        new AssertionRunSummary()
            .setLastPassedAtMillis(100L)
            .setAssertionStatus(com.linkedin.assertion.AssertionStatus.PASSING);
    EnvelopedAspectMap aspects = new EnvelopedAspectMap(new HashMap<>());
    aspects.put(
        ASSERTION_RUN_SUMMARY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(summary.data())));
    Mockito.when(
            mockClient.getV2(
                opContext,
                ASSERTION_ENTITY_NAME,
                TEST_ASSERTION_URN,
                ImmutableSet.of(ASSERTION_RUN_SUMMARY_ASPECT_NAME),
                false))
        .thenReturn(new EntityResponse().setUrn(TEST_ASSERTION_URN).setAspects(aspects));

    AssertionService service = new AssertionService(mockClient, mock(GraphClient.class));

    Assert.assertEquals(service.getAssertionRunSummary(opContext, TEST_ASSERTION_URN), summary);
  }

  @Test
  public void testGetAssertionRunSummaryReturnsNullWhenMissing() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.getV2(
                opContext,
                ASSERTION_ENTITY_NAME,
                TEST_ASSERTION_URN,
                ImmutableSet.of(ASSERTION_RUN_SUMMARY_ASPECT_NAME),
                false))
        .thenReturn(null);

    AssertionService service = new AssertionService(mockClient, mock(GraphClient.class));

    Assert.assertNull(service.getAssertionRunSummary(opContext, TEST_ASSERTION_URN));
  }

  @Test
  public void testPatchAssertionRunSummary() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    AssertionRunSummaryPatchBuilder patchBuilder =
        new AssertionRunSummaryPatchBuilder().setLastPassedAt(100L);
    patchBuilder.urn(TEST_ASSERTION_URN);
    AssertionService service = new AssertionService(mockClient, mock(GraphClient.class));

    service.patchAssertionRunSummary(opContext, patchBuilder);

    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient).ingestProposal(eq(opContext), proposalCaptor.capture(), eq(false));
    Assert.assertEquals(proposalCaptor.getValue().getEntityUrn(), TEST_ASSERTION_URN);
    Assert.assertEquals(
        proposalCaptor.getValue().getAspectName(), ASSERTION_RUN_SUMMARY_ASPECT_NAME);
  }
}
