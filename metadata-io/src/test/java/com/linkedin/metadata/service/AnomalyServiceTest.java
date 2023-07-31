package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.anomaly.AnomalyStatusProperties;
import com.linkedin.common.AnomalySummaryDetailsArray;
import com.linkedin.common.AuditStamp;
import com.linkedin.anomaly.AnomalyInfo;
import com.linkedin.common.AnomaliesSummary;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.AnomalyState;
import com.linkedin.anomaly.AnomalyStatus;
import com.linkedin.anomaly.AnomalyType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Collections;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


public class AnomalyServiceTest {

  private static final Urn TEST_ANOMALY_URN = UrnUtils.getUrn("urn:li:anomaly:test");
  private static final Urn TEST_NON_EXISTENT_ANOMALY_URN = UrnUtils.getUrn("urn:li:anomaly:test-non-existent");
  private static final Urn TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_NON_EXISTENT_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,non-existent,PROD)");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn(SYSTEM_ACTOR);

  @Test
  private void testGetAnomalyInfo() throws Exception {
    final EntityClient mockClient = createMockEntityClient();
    final AnomalyService service = new AnomalyService(
        mockClient,
        Mockito.mock(Authentication.class));

    // Case 1: Info exists
    AnomalyInfo info = service.getAnomalyInfo(TEST_ANOMALY_URN);
    Assert.assertEquals(info, mockAnomalyInfo());
    Mockito.verify(mockClient, Mockito.times(1)).getV2(
        Mockito.eq(Constants.ANOMALY_ENTITY_NAME),
        Mockito.eq(TEST_ANOMALY_URN),
        Mockito.eq(ImmutableSet.of(Constants.ANOMALY_INFO_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Case 2: Info does not exist
    info = service.getAnomalyInfo(TEST_NON_EXISTENT_ANOMALY_URN);
    Assert.assertNull(info);
    Mockito.verify(mockClient, Mockito.times(1)).getV2(
        Mockito.eq(Constants.ANOMALY_ENTITY_NAME),
        Mockito.eq(TEST_NON_EXISTENT_ANOMALY_URN),
        Mockito.eq(ImmutableSet.of(Constants.ANOMALY_INFO_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  private void testGetAnomaliesSummary() throws Exception {
    final EntityClient mockClient = createMockEntityClient();
    final AnomalyService service = new AnomalyService(
        mockClient,
        Mockito.mock(Authentication.class));

    // Case 1: Summary exists
    AnomaliesSummary summary = service.getAnomaliesSummary(TEST_DATASET_URN);
    Assert.assertEquals(summary, mockAnomalySummary());
    Mockito.verify(mockClient, Mockito.times(1)).getV2(
        Mockito.eq(DATASET_ENTITY_NAME),
        Mockito.eq(TEST_DATASET_URN),
        Mockito.eq(ImmutableSet.of(ANOMALIES_SUMMARY_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );

    // Case 2: Summary does not exist
    summary = service.getAnomaliesSummary(TEST_NON_EXISTENT_DATASET_URN);
    Assert.assertNull(summary);
    Mockito.verify(mockClient, Mockito.times(1)).getV2(
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(TEST_DATASET_URN),
        Mockito.eq(ImmutableSet.of(Constants.ANOMALIES_SUMMARY_ASPECT_NAME)),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  private void testUpdateAnomaliesSummary() throws Exception {
    final EntityClient mockClient = createMockEntityClient();
    final AnomalyService service = new AnomalyService(
        mockClient,
        Mockito.mock(Authentication.class));
    service.updateAnomaliesSummary(TEST_DATASET_URN, mockAnomalySummary());
    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(mockAnomalySummaryMcp()),
        Mockito.any(Authentication.class),
        Mockito.eq(false)
    );
  }

  @Test
  private void testRaiseAnomalyRequiredFields() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final AnomalyService service = new AnomalyService(
        mockClient,
        Mockito.mock(Authentication.class));
    service.raiseAnomaly(
        AnomalyType.FRESHNESS,
        null,
        null,
        TEST_DATASET_URN,
        new AnomalySource().setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE),
        TEST_USER_URN
    );

    final AnomalyInfo expectedInfo = new AnomalyInfo()
        .setType(AnomalyType.FRESHNESS)
        .setEntity(TEST_DATASET_URN)
        .setStatus(new AnomalyStatus()
            .setState(AnomalyState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
        )
        .setSource(new AnomalySource().setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE))
        .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));

    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.argThat(new AnomalyInfoArgumentMatcher(
            AspectUtils.buildMetadataChangeProposal(
                TEST_ANOMALY_URN,
                ANOMALY_INFO_ASPECT_NAME,
                expectedInfo
            )
        )),
        Mockito.any(Authentication.class),
        Mockito.eq(false)
    );
  }

  @Test
  private void testRaiseAnomalyAllFields() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final AnomalyService service = new AnomalyService(
        mockClient,
        Mockito.mock(Authentication.class));
    service.raiseAnomaly(
        AnomalyType.FRESHNESS,
        2,

        "description",
        TEST_DATASET_URN,
        new AnomalySource().setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE),
        TEST_USER_URN
    );

    final AnomalyInfo expectedInfo = new AnomalyInfo()
        .setType(AnomalyType.FRESHNESS)
        .setSeverity(2)
        .setDescription("description")
        .setEntity(TEST_DATASET_URN)
        .setStatus(new AnomalyStatus()
            .setState(AnomalyState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
        )
        .setSource(new AnomalySource().setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE))
        .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));

    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.argThat(new AnomalyInfoArgumentMatcher(
            AspectUtils.buildMetadataChangeProposal(
                TEST_ANOMALY_URN,
                ANOMALY_INFO_ASPECT_NAME,
                expectedInfo
            )
        )),
        Mockito.any(Authentication.class),
        Mockito.eq(false)
    );
  }

  @Test
  private void testUpdateAnomalyStatus() throws Exception {
    final EntityClient mockClient = createMockEntityClient();
    final AnomalyService service = new AnomalyService(
        mockClient,
        Mockito.mock(Authentication.class));
    service.updateAnomalyStatus(
        TEST_ANOMALY_URN,
        AnomalyState.RESOLVED,
        new AnomalyStatusProperties().setAssertionRunEventTime(2L),
        TEST_USER_URN);

    AnomalyInfo expectedInfo = new AnomalyInfo(mockAnomalyInfo().data());
    expectedInfo.setSource(new AnomalySource().setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE));
    expectedInfo.setStatus(new AnomalyStatus()
        .setState(AnomalyState.RESOLVED)
        .setLastUpdated(new AuditStamp().setActor(TEST_USER_URN).setTime(0L))
        .setProperties(new AnomalyStatusProperties().setAssertionRunEventTime(2L))
    );

    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.argThat(new AnomalyInfoArgumentMatcher(AspectUtils.buildMetadataChangeProposal(
            TEST_ANOMALY_URN,
            ANOMALY_INFO_ASPECT_NAME,
            expectedInfo
        ))),
        Mockito.any(Authentication.class),
        Mockito.eq(false)
    );
  }

  @Test
  private void testDeleteAnomaly() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final AnomalyService service = new AnomalyService(
        mockClient,
        Mockito.mock(Authentication.class));
    service.deleteAnomaly(TEST_ANOMALY_URN);
    Mockito.verify(mockClient, Mockito.times(1)).deleteEntity(
        Mockito.eq(TEST_ANOMALY_URN),
        Mockito.any(Authentication.class)
    );
    Mockito.verify(mockClient, Mockito.times(1)).deleteEntityReferences(
        Mockito.eq(TEST_ANOMALY_URN),
        Mockito.any(Authentication.class)
    );
  }

  private static EntityClient createMockEntityClient() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Init for anomaly info
    Mockito.when(mockClient.getV2(
        Mockito.eq(Constants.ANOMALY_ENTITY_NAME),
        Mockito.eq(TEST_ANOMALY_URN),
        Mockito.eq(ImmutableSet.of(Constants.ANOMALY_INFO_ASPECT_NAME)),
        Mockito.any(Authentication.class))
    ).thenReturn(
        new EntityResponse()
            .setUrn(TEST_ANOMALY_URN)
            .setEntityName(ANOMALY_ENTITY_NAME)
            .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                ANOMALY_INFO_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(mockAnomalyInfo().data()))
            ))));
    Mockito.when(mockClient.getV2(
        Mockito.eq(Constants.ANOMALY_ENTITY_NAME),
        Mockito.eq(TEST_NON_EXISTENT_ANOMALY_URN),
        Mockito.eq(ImmutableSet.of(Constants.ANOMALY_INFO_ASPECT_NAME)),
        Mockito.any(Authentication.class))
    ).thenReturn(
        new EntityResponse()
            .setUrn(TEST_NON_EXISTENT_ANOMALY_URN)
            .setEntityName(ANOMALY_ENTITY_NAME)
            .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    // Init for anomalies summary
    Mockito.when(mockClient.getV2(
        Mockito.eq(DATASET_ENTITY_NAME),
        Mockito.eq(TEST_DATASET_URN),
        Mockito.eq(ImmutableSet.of(ANOMALIES_SUMMARY_ASPECT_NAME)),
        Mockito.any(Authentication.class))
    ).thenReturn(
        new EntityResponse()
            .setUrn(TEST_DATASET_URN)
            .setEntityName(DATASET_ENTITY_NAME)
            .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                ANOMALIES_SUMMARY_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(mockAnomalySummary().data()))
            ))));
    Mockito.when(mockClient.getV2(
        Mockito.eq(DATASET_ENTITY_NAME),
        Mockito.eq(TEST_NON_EXISTENT_DATASET_URN),
        Mockito.eq(ImmutableSet.of(ANOMALIES_SUMMARY_ASPECT_NAME)),
        Mockito.any(Authentication.class))
    ).thenReturn(
        new EntityResponse()
            .setUrn(TEST_NON_EXISTENT_DATASET_URN)
            .setEntityName(DATASET_ENTITY_NAME)
            .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    // Init for update summary
    Mockito.when(mockClient.ingestProposal(
        Mockito.eq(mockAnomalySummaryMcp()),
        Mockito.any(Authentication.class),
        Mockito.eq(false))).thenReturn(TEST_DATASET_URN.toString());

    return mockClient;
  }

  private static AnomalyInfo mockAnomalyInfo() throws Exception {
    return new AnomalyInfo()
        .setType(AnomalyType.FRESHNESS)
        .setEntity(TEST_DATASET_URN)
        .setStatus(new AnomalyStatus()
            .setState(AnomalyState.ACTIVE)
            .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
        )
        .setSource(new AnomalySource().setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE))
        .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));
  }

  private static AnomaliesSummary mockAnomalySummary() throws Exception {
    final AnomaliesSummary summary = new AnomaliesSummary();
    summary.setResolvedAnomalyDetails(new AnomalySummaryDetailsArray());
    return summary;
  }

  private static MetadataChangeProposal mockAnomalySummaryMcp() throws Exception {

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_DATASET_URN);
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setAspectName(ANOMALIES_SUMMARY_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(mockAnomalySummary()));

    return mcp;
  }
}