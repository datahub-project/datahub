package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.UPSTREAM_LINEAGE_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.EbeanSystemAspect;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityServiceImplTest {
  private final AuditStamp TEST_AUDIT_STAMP = AspectGenerationUtils.createAuditStamp();
  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final EntityRegistry testEntityRegistry = opContext.getEntityRegistry();

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:corpuser:EntityServiceImplTest");

  private EventProducer mockEventProducer;
  private Status oldAspect;
  private Status newAspect;
  private EntityServiceImpl entityService;
  private MetadataChangeProposal testMCP;

  @BeforeMethod
  public void setup() throws Exception {
    mockEventProducer = mock(EventProducer.class);

    // Initialize common test objects
    entityService =
        new EntityServiceImpl(
            mock(AspectDao.class), mockEventProducer, false, mock(PreProcessHooks.class), 0, true);

    // Create test aspects
    oldAspect = new Status().setRemoved(false);
    newAspect = new Status().setRemoved(true);

    testMCP =
        new MetadataChangeProposal()
            .setEntityUrn(TEST_URN)
            .setEntityType(TEST_URN.getEntityType())
            .setAspectName(STATUS_ASPECT_NAME)
            .setAspect(GenericRecordUtils.serializeAspect(newAspect));

    when(mockEventProducer.produceMetadataChangeLog(
            any(OperationContext.class), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
  }

  @Test
  public void testApplyUpsertNoOp() throws Exception {
    // Set up initial system metadata
    SystemMetadata initialMetadata = new SystemMetadata();
    initialMetadata.setRunId("run-1");
    initialMetadata.setVersion("1");
    initialMetadata.setLastObserved(1000L);

    // Create initial aspect that will be stored in database
    CorpUserInfo originalInfo = AspectGenerationUtils.createCorpUserInfo("test@test.com");
    EbeanAspectV2 databaseAspectV2 =
        new EbeanAspectV2(
            "urn:li:corpuser:test", // urn
            "corpUserInfo", // aspect name
            0L, // version
            RecordUtils.toJsonString(originalInfo), // metadata
            new Timestamp(TEST_AUDIT_STAMP.getTime()), // createdOn
            TEST_AUDIT_STAMP.getActor().toString(), // createdBy
            null, // createdFor
            RecordUtils.toJsonString(initialMetadata) // systemMetadata
            );

    // Create the latest aspect that includes database reference
    SystemAspect latestAspect =
        EbeanSystemAspect.builder().forUpdate(databaseAspectV2, testEntityRegistry);

    // Create change with same content but updated metadata
    SystemMetadata newMetadata = new SystemMetadata();
    newMetadata.setRunId("run-2");
    newMetadata.setLastObserved(2000L);

    ChangeMCP changeMCP =
        ChangeItemImpl.builder()
            .urn(UrnUtils.getUrn("urn:li:corpuser:test"))
            .aspectName("corpUserInfo")
            .recordTemplate(originalInfo.copy()) // Same content, different instance
            .systemMetadata(newMetadata)
            .auditStamp(TEST_AUDIT_STAMP)
            .nextAspectVersion(2)
            .build(opContext.getAspectRetriever());

    // Apply upsert
    SystemAspect result = EntityServiceImpl.applyUpsert(changeMCP, latestAspect);

    // Verify metadata was updated but content remained same
    assertEquals(changeMCP.getNextAspectVersion(), 1, "1 which is then incremented back to 2");
    assertEquals(result.getSystemMetadata().getVersion(), "1");
    assertEquals(initialMetadata.getVersion(), "1");
    assertEquals(latestAspect.getSystemMetadataVersion().get(), 1L);
    assertNull(newMetadata.getVersion());

    assertEquals(result.getSystemMetadata().getRunId(), "run-2");
    assertEquals(result.getSystemMetadata().getLastRunId(), "run-1");
    assertEquals(result.getSystemMetadata().getLastObserved(), 2000L);
    assertTrue(DataTemplateUtil.areEqual(result.getRecordTemplate(), originalInfo));
  }

  @Test
  public void testApplyUpsertUpdate() throws Exception {
    // Set up initial system metadata and aspect
    SystemMetadata initialMetadata = new SystemMetadata();
    initialMetadata.setRunId("run-1");
    initialMetadata.setVersion("1");

    // Create initial aspect that will be stored in database
    CorpUserInfo originalInfo = AspectGenerationUtils.createCorpUserInfo("test@test.com");
    EbeanAspectV2 databaseAspectV2 =
        new EbeanAspectV2(
            "urn:li:corpuser:test", // urn
            "corpUserInfo", // aspect name
            0L, // version
            RecordUtils.toJsonString(originalInfo), // metadata
            new Timestamp(TEST_AUDIT_STAMP.getTime()), // createdOn
            TEST_AUDIT_STAMP.getActor().toString(), // createdBy
            null, // createdFor
            RecordUtils.toJsonString(initialMetadata) // systemMetadata
            );

    // Create the latest aspect that includes database reference
    SystemAspect latestAspect =
        EbeanSystemAspect.builder().forUpdate(databaseAspectV2, testEntityRegistry);

    // Create change with different content
    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("new@test.com");
    SystemMetadata newMetadata = new SystemMetadata();
    newMetadata.setRunId("run-2");

    ChangeMCP changeMCP =
        ChangeItemImpl.builder()
            .urn(UrnUtils.getUrn("urn:li:corpuser:test"))
            .aspectName("corpUserInfo")
            .recordTemplate(newInfo)
            .systemMetadata(newMetadata)
            .auditStamp(TEST_AUDIT_STAMP)
            .nextAspectVersion(Long.valueOf(initialMetadata.getVersion()) + 1)
            .build(opContext.getAspectRetriever());

    // Apply upsert
    SystemAspect result = EntityServiceImpl.applyUpsert(changeMCP, latestAspect);

    // Verify both metadata and content were updated
    assertEquals(changeMCP.getNextAspectVersion(), 2, "Expected acceptance of proposed version");
    assertEquals(result.getSystemMetadata().getVersion(), "2");
    assertEquals(initialMetadata.getVersion(), "1");
    assertEquals(latestAspect.getSystemMetadataVersion().get(), 2L);
    assertNull(newMetadata.getVersion());

    assertEquals(result.getSystemMetadata().getRunId(), "run-2");
    assertEquals(result.getSystemMetadata().getLastRunId(), "run-1");
    assertTrue(DataTemplateUtil.areEqual(result.getRecordTemplate(), newInfo));
    assertEquals(result.getSystemMetadataVersion(), Optional.of(2L));

    // Verify previous aspect was set in changeMCP
    assertNotNull(changeMCP.getPreviousSystemAspect());
    assertTrue(
        DataTemplateUtil.areEqual(
            changeMCP.getPreviousSystemAspect().getRecordTemplate(), originalInfo));
  }

  @Test
  public void testApplyUpsertInsert() throws Exception {
    // Create new aspect via ChangeMCP
    SystemMetadata newMetadata = new SystemMetadata();
    newMetadata.setRunId("run-1");

    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("test@test.com");

    // For insert case, there is no existing aspect in the database
    // so latestAspect should be null

    ChangeMCP changeMCP =
        ChangeItemImpl.builder()
            .urn(UrnUtils.getUrn("urn:li:corpuser:test"))
            .aspectName("corpUserInfo")
            .recordTemplate(newInfo)
            .systemMetadata(newMetadata)
            .auditStamp(TEST_AUDIT_STAMP)
            .nextAspectVersion(1)
            .build(opContext.getAspectRetriever());

    // No existing aspect
    SystemAspect result = EntityServiceImpl.applyUpsert(changeMCP, null);

    // Verify new aspect was created correctly
    assertNotNull(result);

    assertEquals(changeMCP.getNextAspectVersion(), 1, "Expected 1 since its initial");
    assertEquals(result.getSystemMetadata().getVersion(), "1");
    assertNull(newMetadata.getVersion());

    assertEquals(result.getSystemMetadata().getRunId(), "run-1");
    assertEquals(result.getSystemMetadata().getLastRunId(), "no-run-id-provided");
    assertTrue(DataTemplateUtil.areEqual(result.getRecordTemplate(), newInfo));

    // Additional verifications
    assertNotNull(result.getCreatedOn());
    assertEquals(result.getCreatedBy(), TEST_AUDIT_STAMP.getActor().toString());
  }

  @Test
  public void testApplyUpsertMultiInsert() throws Exception {
    // Create new aspect via ChangeMCP
    SystemMetadata newMetadata = new SystemMetadata();
    newMetadata.setRunId("run-1");

    CorpUserInfo newInfo1 = AspectGenerationUtils.createCorpUserInfo("test1@test.com");
    CorpUserInfo newInfo2 = AspectGenerationUtils.createCorpUserInfo("test2@test.com");

    // For insert case, there is no existing aspect in the database
    // so latestAspect should be null
    ChangeMCP changeMCP1 =
        ChangeItemImpl.builder()
            .urn(UrnUtils.getUrn("urn:li:corpuser:test"))
            .aspectName("corpUserInfo")
            .recordTemplate(newInfo1)
            .systemMetadata(newMetadata)
            .auditStamp(TEST_AUDIT_STAMP)
            .nextAspectVersion(1)
            .build(opContext.getAspectRetriever());

    // No existing aspect
    SystemAspect result1 = EntityServiceImpl.applyUpsert(changeMCP1, null);

    // Change 1
    assertNotNull(result1);
    assertEquals(changeMCP1.getNextAspectVersion(), 1, "Expected 0 since its initial");
    assertEquals(result1.getSystemMetadata().getVersion(), "1");
    assertNull(newMetadata.getVersion());
    assertEquals(result1.getSystemMetadata().getRunId(), "run-1");
    assertEquals(result1.getSystemMetadata().getLastRunId(), "no-run-id-provided");
    assertTrue(DataTemplateUtil.areEqual(result1.getRecordTemplate(), newInfo1));
    assertNotNull(result1.getCreatedOn());
    assertEquals(result1.getCreatedBy(), TEST_AUDIT_STAMP.getActor().toString());
    assertTrue(result1.getDatabaseAspect().isEmpty());

    ChangeMCP changeMCP2 =
        ChangeItemImpl.builder()
            .urn(UrnUtils.getUrn("urn:li:corpuser:test"))
            .aspectName("corpUserInfo")
            .recordTemplate(newInfo2)
            .systemMetadata(newMetadata)
            .auditStamp(TEST_AUDIT_STAMP)
            .nextAspectVersion(changeMCP1.getNextAspectVersion() + 1)
            .build(opContext.getAspectRetriever());

    SystemAspect result2 =
        EntityServiceImpl.applyUpsert(changeMCP2, result1); // pass previous as latest

    // Change 2
    assertNotNull(result2);
    assertEquals(changeMCP2.getNextAspectVersion(), 2, "Expected 2");
    assertEquals(result2.getSystemMetadata().getVersion(), "2");
    assertNull(newMetadata.getVersion());
    assertEquals(result2.getSystemMetadata().getRunId(), "run-1");
    assertEquals(result2.getSystemMetadata().getLastRunId(), "no-run-id-provided");
    assertTrue(DataTemplateUtil.areEqual(result2.getRecordTemplate(), newInfo2));
    assertNotNull(result2.getCreatedOn());
    assertEquals(result2.getCreatedBy(), TEST_AUDIT_STAMP.getActor().toString());
  }

  @Test
  public void testApplyUpsertNullVersionException() {
    // Set up initial system metadata with null version
    SystemMetadata initialMetadata = new SystemMetadata();
    initialMetadata.setRunId("run-1");

    // Create initial aspect that will be stored in database
    CorpUserInfo originalInfo = AspectGenerationUtils.createCorpUserInfo("test@test.com");
    EbeanAspectV2 databaseAspectV2 =
        new EbeanAspectV2(
            "urn:li:corpuser:test",
            "corpUserInfo",
            0L,
            RecordUtils.toJsonString(originalInfo),
            new Timestamp(TEST_AUDIT_STAMP.getTime()),
            TEST_AUDIT_STAMP.getActor().toString(),
            null,
            RecordUtils.toJsonString(initialMetadata));

    // Create the latest aspect that includes database reference
    SystemAspect latestAspect =
        EbeanSystemAspect.builder().forUpdate(databaseAspectV2, testEntityRegistry);

    // Create change with different content to trigger update path
    SystemMetadata newMetadata = new SystemMetadata();
    newMetadata.setRunId("run-2");

    ChangeMCP changeMCP =
        ChangeItemImpl.builder()
            .urn(UrnUtils.getUrn("urn:li:corpuser:test"))
            .aspectName("corpUserInfo")
            .recordTemplate(originalInfo)
            .systemMetadata(newMetadata)
            .auditStamp(TEST_AUDIT_STAMP)
            .nextAspectVersion(1L)
            .build(opContext.getAspectRetriever());

    SystemAspect upsert = EntityServiceImpl.applyUpsert(changeMCP, latestAspect);
    assertEquals(upsert.getSystemMetadataVersion(), Optional.of(1L));
    assertEquals(upsert.getVersion(), 0);
    assertEquals(changeMCP.getNextAspectVersion(), 1);
    assertEquals(changeMCP.getSystemMetadata().getVersion(), "1");
  }

  @Test
  public void testNoMCLWhenSystemMetadataIsNoOp() {
    // Arrange
    SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();
    SystemMetadataUtils.setNoOp(systemMetadata, true); // Makes it a no-op

    // Act
    Optional<Pair<Future<?>, Boolean>> result =
        entityService.conditionallyProduceMCLAsync(
            opContext,
            oldAspect,
            null, // oldSystemMetadata
            newAspect,
            systemMetadata,
            testMCP,
            TEST_URN,
            TEST_AUDIT_STAMP,
            opContext
                .getEntityRegistry()
                .getEntitySpec(TEST_URN.getEntityType())
                .getAspectSpec(STATUS_ASPECT_NAME));

    // Assert
    assertFalse(result.isPresent(), "Should not produce MCL when system metadata is no-op");
    verify(mockEventProducer, never()).produceMetadataChangeLog(any(), any(), any());
  }

  @Test
  public void testNoMCLWhenAspectsAreEqual() {
    // Arrange
    RecordTemplate sameAspect = newAspect;

    // Act
    Optional<Pair<Future<?>, Boolean>> result =
        entityService.conditionallyProduceMCLAsync(
            opContext,
            sameAspect,
            null, // oldSystemMetadata
            sameAspect,
            SystemMetadataUtils.createDefaultSystemMetadata(),
            testMCP,
            TEST_URN,
            TEST_AUDIT_STAMP,
            opContext
                .getEntityRegistry()
                .getEntitySpec(TEST_URN.getEntityType())
                .getAspectSpec(STATUS_ASPECT_NAME));

    // Assert
    assertFalse(result.isPresent(), "Should not produce MCL when aspects are equal");
    verify(mockEventProducer, never()).produceMetadataChangeLog(any(), any(), any());
  }

  @Test
  public void testProducesMCLWhenChangesExist() {
    // Arrange
    SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();
    SystemMetadataUtils.setNoOp(systemMetadata, false); // Makes it not a no-op

    // Act
    Optional<Pair<Future<?>, Boolean>> result =
        entityService.conditionallyProduceMCLAsync(
            opContext,
            oldAspect,
            null, // oldSystemMetadata
            newAspect,
            systemMetadata,
            testMCP,
            TEST_URN,
            TEST_AUDIT_STAMP,
            opContext
                .getEntityRegistry()
                .getEntitySpec(TEST_URN.getEntityType())
                .getAspectSpec(STATUS_ASPECT_NAME));

    // Assert
    assertTrue(result.isPresent(), "Should produce MCL when changes exist");
    verify(mockEventProducer, times(1))
        .produceMetadataChangeLog(any(OperationContext.class), any(), any(), any());
  }

  @Test
  public void testAlwaysEmitChangeLogFlag() {
    // Arrange
    entityService =
        new EntityServiceImpl(
            mock(AspectDao.class),
            mockEventProducer,
            true, // alwaysEmitChangeLog set to true
            mock(PreProcessHooks.class),
            0,
            true);

    RecordTemplate sameAspect = newAspect;

    // Act
    Optional<Pair<Future<?>, Boolean>> result =
        entityService.conditionallyProduceMCLAsync(
            opContext,
            sameAspect,
            null, // oldSystemMetadata
            sameAspect, // Same aspect
            SystemMetadataUtils.createDefaultSystemMetadata(),
            testMCP,
            TEST_URN,
            TEST_AUDIT_STAMP,
            opContext
                .getEntityRegistry()
                .getEntitySpec(TEST_URN.getEntityType())
                .getAspectSpec(STATUS_ASPECT_NAME));

    // Assert
    assertTrue(
        result.isPresent(),
        "Should produce MCL when alwaysEmitChangeLog is true, regardless of no-op status");
    verify(mockEventProducer, times(1))
        .produceMetadataChangeLog(any(OperationContext.class), any(), any(), any());
  }

  @Test
  public void testAspectWithLineageRelationship() {
    // Arrange
    Urn datasetUrn =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:test,testAspectWithLineageRelationship,PROD)");
    UpstreamLineage sameLineageAspect = new UpstreamLineage();
    MetadataChangeProposal datasetMCP =
        new MetadataChangeProposal()
            .setEntityUrn(datasetUrn)
            .setEntityType(datasetUrn.getEntityType())
            .setAspectName(UPSTREAM_LINEAGE_ASPECT_NAME)
            .setAspect(GenericRecordUtils.serializeAspect(sameLineageAspect));

    // Act
    Optional<Pair<Future<?>, Boolean>> result =
        entityService.conditionallyProduceMCLAsync(
            opContext,
            sameLineageAspect,
            null, // oldSystemMetadata
            sameLineageAspect, // Same aspect
            SystemMetadataUtils.createDefaultSystemMetadata(),
            datasetMCP,
            datasetUrn,
            TEST_AUDIT_STAMP,
            opContext
                .getEntityRegistry()
                .getEntitySpec(datasetUrn.getEntityType())
                .getAspectSpec(UPSTREAM_LINEAGE_ASPECT_NAME));

    // Assert
    assertTrue(
        result.isPresent(),
        "Should produce MCL when aspect has lineage relationship, regardless of no-op status");
    verify(mockEventProducer, times(1))
        .produceMetadataChangeLog(any(OperationContext.class), any(), any(), any());
  }
}
