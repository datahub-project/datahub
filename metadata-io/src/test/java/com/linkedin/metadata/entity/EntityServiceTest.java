package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.VersionBasedRetention;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.annotation.Nonnull;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * A class to test {@link EntityServiceImpl}
 *
 * <p>This class is generic to allow same integration tests to be reused to test all supported
 * storage backends. If you're adding another storage backend - you should create a new test class
 * that extends this one providing hard implementations of {@link AspectDao} and {@link
 * RetentionService} and implements {@code @BeforeMethod} etc to set up and tear down state.
 *
 * <p>If you realise that a feature you want to test, sadly, has divergent behaviours between
 * different storage implementations, that you can't rectify - you should make the test method
 * abstract and implement it in all implementations of this class.
 *
 * @param <T_AD> {@link AspectDao} implementation.
 * @param <T_RS> {@link RetentionService} implementation.
 */
public abstract class EntityServiceTest<T_AD extends AspectDao, T_RS extends RetentionService> {

  protected EntityServiceImpl _entityServiceImpl;
  protected T_AD _aspectDao;
  protected T_RS _retentionService;

  protected static final AuditStamp TEST_AUDIT_STAMP = AspectGenerationUtils.createAuditStamp();
  protected OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  protected final EntityRegistry _testEntityRegistry = opContext.getEntityRegistry();
  protected OperationContext userContext =
      TestOperationContexts.userContextNoSearchAuthorization(_testEntityRegistry);
  protected EventProducer _mockProducer;
  protected UpdateIndicesService _mockUpdateIndicesService;

  protected final AspectSpec structuredPropertiesDefinitionAspect =
      _testEntityRegistry.getAspectSpecs().get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);

  protected EntityServiceTest() throws EntityRegistryException {}

  // This test had to be split out because Cassandra relational databases have different result
  // ordering restrictions
  @Test
  public abstract void testIngestListLatestAspects() throws Exception;

  // This test had to be split out because Cassandra relational databases have different result
  // ordering restrictions
  @Test
  public abstract void testIngestListUrns() throws Exception;

  // This test had to be split out because Cassandra doesn't support nested transactions
  @Test
  public abstract void testNestedTransactions() throws Exception;

  @Test
  public void testStructuredPropertiesAspectExists() {
    assertNotNull(structuredPropertiesDefinitionAspect);
  }

  @Test
  public void testIngestGetEntity() throws Exception {
    // Test Writing a CorpUser Entity
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test");
    com.linkedin.entity.Entity writeEntity = createCorpUserEntity(entityUrn, "tester@test.com");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata();

    // 1. Ingest Entity
    _entityServiceImpl.ingestEntity(opContext, writeEntity, TEST_AUDIT_STAMP, metadata1);

    // 2. Retrieve Entity
    com.linkedin.entity.Entity readEntity =
        _entityServiceImpl.getEntity(opContext, entityUrn, Collections.emptySet(), true);

    // 3. Compare Entity Objects
    assertEquals(
        readEntity.getValue().getCorpUserSnapshot().getAspects().size(), 2); // Key + Info aspect.
    assertTrue(
        DataTemplateUtil.areEqual(
            writeEntity.getValue().getCorpUserSnapshot().getAspects().get(0),
            readEntity.getValue().getCorpUserSnapshot().getAspects().get(1)));
    CorpUserKey expectedKey = new CorpUserKey();
    expectedKey.setUsername("test");
    assertTrue(
        DataTemplateUtil.areEqual(
            expectedKey,
            readEntity
                .getValue()
                .getCorpUserSnapshot()
                .getAspects()
                .get(0)
                .getCorpUserKey())); // Key + Info aspect.

    ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());
    MetadataChangeLog mcl = mclCaptor.getValue();
    assertEquals(mcl.getEntityType(), "corpuser");
    assertNull(mcl.getPreviousAspectValue());
    assertNull(mcl.getPreviousSystemMetadata());
    assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testAddKey() throws Exception {
    // Test Writing a CorpUser Key
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test");
    com.linkedin.entity.Entity writeEntity = createCorpUserEntity(entityUrn, "tester@test.com");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata();

    // 1. Ingest Entity
    _entityServiceImpl.ingestEntity(opContext, writeEntity, TEST_AUDIT_STAMP, metadata1);

    // 2. Retrieve Entity
    com.linkedin.entity.Entity readEntity =
        _entityServiceImpl.getEntity(opContext, entityUrn, Collections.emptySet(), true);

    // 3. Compare Entity Objects
    assertEquals(
        readEntity.getValue().getCorpUserSnapshot().getAspects().size(), 2); // Key + Info aspect.
    assertTrue(
        DataTemplateUtil.areEqual(
            writeEntity.getValue().getCorpUserSnapshot().getAspects().get(0),
            readEntity.getValue().getCorpUserSnapshot().getAspects().get(1)));
    CorpUserKey expectedKey = new CorpUserKey();
    expectedKey.setUsername("test");
    assertTrue(
        DataTemplateUtil.areEqual(
            expectedKey,
            readEntity
                .getValue()
                .getCorpUserSnapshot()
                .getAspects()
                .get(0)
                .getCorpUserKey())); // Key + Info aspect.

    ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());
    MetadataChangeLog mcl = mclCaptor.getValue();
    assertEquals(mcl.getEntityType(), "corpuser");
    assertNull(mcl.getPreviousAspectValue());
    assertNull(mcl.getPreviousSystemMetadata());
    assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestGetEntities() throws Exception {
    // Test Writing a CorpUser Entity
    Urn entityUrn1 = UrnUtils.getUrn("urn:li:corpuser:tester1");
    com.linkedin.entity.Entity writeEntity1 = createCorpUserEntity(entityUrn1, "tester@test.com");

    Urn entityUrn2 = UrnUtils.getUrn("urn:li:corpuser:tester2");
    com.linkedin.entity.Entity writeEntity2 = createCorpUserEntity(entityUrn2, "tester2@test.com");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata(1625792689, "run-123");
    SystemMetadata metadata2 = AspectGenerationUtils.createSystemMetadata(1625792690, "run-123");

    // 1. Ingest Entities
    _entityServiceImpl.ingestEntities(
        opContext,
        ImmutableList.of(writeEntity1, writeEntity2),
        TEST_AUDIT_STAMP,
        ImmutableList.of(metadata1, metadata2));

    // 2. Retrieve Entities
    Map<Urn, Entity> readEntities =
        _entityServiceImpl.getEntities(
            opContext, ImmutableSet.of(entityUrn1, entityUrn2), Collections.emptySet(), true);

    // 3. Compare Entity Objects

    // Entity 1
    com.linkedin.entity.Entity readEntity1 = readEntities.get(entityUrn1);
    assertEquals(
        readEntity1.getValue().getCorpUserSnapshot().getAspects().size(), 2); // Key + Info aspect.
    assertTrue(
        DataTemplateUtil.areEqual(
            writeEntity1.getValue().getCorpUserSnapshot().getAspects().get(0),
            readEntity1.getValue().getCorpUserSnapshot().getAspects().get(1)));
    CorpUserKey expectedKey1 = new CorpUserKey();
    expectedKey1.setUsername("tester1");
    assertTrue(
        DataTemplateUtil.areEqual(
            expectedKey1,
            readEntity1
                .getValue()
                .getCorpUserSnapshot()
                .getAspects()
                .get(0)
                .getCorpUserKey())); // Key + Info aspect.

    // Entity 2
    com.linkedin.entity.Entity readEntity2 = readEntities.get(entityUrn2);
    assertEquals(
        readEntity2.getValue().getCorpUserSnapshot().getAspects().size(), 2); // Key + Info aspect.
    Optional<CorpUserAspect> writer2UserInfo =
        writeEntity2.getValue().getCorpUserSnapshot().getAspects().stream()
            .filter(CorpUserAspect::isCorpUserInfo)
            .findAny();
    Optional<CorpUserAspect> reader2UserInfo =
        writeEntity2.getValue().getCorpUserSnapshot().getAspects().stream()
            .filter(CorpUserAspect::isCorpUserInfo)
            .findAny();

    assertTrue(writer2UserInfo.isPresent(), "Writer2 user info exists");
    assertTrue(reader2UserInfo.isPresent(), "Reader2 user info exists");
    assertTrue(
        DataTemplateUtil.areEqual(writer2UserInfo.get(), reader2UserInfo.get()),
        "UserInfo's are the same");
    CorpUserKey expectedKey2 = new CorpUserKey();
    expectedKey2.setUsername("tester2");
    assertTrue(
        DataTemplateUtil.areEqual(
            expectedKey2,
            readEntity2
                .getValue()
                .getCorpUserSnapshot()
                .getAspects()
                .get(0)
                .getCorpUserKey())); // Key + Info aspect.

    ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(Mockito.eq(entityUrn1), Mockito.any(), mclCaptor.capture());
    MetadataChangeLog mcl = mclCaptor.getValue();
    assertEquals(mcl.getEntityType(), "corpuser");
    assertNull(mcl.getPreviousAspectValue());
    assertNull(mcl.getPreviousSystemMetadata());
    assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(Mockito.eq(entityUrn2), Mockito.any(), mclCaptor.capture());
    mcl = mclCaptor.getValue();
    assertEquals(mcl.getEntityType(), "corpuser");
    assertNull(mcl.getPreviousAspectValue());
    assertNull(mcl.getPreviousSystemMetadata());
    assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestGetEntitiesV2() throws Exception {
    // Test Writing a CorpUser Entity
    Urn entityUrn1 = UrnUtils.getUrn("urn:li:corpuser:tester1");
    com.linkedin.entity.Entity writeEntity1 = createCorpUserEntity(entityUrn1, "tester@test.com");

    Urn entityUrn2 = UrnUtils.getUrn("urn:li:corpuser:tester2");
    com.linkedin.entity.Entity writeEntity2 = createCorpUserEntity(entityUrn2, "tester2@test.com");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata(1625792689, "run-123");
    SystemMetadata metadata2 = AspectGenerationUtils.createSystemMetadata(1625792690, "run-123");

    String aspectName = "corpUserInfo";
    String keyName = "corpUserKey";

    // 1. Ingest Entities
    _entityServiceImpl.ingestEntities(
        opContext,
        ImmutableList.of(writeEntity1, writeEntity2),
        TEST_AUDIT_STAMP,
        ImmutableList.of(metadata1, metadata2));

    // 2. Retrieve Entities
    Map<Urn, EntityResponse> readEntities =
        _entityServiceImpl.getEntitiesV2(
            opContext,
            "corpuser",
            ImmutableSet.of(entityUrn1, entityUrn2),
            ImmutableSet.of(aspectName));

    // 3. Compare Entity Objects

    // Entity 1
    EntityResponse readEntityResponse1 = readEntities.get(entityUrn1);
    assertEquals(readEntityResponse1.getAspects().size(), 2); // Key + Info aspect.
    EnvelopedAspect envelopedAspect1 = readEntityResponse1.getAspects().get(aspectName);
    assertEquals(envelopedAspect1.getName(), aspectName);
    assertTrue(
        DataTemplateUtil.areEqual(
            writeEntity1.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserInfo(),
            new CorpUserInfo(envelopedAspect1.getValue().data())));
    CorpUserKey expectedKey1 = new CorpUserKey();
    expectedKey1.setUsername("tester1");
    EnvelopedAspect envelopedKey1 = readEntityResponse1.getAspects().get(keyName);
    assertTrue(
        DataTemplateUtil.areEqual(expectedKey1, new CorpUserKey(envelopedKey1.getValue().data())));

    // Entity 2
    EntityResponse readEntityResponse2 = readEntities.get(entityUrn2);
    assertEquals(readEntityResponse2.getAspects().size(), 2); // Key + Info aspect.
    EnvelopedAspect envelopedAspect2 = readEntityResponse2.getAspects().get(aspectName);
    assertEquals(envelopedAspect2.getName(), aspectName);
    assertTrue(
        DataTemplateUtil.areEqual(
            writeEntity2.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserInfo(),
            new CorpUserInfo(envelopedAspect2.getValue().data())));
    CorpUserKey expectedKey2 = new CorpUserKey();
    expectedKey2.setUsername("tester2");
    EnvelopedAspect envelopedKey2 = readEntityResponse2.getAspects().get(keyName);
    assertTrue(
        DataTemplateUtil.areEqual(expectedKey2, new CorpUserKey(envelopedKey2.getValue().data())));

    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(Mockito.eq(entityUrn1), Mockito.any(), Mockito.any());

    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(Mockito.eq(entityUrn2), Mockito.any(), Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestGetEntitiesVersionedV2() throws Exception {
    // Test Writing a CorpUser Entity
    Urn entityUrn1 = UrnUtils.getUrn("urn:li:corpuser:tester1");
    VersionedUrn versionedUrn1 =
        new VersionedUrn().setUrn(entityUrn1).setVersionStamp("corpUserInfo:0");
    com.linkedin.entity.Entity writeEntity1 = createCorpUserEntity(entityUrn1, "tester@test.com");

    Urn entityUrn2 = UrnUtils.getUrn("urn:li:corpuser:tester2");
    VersionedUrn versionedUrn2 = new VersionedUrn().setUrn(entityUrn2);
    com.linkedin.entity.Entity writeEntity2 = createCorpUserEntity(entityUrn2, "tester2@test.com");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata(1625792689, "run-123");
    SystemMetadata metadata2 = AspectGenerationUtils.createSystemMetadata(1625792690, "run-123");

    String aspectName = "corpUserInfo";
    String keyName = "corpUserKey";

    // 1. Ingest Entities
    _entityServiceImpl.ingestEntities(
        opContext,
        ImmutableList.of(writeEntity1, writeEntity2),
        TEST_AUDIT_STAMP,
        ImmutableList.of(metadata1, metadata2));

    // 2. Retrieve Entities
    Map<Urn, EntityResponse> readEntities =
        _entityServiceImpl.getEntitiesVersionedV2(
            opContext, ImmutableSet.of(versionedUrn1, versionedUrn2), ImmutableSet.of(aspectName));

    // 3. Compare Entity Objects

    // Entity 1
    EntityResponse readEntityResponse1 = readEntities.get(entityUrn1);
    assertEquals(2, readEntityResponse1.getAspects().size()); // Key + Info aspect.
    EnvelopedAspect envelopedAspect1 = readEntityResponse1.getAspects().get(aspectName);
    assertEquals(envelopedAspect1.getName(), aspectName);
    assertTrue(
        DataTemplateUtil.areEqual(
            writeEntity1.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserInfo(),
            new CorpUserInfo(envelopedAspect1.getValue().data())));
    CorpUserKey expectedKey1 = new CorpUserKey();
    expectedKey1.setUsername("tester1");
    EnvelopedAspect envelopedKey1 = readEntityResponse1.getAspects().get(keyName);
    assertTrue(
        DataTemplateUtil.areEqual(expectedKey1, new CorpUserKey(envelopedKey1.getValue().data())));

    // Entity 2
    EntityResponse readEntityResponse2 = readEntities.get(entityUrn2);
    assertEquals(2, readEntityResponse2.getAspects().size()); // Key + Info aspect.
    EnvelopedAspect envelopedAspect2 = readEntityResponse2.getAspects().get(aspectName);
    assertEquals(envelopedAspect2.getName(), aspectName);
    assertTrue(
        DataTemplateUtil.areEqual(
            writeEntity2.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserInfo(),
            new CorpUserInfo(envelopedAspect2.getValue().data())));
    CorpUserKey expectedKey2 = new CorpUserKey();
    expectedKey2.setUsername("tester2");
    EnvelopedAspect envelopedKey2 = readEntityResponse2.getAspects().get(keyName);
    assertTrue(
        DataTemplateUtil.areEqual(expectedKey2, new CorpUserKey(envelopedKey2.getValue().data())));

    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(Mockito.eq(entityUrn1), Mockito.any(), Mockito.any());

    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(Mockito.eq(entityUrn2), Mockito.any(), Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestAspectsGetLatestAspects() throws Exception {

    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test");

    List<Pair<String, RecordTemplate>> pairToIngest = new ArrayList<>();

    Status writeAspect1 = new Status().setRemoved(false);
    String aspectName1 = AspectGenerationUtils.getAspectName(writeAspect1);
    pairToIngest.add(getAspectRecordPair(writeAspect1, Status.class));

    CorpUserInfo writeAspect2 = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    String aspectName2 = AspectGenerationUtils.getAspectName(writeAspect2);
    pairToIngest.add(getAspectRecordPair(writeAspect2, CorpUserInfo.class));

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata();
    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, pairToIngest, TEST_AUDIT_STAMP, metadata1);

    Map<String, RecordTemplate> latestAspects =
        _entityServiceImpl.getLatestAspectsForUrn(
            opContext, entityUrn, new HashSet<>(Arrays.asList(aspectName1, aspectName2)), false);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, latestAspects.get(aspectName1)));
    assertTrue(DataTemplateUtil.areEqual(writeAspect2, latestAspects.get(aspectName2)));

    verify(_mockProducer, times(3))
        .produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testReingestAspectsGetLatestAspects() throws Exception {

    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test");

    List<Pair<String, RecordTemplate>> pairToIngest = new ArrayList<>();

    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    writeAspect1.setCustomProperties(new StringMap());
    String aspectName1 = AspectGenerationUtils.getAspectName(writeAspect1);
    pairToIngest.add(getAspectRecordPair(writeAspect1, CorpUserInfo.class));

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata(1);
    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, pairToIngest, TEST_AUDIT_STAMP, metadata1);

    final MetadataChangeLog initialChangeLog = new MetadataChangeLog();
    initialChangeLog.setEntityType(entityUrn.getEntityType());
    initialChangeLog.setEntityUrn(entityUrn);
    initialChangeLog.setChangeType(ChangeType.UPSERT);
    initialChangeLog.setAspectName(aspectName1);
    initialChangeLog.setCreated(TEST_AUDIT_STAMP);

    GenericAspect aspect = GenericRecordUtils.serializeAspect(pairToIngest.get(0).getSecond());

    initialChangeLog.setAspect(aspect);
    initialChangeLog.setSystemMetadata(metadata1);
    initialChangeLog.setEntityKeyAspect(
        GenericRecordUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(
                entityUrn,
                _testEntityRegistry.getEntitySpec(entityUrn.getEntityType()).getKeyAspectSpec())));

    final MetadataChangeLog restateChangeLog = new MetadataChangeLog();
    restateChangeLog.setEntityType(entityUrn.getEntityType());
    restateChangeLog.setEntityUrn(entityUrn);
    restateChangeLog.setChangeType(ChangeType.RESTATE);
    restateChangeLog.setAspectName(aspectName1);
    restateChangeLog.setCreated(TEST_AUDIT_STAMP);
    restateChangeLog.setAspect(aspect);
    restateChangeLog.setSystemMetadata(metadata1);
    restateChangeLog.setPreviousAspectValue(aspect);
    restateChangeLog.setPreviousSystemMetadata(simulatePullFromDB(metadata1, SystemMetadata.class));

    Map<String, RecordTemplate> latestAspects =
        _entityServiceImpl.getLatestAspectsForUrn(
            opContext, entityUrn, new HashSet<>(List.of(aspectName1)), false);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, latestAspects.get(aspectName1)));

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), Mockito.any(), Mockito.eq(initialChangeLog));

    // Mockito detects the previous invocation and throws an error in verifying the second call
    // unless invocations are cleared
    clearInvocations(_mockProducer);

    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, pairToIngest, TEST_AUDIT_STAMP, metadata1);

    verify(_mockProducer, times(0))
        .produceMetadataChangeLog(Mockito.any(), Mockito.any(), Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testReingestLineageAspect() throws Exception {

    Urn entityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:looker,sample_dataset,PROD)");

    List<Pair<String, RecordTemplate>> pairToIngest = new ArrayList<>();

    final UpstreamLineage upstreamLineage = AspectGenerationUtils.createUpstreamLineage();
    String aspectName1 = AspectGenerationUtils.getAspectName(upstreamLineage);
    pairToIngest.add(getAspectRecordPair(upstreamLineage, UpstreamLineage.class));

    _entityServiceImpl.ingestAspects(
        opContext,
        entityUrn,
        pairToIngest,
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata());

    final MetadataChangeLog initialChangeLog = new MetadataChangeLog();
    initialChangeLog.setEntityType(entityUrn.getEntityType());
    initialChangeLog.setEntityUrn(entityUrn);
    initialChangeLog.setChangeType(ChangeType.UPSERT);
    initialChangeLog.setAspectName(aspectName1);
    initialChangeLog.setCreated(TEST_AUDIT_STAMP);

    GenericAspect aspect = GenericRecordUtils.serializeAspect(pairToIngest.get(0).getSecond());

    SystemMetadata initialSystemMetadata = AspectGenerationUtils.createSystemMetadata(1);
    initialChangeLog.setAspect(aspect);
    initialChangeLog.setSystemMetadata(initialSystemMetadata);
    initialChangeLog.setEntityKeyAspect(
        GenericRecordUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(
                entityUrn,
                _testEntityRegistry.getEntitySpec(entityUrn.getEntityType()).getKeyAspectSpec())));

    SystemMetadata futureSystemMetadata = AspectGenerationUtils.createSystemMetadata(1);
    futureSystemMetadata.setLastObserved(futureSystemMetadata.getLastObserved() + 1);

    final MetadataChangeLog restateChangeLog = new MetadataChangeLog();
    restateChangeLog.setEntityType(entityUrn.getEntityType());
    restateChangeLog.setEntityUrn(entityUrn);
    restateChangeLog.setChangeType(ChangeType.RESTATE);
    restateChangeLog.setAspectName(aspectName1);
    restateChangeLog.setCreated(TEST_AUDIT_STAMP);
    restateChangeLog.setAspect(aspect);
    restateChangeLog.setSystemMetadata(futureSystemMetadata);
    restateChangeLog.setPreviousAspectValue(aspect);
    restateChangeLog.setPreviousSystemMetadata(
        simulatePullFromDB(initialSystemMetadata, SystemMetadata.class));
    restateChangeLog.setEntityKeyAspect(
        GenericRecordUtils.serializeAspect(
            EntityKeyUtils.convertUrnToEntityKey(
                entityUrn,
                _testEntityRegistry.getEntitySpec(entityUrn.getEntityType()).getKeyAspectSpec())));

    Map<String, RecordTemplate> latestAspects =
        _entityServiceImpl.getLatestAspectsForUrn(
            opContext, entityUrn, new HashSet<>(List.of(aspectName1)), false);
    assertTrue(DataTemplateUtil.areEqual(upstreamLineage, latestAspects.get(aspectName1)));

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), Mockito.any(), Mockito.eq(initialChangeLog));

    // Mockito detects the previous invocation and throws an error in verifying the second call
    // unless invocations are cleared
    clearInvocations(_mockProducer);

    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, pairToIngest, TEST_AUDIT_STAMP, futureSystemMetadata);

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), Mockito.any(), Mockito.eq(restateChangeLog));

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testReingestLineageProposal() throws Exception {

    Urn entityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:looker,sample_dataset,PROD)");

    List<Pair<String, RecordTemplate>> pairToIngest = new ArrayList<>();

    final UpstreamLineage upstreamLineage = AspectGenerationUtils.createUpstreamLineage();
    String aspectName1 = AspectGenerationUtils.getAspectName(upstreamLineage);

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata(1);
    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setEntityType(entityUrn.getEntityType());
    GenericAspect genericAspect = GenericRecordUtils.serializeAspect(upstreamLineage);
    mcp1.setAspect(genericAspect);
    mcp1.setEntityUrn(entityUrn);
    mcp1.setChangeType(ChangeType.UPSERT);
    mcp1.setSystemMetadata(metadata1);
    mcp1.setAspectName(UPSTREAM_LINEAGE_ASPECT_NAME);

    _entityServiceImpl.ingestProposal(opContext, mcp1, TEST_AUDIT_STAMP, false);

    final MetadataChangeLog initialChangeLog = new MetadataChangeLog();
    initialChangeLog.setEntityType(entityUrn.getEntityType());
    initialChangeLog.setEntityUrn(entityUrn);
    initialChangeLog.setChangeType(ChangeType.UPSERT);
    initialChangeLog.setAspectName(aspectName1);
    initialChangeLog.setCreated(TEST_AUDIT_STAMP);

    initialChangeLog.setAspect(genericAspect);
    initialChangeLog.setSystemMetadata(metadata1);

    SystemMetadata futureSystemMetadata = AspectGenerationUtils.createSystemMetadata(1);
    futureSystemMetadata.setLastObserved(futureSystemMetadata.getLastObserved() + 1);

    MetadataChangeProposal mcp2 = new MetadataChangeProposal(mcp1.data().copy());
    mcp2.getSystemMetadata().setLastObserved(futureSystemMetadata.getLastObserved());

    final MetadataChangeLog restateChangeLog = new MetadataChangeLog();
    restateChangeLog.setEntityType(entityUrn.getEntityType());
    restateChangeLog.setEntityUrn(entityUrn);
    restateChangeLog.setChangeType(ChangeType.RESTATE);
    restateChangeLog.setAspectName(aspectName1);
    restateChangeLog.setCreated(TEST_AUDIT_STAMP);
    restateChangeLog.setAspect(genericAspect);
    restateChangeLog.setSystemMetadata(futureSystemMetadata);
    restateChangeLog.setPreviousAspectValue(genericAspect);
    restateChangeLog.setPreviousSystemMetadata(simulatePullFromDB(metadata1, SystemMetadata.class));

    Map<String, RecordTemplate> latestAspects =
        _entityServiceImpl.getLatestAspectsForUrn(
            opContext, entityUrn, new HashSet<>(List.of(aspectName1)), false);
    assertTrue(DataTemplateUtil.areEqual(upstreamLineage, latestAspects.get(aspectName1)));

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), Mockito.any(), Mockito.eq(initialChangeLog));

    // Mockito detects the previous invocation and throws an error in verifying the second call
    // unless invocations are cleared
    clearInvocations(_mockProducer);

    _entityServiceImpl.ingestProposal(opContext, mcp2, TEST_AUDIT_STAMP, false);

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), Mockito.any(), Mockito.eq(restateChangeLog));

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestTimeseriesAspect() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
    DatasetProfile datasetProfile = new DatasetProfile();
    datasetProfile.setRowCount(1000);
    datasetProfile.setColumnCount(15);
    datasetProfile.setTimestampMillis(0L);
    MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityUrn(entityUrn);
    gmce.setChangeType(ChangeType.UPSERT);
    gmce.setEntityType("dataset");
    gmce.setAspectName("datasetProfile");
    JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();
    byte[] datasetProfileSerialized = dataTemplateCodec.dataTemplateToBytes(datasetProfile);
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(datasetProfileSerialized));
    genericAspect.setContentType("application/json");
    gmce.setAspect(genericAspect);
    _entityServiceImpl.ingestProposal(opContext, gmce, TEST_AUDIT_STAMP, false);
  }

  @Test
  public void testAsyncProposalVersioned() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
    DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setName("Foo Bar");
    MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityUrn(entityUrn);
    gmce.setChangeType(ChangeType.UPSERT);
    gmce.setEntityType("dataset");
    gmce.setAspectName("datasetProperties");
    gmce.setSystemMetadata(new SystemMetadata());
    JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();
    byte[] datasetPropertiesSerialized = dataTemplateCodec.dataTemplateToBytes(datasetProperties);
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(datasetPropertiesSerialized));
    genericAspect.setContentType("application/json");
    gmce.setAspect(genericAspect);
    _entityServiceImpl.ingestProposal(opContext, gmce, TEST_AUDIT_STAMP, true);
    verify(_mockProducer, times(0))
        .produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), Mockito.any());
    verify(_mockProducer, times(1))
        .produceMetadataChangeProposal(Mockito.eq(entityUrn), Mockito.eq(gmce));
  }

  @Test
  public void testAsyncProposalTimeseries() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
    DatasetProfile datasetProfile = new DatasetProfile();
    datasetProfile.setRowCount(1000);
    datasetProfile.setColumnCount(15);
    datasetProfile.setTimestampMillis(0L);
    MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityUrn(entityUrn);
    gmce.setChangeType(ChangeType.UPSERT);
    gmce.setEntityType("dataset");
    gmce.setAspectName("datasetProfile");
    JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();
    byte[] datasetProfileSerialized = dataTemplateCodec.dataTemplateToBytes(datasetProfile);
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(datasetProfileSerialized));
    genericAspect.setContentType("application/json");
    gmce.setAspect(genericAspect);
    _entityServiceImpl.ingestProposal(opContext, gmce, TEST_AUDIT_STAMP, true);
    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), Mockito.any());
    verify(_mockProducer, times(0))
        .produceMetadataChangeProposal(Mockito.eq(entityUrn), Mockito.eq(gmce));
  }

  @Test
  public void testUpdateGetAspect() throws AssertionError {
    // Test Writing a CorpUser Entity
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test");

    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());
    AspectSpec corpUserInfoSpec =
        opContext.getEntityRegistry().getEntitySpec("corpuser").getAspectSpec("corpUserInfo");

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect = AspectGenerationUtils.createCorpUserInfo("email@test.com");

    // Validate retrieval of CorpUserInfo Aspect #1
    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, List.of(Pair.of(aspectName, writeAspect)), TEST_AUDIT_STAMP, null);

    RecordTemplate readAspect1 = _entityServiceImpl.getAspect(opContext, entityUrn, aspectName, 0);
    assertTrue(DataTemplateUtil.areEqual(writeAspect, readAspect1));
    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), Mockito.eq(corpUserInfoSpec), Mockito.any());

    // Ingest CorpUserInfo Aspect #2
    writeAspect.setEmail("newemail@test.com");

    // Validate retrieval of CorpUserInfo Aspect #2
    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, List.of(Pair.of(aspectName, writeAspect)), TEST_AUDIT_STAMP, null);

    RecordTemplate readAspect2 = _entityServiceImpl.getAspect(opContext, entityUrn, aspectName, 0);
    assertTrue(DataTemplateUtil.areEqual(writeAspect, readAspect2));
    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), Mockito.eq(corpUserInfoSpec), Mockito.any());

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn),
            Mockito.eq(
                opContext
                    .getEntityRegistry()
                    .getEntitySpec("corpUser")
                    .getAspectSpec("corpUserKey")),
            Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testGetAspectAtVersion() throws AssertionError {
    // Test Writing a CorpUser Entity
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test");

    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());
    AspectSpec corpUserInfoSpec =
        opContext.getEntityRegistry().getEntitySpec("corpuser").getAspectSpec("corpUserInfo");

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    CorpUserInfo writeAspect2 = AspectGenerationUtils.createCorpUserInfo("email2@test.com");

    // Validate retrieval of CorpUserInfo Aspect #1
    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, List.of(Pair.of(aspectName, writeAspect1)), TEST_AUDIT_STAMP, null);

    VersionedAspect writtenVersionedAspect1 = new VersionedAspect();
    writtenVersionedAspect1.setAspect(Aspect.create(writeAspect1));
    writtenVersionedAspect1.setVersion(0);

    VersionedAspect readAspect1 =
        _entityServiceImpl.getVersionedAspect(opContext, entityUrn, aspectName, 0);
    assertTrue(DataTemplateUtil.areEqual(writtenVersionedAspect1, readAspect1));
    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), Mockito.eq(corpUserInfoSpec), Mockito.any());

    readAspect1 = _entityServiceImpl.getVersionedAspect(opContext, entityUrn, aspectName, -1);
    assertTrue(DataTemplateUtil.areEqual(writtenVersionedAspect1, readAspect1));

    // Validate retrieval of CorpUserInfo Aspect #2
    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, List.of(Pair.of(aspectName, writeAspect2)), TEST_AUDIT_STAMP, null);

    VersionedAspect writtenVersionedAspect2 = new VersionedAspect();
    writtenVersionedAspect2.setAspect(Aspect.create(writeAspect2));
    writtenVersionedAspect2.setVersion(0);

    VersionedAspect readAspectVersion2 =
        _entityServiceImpl.getVersionedAspect(opContext, entityUrn, aspectName, 0);
    assertFalse(DataTemplateUtil.areEqual(writtenVersionedAspect1, readAspectVersion2));
    assertTrue(DataTemplateUtil.areEqual(writtenVersionedAspect2, readAspectVersion2));
    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), Mockito.eq(corpUserInfoSpec), Mockito.any());

    readAspect1 = _entityServiceImpl.getVersionedAspect(opContext, entityUrn, aspectName, -1);
    assertFalse(DataTemplateUtil.areEqual(writtenVersionedAspect1, readAspect1));

    // check key aspect
    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn),
            Mockito.eq(
                opContext
                    .getEntityRegistry()
                    .getEntitySpec("corpuser")
                    .getAspectSpec("corpUserKey")),
            Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testRollbackAspect() throws AssertionError {
    Urn entityUrn1 = UrnUtils.getUrn("urn:li:corpuser:test1");
    Urn entityUrn2 = UrnUtils.getUrn("urn:li:corpuser:test2");
    Urn entityUrn3 = UrnUtils.getUrn("urn:li:corpuser:test3");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata(1625792689, "run-123");
    SystemMetadata metadata2 = AspectGenerationUtils.createSystemMetadata(1635792689, "run-456");

    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");

    // Ingest CorpUserInfo Aspect #2
    CorpUserInfo writeAspect2 = AspectGenerationUtils.createCorpUserInfo("email2@test.com");

    // Ingest CorpUserInfo Aspect #3
    CorpUserInfo writeAspect3 = AspectGenerationUtils.createCorpUserInfo("email3@test.com");

    // Ingest CorpUserInfo Aspect #1 Overwrite
    CorpUserInfo writeAspect1Overwrite =
        AspectGenerationUtils.createCorpUserInfo("email1.overwrite@test.com");

    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn2)
                .aspectName(aspectName)
                .recordTemplate(writeAspect2)
                .auditStamp(TEST_AUDIT_STAMP)
                .systemMetadata(metadata1)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn3)
                .aspectName(aspectName)
                .recordTemplate(writeAspect3)
                .auditStamp(TEST_AUDIT_STAMP)
                .systemMetadata(metadata1)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1Overwrite)
                .systemMetadata(metadata2)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // this should no-op since this run has been overwritten
    AspectRowSummary rollbackOverwrittenAspect = new AspectRowSummary();
    rollbackOverwrittenAspect.setRunId("run-123");
    rollbackOverwrittenAspect.setAspectName(aspectName);
    rollbackOverwrittenAspect.setUrn(entityUrn1.toString());

    _entityServiceImpl.rollbackRun(
        opContext, ImmutableList.of(rollbackOverwrittenAspect), "run-123", true);

    // assert nothing was deleted
    RecordTemplate readAspectOriginal =
        _entityServiceImpl.getAspect(opContext, entityUrn1, aspectName, 1);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, readAspectOriginal));

    RecordTemplate readAspectOverwrite =
        _entityServiceImpl.getAspect(opContext, entityUrn1, aspectName, 0);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1Overwrite, readAspectOverwrite));

    // this should delete the most recent aspect
    AspectRowSummary rollbackRecentAspect = new AspectRowSummary();
    rollbackRecentAspect.setRunId("run-456");
    rollbackRecentAspect.setAspectName(aspectName);
    rollbackRecentAspect.setUrn(entityUrn1.toString());

    _entityServiceImpl.rollbackRun(
        opContext, ImmutableList.of(rollbackOverwrittenAspect), "run-456", true);

    // assert the new most recent aspect is the original one
    RecordTemplate readNewRecentAspect =
        _entityServiceImpl.getAspect(opContext, entityUrn1, aspectName, 0);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, readNewRecentAspect));
  }

  @Test
  public void testRollbackKey() throws AssertionError {
    Urn entityUrn1 = UrnUtils.getUrn("urn:li:corpuser:test1");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata(1625792689, "run-123");
    SystemMetadata metadata2 = AspectGenerationUtils.createSystemMetadata(1635792689, "run-456");

    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());
    String keyAspectName = opContext.getKeyAspectName(entityUrn1);

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");

    RecordTemplate writeKey1 =
        EntityApiUtils.buildKeyAspect(opContext.getEntityRegistry(), entityUrn1);

    // Ingest CorpUserInfo Aspect #1 Overwrite
    CorpUserInfo writeAspect1Overwrite =
        AspectGenerationUtils.createCorpUserInfo("email1.overwrite@test.com");

    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(keyAspectName)
                .recordTemplate(writeKey1)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1Overwrite)
                .systemMetadata(metadata2)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // this should no-op since the key should have been written in the furst run
    AspectRowSummary rollbackKeyWithWrongRunId = new AspectRowSummary();
    rollbackKeyWithWrongRunId.setRunId("run-456");
    rollbackKeyWithWrongRunId.setAspectName("corpUserKey");
    rollbackKeyWithWrongRunId.setUrn(entityUrn1.toString());

    _entityServiceImpl.rollbackRun(
        opContext, ImmutableList.of(rollbackKeyWithWrongRunId), "run-456", true);

    // assert nothing was deleted
    RecordTemplate readAspectOriginal =
        _entityServiceImpl.getAspect(opContext, entityUrn1, aspectName, 1);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, readAspectOriginal));

    RecordTemplate readAspectOverwrite =
        _entityServiceImpl.getAspect(opContext, entityUrn1, aspectName, 0);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1Overwrite, readAspectOverwrite));

    // this should delete the most recent aspect
    AspectRowSummary rollbackKeyWithCorrectRunId = new AspectRowSummary();
    rollbackKeyWithCorrectRunId.setRunId("run-123");
    rollbackKeyWithCorrectRunId.setAspectName("corpUserKey");
    rollbackKeyWithCorrectRunId.setUrn(entityUrn1.toString());

    _entityServiceImpl.rollbackRun(
        opContext, ImmutableList.of(rollbackKeyWithCorrectRunId), "run-123", true);

    // assert the new most recent aspect is null
    RecordTemplate readNewRecentAspect =
        _entityServiceImpl.getAspect(opContext, entityUrn1, aspectName, 0);
    assertTrue(DataTemplateUtil.areEqual(null, readNewRecentAspect));
  }

  @Test
  public void testRollbackUrn() throws AssertionError {
    Urn entityUrn1 = UrnUtils.getUrn("urn:li:corpuser:test1");
    Urn entityUrn2 = UrnUtils.getUrn("urn:li:corpuser:test2");
    Urn entityUrn3 = UrnUtils.getUrn("urn:li:corpuser:test3");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata(1625792689, "run-123");
    SystemMetadata metadata2 = AspectGenerationUtils.createSystemMetadata(1635792689, "run-456");

    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());
    String keyAspectName = opContext.getKeyAspectName(entityUrn1);

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");

    RecordTemplate writeKey1 =
        EntityApiUtils.buildKeyAspect(opContext.getEntityRegistry(), entityUrn1);

    // Ingest CorpUserInfo Aspect #2
    CorpUserInfo writeAspect2 = AspectGenerationUtils.createCorpUserInfo("email2@test.com");

    // Ingest CorpUserInfo Aspect #3
    CorpUserInfo writeAspect3 = AspectGenerationUtils.createCorpUserInfo("email3@test.com");

    // Ingest CorpUserInfo Aspect #1 Overwrite
    CorpUserInfo writeAspect1Overwrite =
        AspectGenerationUtils.createCorpUserInfo("email1.overwrite@test.com");

    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(keyAspectName)
                .recordTemplate(writeKey1)
                .auditStamp(TEST_AUDIT_STAMP)
                .systemMetadata(metadata1)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn2)
                .aspectName(aspectName)
                .recordTemplate(writeAspect2)
                .auditStamp(TEST_AUDIT_STAMP)
                .systemMetadata(metadata1)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn3)
                .aspectName(aspectName)
                .recordTemplate(writeAspect3)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1Overwrite)
                .systemMetadata(metadata2)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // this should no-op since the key should have been written in the furst run
    AspectRowSummary rollbackKeyWithWrongRunId = new AspectRowSummary();
    rollbackKeyWithWrongRunId.setRunId("run-456");
    rollbackKeyWithWrongRunId.setAspectName("CorpUserKey");
    rollbackKeyWithWrongRunId.setUrn(entityUrn1.toString());

    // this should delete all related aspects
    _entityServiceImpl.deleteUrn(opContext, UrnUtils.getUrn("urn:li:corpuser:test1"));

    // assert the new most recent aspect is null
    RecordTemplate readNewRecentAspect =
        _entityServiceImpl.getAspect(opContext, entityUrn1, aspectName, 0);
    assertTrue(DataTemplateUtil.areEqual(null, readNewRecentAspect));

    RecordTemplate deletedKeyAspect =
        _entityServiceImpl.getAspect(opContext, entityUrn1, "corpUserKey", 0);
    assertTrue(DataTemplateUtil.areEqual(null, deletedKeyAspect));
  }

  @Test
  public void testIngestGetLatestAspect() throws AssertionError {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test");

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    String aspectName = AspectGenerationUtils.getAspectName(writeAspect1);

    SystemMetadata metadata1 =
        AspectGenerationUtils.createSystemMetadata(1625792689, "run-123", "run-123", "1");
    SystemMetadata metadata2 =
        AspectGenerationUtils.createSystemMetadata(1635792689, "run-456", "run-456", "2");

    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1)
                .auditStamp(TEST_AUDIT_STAMP)
                .systemMetadata(metadata1)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // Validate retrieval of CorpUserInfo Aspect #1
    RecordTemplate readAspect1 =
        _entityServiceImpl.getLatestAspect(opContext, entityUrn, aspectName);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, readAspect1));

    ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn),
            Mockito.eq(
                opContext
                    .getEntityRegistry()
                    .getEntitySpec("corpUser")
                    .getAspectSpec("corpUserInfo")),
            mclCaptor.capture());
    MetadataChangeLog mcl = mclCaptor.getValue();
    assertEquals(mcl.getEntityType(), "corpuser");
    assertNull(mcl.getPreviousAspectValue());
    assertNull(mcl.getPreviousSystemMetadata());
    assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn),
            Mockito.eq(
                opContext
                    .getEntityRegistry()
                    .getEntitySpec("corpUser")
                    .getAspectSpec("corpUserKey")),
            Mockito.any());

    verifyNoMoreInteractions(_mockProducer);

    reset(_mockProducer);

    // Ingest CorpUserInfo Aspect #2
    CorpUserInfo writeAspect2 = AspectGenerationUtils.createCorpUserInfo("email2@test.com");

    items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect2)
                .auditStamp(TEST_AUDIT_STAMP)
                .systemMetadata(metadata2)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // Validate retrieval of CorpUserInfo Aspect #2
    RecordTemplate readAspect2 =
        _entityServiceImpl.getLatestAspect(opContext, entityUrn, aspectName);
    EntityAspect readAspectDao1 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 1);
    EntityAspect readAspectDao2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

    assertTrue(DataTemplateUtil.areEqual(writeAspect2, readAspect2));
    assertTrue(
        DataTemplateUtil.areEqual(
            EntityApiUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata2));
    assertTrue(
        DataTemplateUtil.areEqual(
            EntityApiUtils.parseSystemMetadata(readAspectDao1.getSystemMetadata()), metadata1));

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());
    mcl = mclCaptor.getValue();
    assertEquals(mcl.getEntityType(), "corpuser");
    assertNotNull(mcl.getPreviousAspectValue());
    assertNotNull(mcl.getPreviousSystemMetadata());
    assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestGetLatestEnvelopedAspect() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test");

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    String aspectName = AspectGenerationUtils.getAspectName(writeAspect1);

    SystemMetadata metadata1 =
        AspectGenerationUtils.createSystemMetadata(1625792689, "run-123", "run-123", "1");
    SystemMetadata metadata2 =
        AspectGenerationUtils.createSystemMetadata(1635792689, "run-456", "run-456", "2");

    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1)
                .auditStamp(TEST_AUDIT_STAMP)
                .systemMetadata(metadata1)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // Validate retrieval of CorpUserInfo Aspect #1
    EnvelopedAspect readAspect1 =
        _entityServiceImpl.getLatestEnvelopedAspect(opContext, "corpuser", entityUrn, aspectName);
    assertTrue(
        DataTemplateUtil.areEqual(writeAspect1, new CorpUserInfo(readAspect1.getValue().data())));

    // Ingest CorpUserInfo Aspect #2
    CorpUserInfo writeAspect2 = AspectGenerationUtils.createCorpUserInfo("email2@test.com");

    items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect2)
                .systemMetadata(metadata2)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // Validate retrieval of CorpUserInfo Aspect #2
    EnvelopedAspect readAspect2 =
        _entityServiceImpl.getLatestEnvelopedAspect(opContext, "corpuser", entityUrn, aspectName);
    EntityAspect readAspectDao1 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 1);
    EntityAspect readAspectDao2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

    assertTrue(
        DataTemplateUtil.areEqual(writeAspect2, new CorpUserInfo(readAspect2.getValue().data())));
    assertTrue(
        DataTemplateUtil.areEqual(
            EntityApiUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata2));
    assertTrue(
        DataTemplateUtil.areEqual(
            EntityApiUtils.parseSystemMetadata(readAspectDao1.getSystemMetadata()), metadata1));

    verify(_mockProducer, times(2))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn),
            Mockito.eq(
                opContext
                    .getEntityRegistry()
                    .getEntitySpec("corpUser")
                    .getAspectSpec("corpUserInfo")),
            Mockito.any());

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn),
            Mockito.eq(
                opContext
                    .getEntityRegistry()
                    .getEntitySpec("corpUser")
                    .getAspectSpec("corpUserKey")),
            Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestSameAspect() throws AssertionError {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test");

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    String aspectName = AspectGenerationUtils.getAspectName(writeAspect1);

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata(1625792689, "run-123");
    SystemMetadata metadata2 = AspectGenerationUtils.createSystemMetadata(1635792689, "run-456");
    SystemMetadata metadata3 =
        AspectGenerationUtils.createSystemMetadata(1635792689, "run-456", "run-123", "1");

    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // Validate retrieval of CorpUserInfo Aspect #1
    RecordTemplate readAspect1 =
        _entityServiceImpl.getLatestAspect(opContext, entityUrn, aspectName);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, readAspect1));

    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn),
            Mockito.eq(
                opContext
                    .getEntityRegistry()
                    .getEntitySpec("corpUser")
                    .getAspectSpec("corpUserKey")),
            Mockito.any());

    ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn),
            Mockito.eq(
                opContext
                    .getEntityRegistry()
                    .getEntitySpec("corpUser")
                    .getAspectSpec("corpUserInfo")),
            mclCaptor.capture());
    MetadataChangeLog mcl = mclCaptor.getValue();
    assertEquals(mcl.getEntityType(), "corpuser");
    assertNull(mcl.getPreviousAspectValue());
    assertNull(mcl.getPreviousSystemMetadata());
    assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

    verifyNoMoreInteractions(_mockProducer);

    reset(_mockProducer);

    // Ingest CorpUserInfo Aspect #2
    CorpUserInfo writeAspect2 = AspectGenerationUtils.createCorpUserInfo("email@test.com");

    items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect2)
                .systemMetadata(metadata2)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // Validate retrieval of CorpUserInfo Aspect #2
    RecordTemplate readAspect2 =
        _entityServiceImpl.getLatestAspect(opContext, entityUrn, aspectName);
    EntityAspect readAspectDao2 =
        _aspectDao.getAspect(entityUrn.toString(), aspectName, ASPECT_LATEST_VERSION);

    assertTrue(DataTemplateUtil.areEqual(writeAspect2, readAspect2));
    assertFalse(
        DataTemplateUtil.areEqual(
            EntityApiUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata2));
    assertFalse(
        DataTemplateUtil.areEqual(
            EntityApiUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata1));

    assertTrue(
        DataTemplateUtil.areEqual(
            EntityApiUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata3),
        String.format(
            "Expected %s == %s",
            EntityApiUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata3));

    verify(_mockProducer, times(0))
        .produceMetadataChangeLog(Mockito.any(), Mockito.any(), Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testRetention() throws AssertionError {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test1");

    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());

    // Ingest CorpUserInfo Aspect
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    CorpUserInfo writeAspect1a = AspectGenerationUtils.createCorpUserInfo("email_a@test.com");
    CorpUserInfo writeAspect1b = AspectGenerationUtils.createCorpUserInfo("email_b@test.com");

    String aspectName2 = AspectGenerationUtils.getAspectName(new Status());
    // Ingest Status Aspect
    Status writeAspect2 = new Status().setRemoved(true);
    Status writeAspect2a = new Status().setRemoved(false);
    Status writeAspect2b = new Status().setRemoved(true);

    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1)
                .systemMetadata(AspectGenerationUtils.createSystemMetadata())
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1a)
                .systemMetadata(AspectGenerationUtils.createSystemMetadata())
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1b)
                .systemMetadata(AspectGenerationUtils.createSystemMetadata())
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName2)
                .recordTemplate(writeAspect2)
                .systemMetadata(AspectGenerationUtils.createSystemMetadata())
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName2)
                .recordTemplate(writeAspect2a)
                .systemMetadata(AspectGenerationUtils.createSystemMetadata())
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName2)
                .recordTemplate(writeAspect2b)
                .systemMetadata(AspectGenerationUtils.createSystemMetadata())
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    assertEquals(_entityServiceImpl.getAspect(opContext, entityUrn, aspectName, 1), writeAspect1);
    assertEquals(_entityServiceImpl.getAspect(opContext, entityUrn, aspectName2, 1), writeAspect2);

    _retentionService.setRetention(
        opContext,
        null,
        null,
        new DataHubRetentionConfig()
            .setRetention(
                new Retention().setVersion(new VersionBasedRetention().setMaxVersions(2))));
    _retentionService.setRetention(
        opContext,
        "corpuser",
        "status",
        new DataHubRetentionConfig()
            .setRetention(
                new Retention().setVersion(new VersionBasedRetention().setMaxVersions(4))));

    // Ingest CorpUserInfo Aspect again
    CorpUserInfo writeAspect1c = AspectGenerationUtils.createCorpUserInfo("email_c@test.com");
    // Ingest Status Aspect again
    Status writeAspect2c = new Status().setRemoved(false);

    items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1c)
                .systemMetadata(AspectGenerationUtils.createSystemMetadata())
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()),
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName2)
                .recordTemplate(writeAspect2c)
                .systemMetadata(AspectGenerationUtils.createSystemMetadata())
                .auditStamp(TEST_AUDIT_STAMP)
                .build(opContext.getAspectRetriever()));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    assertNull(_entityServiceImpl.getAspect(opContext, entityUrn, aspectName, 1));
    assertEquals(_entityServiceImpl.getAspect(opContext, entityUrn, aspectName2, 1), writeAspect2);

    // Reset retention policies
    _retentionService.setRetention(
        opContext,
        null,
        null,
        new DataHubRetentionConfig()
            .setRetention(
                new Retention().setVersion(new VersionBasedRetention().setMaxVersions(1))));
    _retentionService.deleteRetention(opContext, "corpuser", "status");
    // Invoke batch apply
    _retentionService.batchApplyRetention(null, null);
    assertEquals(
        _entityServiceImpl
            .listLatestAspects(opContext, entityUrn.getEntityType(), aspectName, 0, 10)
            .getTotalCount(),
        1);
    assertEquals(
        _entityServiceImpl
            .listLatestAspects(opContext, entityUrn.getEntityType(), aspectName2, 0, 10)
            .getTotalCount(),
        1);
  }

  @Test
  public void testIngestAspectIfNotPresent() throws AssertionError {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:test1");

    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());

    // Ingest CorpUserInfo Aspect
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    _entityServiceImpl.ingestAspectIfNotPresent(
        opContext,
        entityUrn,
        aspectName,
        writeAspect1,
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata());
    CorpUserInfo writeAspect1a = AspectGenerationUtils.createCorpUserInfo("email_a@test.com");
    _entityServiceImpl.ingestAspectIfNotPresent(
        opContext,
        entityUrn,
        aspectName,
        writeAspect1a,
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata());
    CorpUserInfo writeAspect1b = AspectGenerationUtils.createCorpUserInfo("email_b@test.com");
    _entityServiceImpl.ingestAspectIfNotPresent(
        opContext,
        entityUrn,
        aspectName,
        writeAspect1b,
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata());

    String aspectName2 = AspectGenerationUtils.getAspectName(new Status());
    // Ingest Status Aspect
    Status writeAspect2 = new Status().setRemoved(true);
    _entityServiceImpl.ingestAspectIfNotPresent(
        opContext,
        entityUrn,
        aspectName2,
        writeAspect2,
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata());
    Status writeAspect2a = new Status().setRemoved(false);
    _entityServiceImpl.ingestAspectIfNotPresent(
        opContext,
        entityUrn,
        aspectName2,
        writeAspect2a,
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata());
    Status writeAspect2b = new Status().setRemoved(true);
    _entityServiceImpl.ingestAspectIfNotPresent(
        opContext,
        entityUrn,
        aspectName2,
        writeAspect2b,
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata());

    assertEquals(_entityServiceImpl.getAspect(opContext, entityUrn, aspectName, 0), writeAspect1);
    assertEquals(_entityServiceImpl.getAspect(opContext, entityUrn, aspectName2, 0), writeAspect2);

    assertNull(_entityServiceImpl.getAspect(opContext, entityUrn, aspectName, 1));
    assertNull(_entityServiceImpl.getAspect(opContext, entityUrn, aspectName2, 1));

    assertEquals(
        _entityServiceImpl
            .listLatestAspects(opContext, entityUrn.getEntityType(), aspectName, 0, 10)
            .getTotalCount(),
        1);
    assertEquals(
        _entityServiceImpl
            .listLatestAspects(opContext, entityUrn.getEntityType(), aspectName2, 0, 10)
            .getTotalCount(),
        1);
  }

  /**
   * Equivalence for mocks fails when directly using the object as when converting from
   * RecordTemplate from JSON it reorders the fields. This simulates pulling the historical
   * SystemMetadata from the previous call.
   */
  protected <T extends RecordTemplate> T simulatePullFromDB(T aspect, Class<T> clazz)
      throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    return RecordUtils.toRecordTemplate(clazz, objectMapper.writeValueAsString(aspect));
  }

  @Test
  public void testRestoreIndices() throws Exception {
    if (this instanceof EbeanEntityServiceTest) {
      String urnStr = "urn:li:dataset:(urn:li:dataPlatform:looker,sample_dataset_unique,PROD)";
      Urn entityUrn = UrnUtils.getUrn(urnStr);
      List<Pair<String, RecordTemplate>> pairToIngest = new ArrayList<>();

      final UpstreamLineage upstreamLineage = AspectGenerationUtils.createUpstreamLineage();
      String aspectName1 = AspectGenerationUtils.getAspectName(upstreamLineage);
      pairToIngest.add(getAspectRecordPair(upstreamLineage, UpstreamLineage.class));

      SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata();

      _entityServiceImpl.ingestAspects(
          opContext, entityUrn, pairToIngest, TEST_AUDIT_STAMP, metadata1);

      clearInvocations(_mockProducer);

      RestoreIndicesArgs args = new RestoreIndicesArgs();
      args.aspectName(UPSTREAM_LINEAGE_ASPECT_NAME);
      args.batchSize(1);
      args.start(0);
      args.batchDelayMs(1L);
      args.numThreads(1);
      args.urn(urnStr);
      _entityServiceImpl.restoreIndices(opContext, args, obj -> {});

      ArgumentCaptor<MetadataChangeLog> mclCaptor =
          ArgumentCaptor.forClass(MetadataChangeLog.class);
      verify(_mockProducer, times(1))
          .produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());
      MetadataChangeLog mcl = mclCaptor.getValue();
      assertEquals(mcl.getEntityType(), "dataset");
      assertNull(mcl.getPreviousAspectValue());
      assertNull(mcl.getPreviousSystemMetadata());
      assertEquals(mcl.getChangeType(), ChangeType.RESTATE);
      assertEquals(mcl.getSystemMetadata().getProperties().get(FORCE_INDEXING_KEY), "true");
    }
  }

  @Test
  public void testValidateUrn() throws Exception {
    // Valid URN
    Urn validTestUrn = new Urn("li", "corpuser", new TupleKey("testKey"));
    ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), validTestUrn);

    // URN with trailing whitespace
    Urn testUrnWithTrailingWhitespace = new Urn("li", "corpuser", new TupleKey("testKey   "));
    try {
      ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), testUrnWithTrailingWhitespace);
      Assert.fail("Should have raised IllegalArgumentException for URN with trailing whitespace");
    } catch (IllegalArgumentException e) {
      assertEquals(
          e.getMessage(), "Error: cannot provide an URN with leading or trailing whitespace");
    }

    // Urn purely too long
    String stringTooLong = "a".repeat(510);

    Urn testUrnTooLong = new Urn("li", "corpuser", new TupleKey(stringTooLong));
    try {
      ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), testUrnTooLong);
      Assert.fail("Should have raised IllegalArgumentException for URN too long");
    } catch (IllegalArgumentException e) {
      assertEquals(
          e.getMessage(), "Error: cannot provide an URN longer than 512 bytes (when URL encoded)");
    }

    // Urn too long when URL encoded
    StringBuilder buildStringTooLongWhenEncoded = new StringBuilder();
    StringBuilder buildStringSameLengthWhenEncoded = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      buildStringTooLongWhenEncoded.append('>');
      buildStringSameLengthWhenEncoded.append('a');
    }
    Urn testUrnTooLongWhenEncoded =
        new Urn("li", "corpUser", new TupleKey(buildStringTooLongWhenEncoded.toString()));
    Urn testUrnSameLengthWhenEncoded =
        new Urn("li", "corpUser", new TupleKey(buildStringSameLengthWhenEncoded.toString()));
    // Same length when encoded should be allowed, the encoded one should not be
    ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), testUrnSameLengthWhenEncoded);
    try {
      ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), testUrnTooLongWhenEncoded);
      Assert.fail("Should have raised IllegalArgumentException for URN too long");
    } catch (IllegalArgumentException e) {
      assertEquals(
          e.getMessage(), "Error: cannot provide an URN longer than 512 bytes (when URL encoded)");
    }

    // Urn containing disallowed character
    Urn testUrnSpecialCharValid = new Urn("li", "corpUser", new TupleKey("bob␇"));
    Urn testUrnSpecialCharInvalid = new Urn("li", "corpUser", new TupleKey("bob␟"));
    ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), testUrnSpecialCharValid);
    try {
      ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), testUrnSpecialCharInvalid);
      Assert.fail(
          "Should have raised IllegalArgumentException for URN containing the illegal char");
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Error: URN cannot contain ␟ character");
    }

    Urn urnWithMismatchedParens = new Urn("li", "corpuser", new TupleKey("test(Key"));
    try {
      ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), urnWithMismatchedParens);
      Assert.fail("Should have raised IllegalArgumentException for URN with mismatched parens");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("[test(Key]"));
    }

    Urn invalidType = new Urn("li", "fakeMadeUpType", new TupleKey("testKey"));
    try {
      ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), invalidType);
      Assert.fail(
          "Should have raised IllegalArgumentException for URN with non-existent entity type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Failed to find entity with name fakeMadeUpType"));
    }

    Urn validFabricType =
        new Urn("li", "dataset", new TupleKey("urn:li:dataPlatform:foo", "bar", "PROD"));
    ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), validFabricType);

    Urn invalidFabricType =
        new Urn("li", "dataset", new TupleKey("urn:li:dataPlatform:foo", "bar", "prod"));
    try {
      ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), invalidFabricType);
      Assert.fail("Should have raised IllegalArgumentException for URN with invalid fabric type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(invalidFabricType.toString()));
    }

    Urn urnEndingInComma =
        new Urn("li", "dataset", new TupleKey("urn:li:dataPlatform:foo", "bar", "PROD", ""));
    try {
      ValidationApiUtils.validateUrn(opContext.getEntityRegistry(), urnEndingInComma);
      Assert.fail("Should have raised IllegalArgumentException for URN ending in comma");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(urnEndingInComma.toString()));
    }
  }

  @Test
  public void testUIPreProcessedProposal() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
    EditableDatasetProperties datasetProperties = new EditableDatasetProperties();
    datasetProperties.setDescription("Foo Bar");
    MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityUrn(entityUrn);
    gmce.setChangeType(ChangeType.UPSERT);
    gmce.setEntityType("dataset");
    gmce.setAspectName("editableDatasetProperties");
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    gmce.setSystemMetadata(systemMetadata);
    JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();
    byte[] datasetPropertiesSerialized = dataTemplateCodec.dataTemplateToBytes(datasetProperties);
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(datasetPropertiesSerialized));
    genericAspect.setContentType("application/json");
    gmce.setAspect(genericAspect);
    _entityServiceImpl.ingestProposal(opContext, gmce, TEST_AUDIT_STAMP, false);

    ArgumentCaptor<MetadataChangeLog> captor = ArgumentCaptor.forClass(MetadataChangeLog.class);
    ArgumentCaptor<AspectSpec> aspectSpecCaptor = ArgumentCaptor.forClass(AspectSpec.class);
    verify(_mockProducer, times(4))
        .produceMetadataChangeLog(
            Mockito.eq(entityUrn), aspectSpecCaptor.capture(), captor.capture());
    assertEquals(UI_SOURCE, captor.getValue().getSystemMetadata().getProperties().get(APP_SOURCE));
    assertEquals(
        aspectSpecCaptor.getAllValues().stream()
            .map(AspectSpec::getName)
            .collect(Collectors.toSet()),
        Set.of(
            "browsePathsV2",
            "editableDatasetProperties",
            // "browsePaths",
            "dataPlatformInstance",
            "datasetKey"));
  }

  @Test
  public void testStructuredPropertyIngestProposal() throws Exception {
    String urnStr = "urn:li:dataset:(urn:li:dataPlatform:looker,sample_dataset_unique,PROD)";
    Urn entityUrn = UrnUtils.getUrn(urnStr);

    // Ingest one structured property definition
    String definitionAspectName = "propertyDefinition";
    Urn firstPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:firstStructuredProperty");
    MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityUrn(firstPropertyUrn);
    gmce.setChangeType(ChangeType.UPSERT);
    gmce.setEntityType("structuredProperty");
    gmce.setAspectName(definitionAspectName);
    StructuredPropertyDefinition structuredPropertyDefinition =
        new StructuredPropertyDefinition()
            .setQualifiedName("firstStructuredProperty")
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"))
            .setEntityTypes(new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")));
    JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();
    byte[] definitionSerialized =
        dataTemplateCodec.dataTemplateToBytes(structuredPropertyDefinition);
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(definitionSerialized));
    genericAspect.setContentType("application/json");
    gmce.setAspect(genericAspect);
    _entityServiceImpl.ingestProposal(opContext, gmce, TEST_AUDIT_STAMP, false);

    ArgumentCaptor<MetadataChangeLog> captor = ArgumentCaptor.forClass(MetadataChangeLog.class);
    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(firstPropertyUrn),
            Mockito.eq(structuredPropertiesDefinitionAspect),
            captor.capture());
    assertEquals(
        _entityServiceImpl.getAspect(opContext, firstPropertyUrn, definitionAspectName, 0),
        structuredPropertyDefinition);

    Urn secondPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:secondStructuredProperty");
    assertNull(_entityServiceImpl.getAspect(opContext, secondPropertyUrn, definitionAspectName, 0));
    assertEquals(
        _entityServiceImpl.getAspect(opContext, firstPropertyUrn, definitionAspectName, 0),
        structuredPropertyDefinition);

    Set<StructuredPropertyDefinition> defs;
    try (Stream<EntityAspect> stream =
        _aspectDao.streamAspects(
            STRUCTURED_PROPERTY_ENTITY_NAME, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)) {
      defs =
          stream
              .map(
                  entityAspect ->
                      EntityUtils.toSystemAspect(opContext.getRetrieverContext(), entityAspect)
                          .get()
                          .getAspect(StructuredPropertyDefinition.class))
              .collect(Collectors.toSet());
    }

    assertEquals(defs.size(), 1);
    assertEquals(defs, Set.of(structuredPropertyDefinition));

    SystemEntityClient mockSystemEntityClient = Mockito.mock(SystemEntityClient.class);
    Mockito.when(
            mockSystemEntityClient.getLatestAspectObject(
                any(OperationContext.class),
                eq(firstPropertyUrn),
                eq("propertyDefinition"),
                anyBoolean()))
        .thenReturn(new com.linkedin.entity.Aspect(structuredPropertyDefinition.data()));

    // Add a value for that property
    PrimitivePropertyValueArray propertyValues = new PrimitivePropertyValueArray();
    propertyValues.add(PrimitivePropertyValue.create("hello"));
    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(firstPropertyUrn)
            .setValues(propertyValues);
    StructuredProperties structuredProperties =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));
    MetadataChangeProposal asgnMce = new MetadataChangeProposal();
    asgnMce.setEntityUrn(entityUrn);
    asgnMce.setChangeType(ChangeType.UPSERT);
    asgnMce.setEntityType("dataset");
    asgnMce.setAspectName("structuredProperties");
    JacksonDataTemplateCodec asgnTemplateCodec = new JacksonDataTemplateCodec();
    byte[] asgnSerialized = asgnTemplateCodec.dataTemplateToBytes(structuredProperties);
    GenericAspect asgnGenericAspect = new GenericAspect();
    asgnGenericAspect.setValue(ByteString.unsafeWrap(asgnSerialized));
    asgnGenericAspect.setContentType("application/json");
    asgnMce.setAspect(asgnGenericAspect);
    _entityServiceImpl.ingestProposal(opContext, asgnMce, TEST_AUDIT_STAMP, false);
    assertEquals(
        _entityServiceImpl.getAspect(opContext, entityUrn, "structuredProperties", 0),
        structuredProperties);

    // Ingest second structured property definition
    MetadataChangeProposal gmce2 = new MetadataChangeProposal();
    gmce2.setEntityUrn(secondPropertyUrn);
    gmce2.setChangeType(ChangeType.UPSERT);
    gmce2.setEntityType("structuredProperty");
    gmce2.setAspectName(definitionAspectName);
    StructuredPropertyDefinition secondDefinition =
        new StructuredPropertyDefinition()
            .setQualifiedName("secondStructuredProperty")
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "number"))
            .setEntityTypes(new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")));
    JacksonDataTemplateCodec secondDataTemplate = new JacksonDataTemplateCodec();
    byte[] secondDefinitionSerialized = secondDataTemplate.dataTemplateToBytes(secondDefinition);
    GenericAspect secondGenericAspect = new GenericAspect();
    secondGenericAspect.setValue(ByteString.unsafeWrap(secondDefinitionSerialized));
    secondGenericAspect.setContentType("application/json");
    gmce2.setAspect(secondGenericAspect);
    _entityServiceImpl.ingestProposal(opContext, gmce2, TEST_AUDIT_STAMP, false);
    ArgumentCaptor<MetadataChangeLog> secondCaptor =
        ArgumentCaptor.forClass(MetadataChangeLog.class);
    verify(_mockProducer, times(1))
        .produceMetadataChangeLog(
            Mockito.eq(secondPropertyUrn),
            Mockito.eq(structuredPropertiesDefinitionAspect),
            secondCaptor.capture());
    assertEquals(
        _entityServiceImpl.getAspect(opContext, firstPropertyUrn, definitionAspectName, 0),
        structuredPropertyDefinition);
    assertEquals(
        _entityServiceImpl.getAspect(opContext, secondPropertyUrn, definitionAspectName, 0),
        secondDefinition);
    try (Stream<EntityAspect> stream =
        _aspectDao.streamAspects(
            STRUCTURED_PROPERTY_ENTITY_NAME, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)) {
      defs =
          stream
              .map(
                  entityAspect ->
                      EntityUtils.toSystemAspect(opContext.getRetrieverContext(), entityAspect)
                          .get()
                          .getAspect(StructuredPropertyDefinition.class))
              .collect(Collectors.toSet());
    }

    assertEquals(defs.size(), 2);
    assertEquals(defs, Set.of(secondDefinition, structuredPropertyDefinition));

    Mockito.when(
            mockSystemEntityClient.getLatestAspectObject(
                any(OperationContext.class),
                eq(secondPropertyUrn),
                eq("propertyDefinition"),
                anyBoolean()))
        .thenReturn(new com.linkedin.entity.Aspect(secondDefinition.data()));

    // Get existing value for first structured property
    assertEquals(
        _entityServiceImpl.getAspect(opContext, entityUrn, "structuredProperties", 0),
        structuredProperties);

    // Add a value for second property
    propertyValues = new PrimitivePropertyValueArray();
    propertyValues.add(PrimitivePropertyValue.create(15.0));
    StructuredPropertyValueAssignment secondAssignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(secondPropertyUrn)
            .setValues(propertyValues);
    StructuredProperties secondPropertyArr =
        new StructuredProperties()
            .setProperties(
                new StructuredPropertyValueAssignmentArray(assignment, secondAssignment));
    MetadataChangeProposal asgn2Mce = new MetadataChangeProposal();
    asgn2Mce.setEntityUrn(entityUrn);
    asgn2Mce.setChangeType(ChangeType.UPSERT);
    asgn2Mce.setEntityType("dataset");
    asgn2Mce.setAspectName("structuredProperties");
    JacksonDataTemplateCodec asgnTemplateCodec2 = new JacksonDataTemplateCodec();
    byte[] asgnSerialized2 = asgnTemplateCodec2.dataTemplateToBytes(secondPropertyArr);
    GenericAspect asgnGenericAspect2 = new GenericAspect();
    asgnGenericAspect2.setValue(ByteString.unsafeWrap(asgnSerialized2));
    asgnGenericAspect2.setContentType("application/json");
    asgn2Mce.setAspect(asgnGenericAspect2);
    _entityServiceImpl.ingestProposal(opContext, asgn2Mce, TEST_AUDIT_STAMP, false);
    StructuredProperties expectedProperties =
        new StructuredProperties()
            .setProperties(
                new StructuredPropertyValueAssignmentArray(assignment, secondAssignment));
    assertEquals(
        _entityServiceImpl.getAspect(opContext, entityUrn, "structuredProperties", 0),
        expectedProperties);
  }

  @Test
  public void testCreateChangeTypeProposal() {
    Urn user1 = UrnUtils.getUrn("urn:li:corpuser:test1");
    Urn user2 = UrnUtils.getUrn("urn:li:corpuser:test2");
    Urn entityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:looker,sample_dataset,PROD)");

    MetadataChangeProposal initialCreateProposal = new MetadataChangeProposal();
    initialCreateProposal.setEntityUrn(entityUrn);
    initialCreateProposal.setChangeType(ChangeType.CREATE);
    initialCreateProposal.setEntityType(entityUrn.getEntityType());
    initialCreateProposal.setAspectName(OWNERSHIP_ASPECT_NAME);
    initialCreateProposal.setAspect(
        GenericRecordUtils.serializeAspect(
            new Ownership()
                .setOwners(
                    new OwnerArray(
                        new Owner()
                            .setOwner(user1)
                            .setType(OwnershipType.CUSTOM)
                            .setTypeUrn(DEFAULT_OWNERSHIP_TYPE_URN)))));

    MetadataChangeProposal secondCreateProposal = new MetadataChangeProposal();
    secondCreateProposal.setEntityUrn(entityUrn);
    secondCreateProposal.setChangeType(ChangeType.CREATE);
    secondCreateProposal.setEntityType(entityUrn.getEntityType());
    secondCreateProposal.setAspectName(OWNERSHIP_ASPECT_NAME);
    secondCreateProposal.setAspect(
        GenericRecordUtils.serializeAspect(
            new Ownership()
                .setOwners(
                    new OwnerArray(
                        new Owner()
                            .setOwner(user2)
                            .setType(OwnershipType.CUSTOM)
                            .setTypeUrn(DEFAULT_OWNERSHIP_TYPE_URN)))));

    _entityServiceImpl.ingestProposal(opContext, initialCreateProposal, TEST_AUDIT_STAMP, false);

    // create when entity exists should be denied
    assertThrows(
        ValidationException.class,
        () ->
            _entityServiceImpl.ingestProposal(
                userContext, secondCreateProposal, TEST_AUDIT_STAMP, false));
  }

  @Test
  public void testExists() throws Exception {
    Urn existentUrn = UrnUtils.getUrn("urn:li:corpuser:exists");
    Urn softDeletedUrn = UrnUtils.getUrn("urn:li:corpuser:softDeleted");
    Urn nonExistentUrn = UrnUtils.getUrn("urn:li:corpuser:nonExistent");
    Urn noStatusUrn = UrnUtils.getUrn("urn:li:corpuser:noStatus");

    List<Pair<String, RecordTemplate>> pairToIngest = new ArrayList<>();
    SystemMetadata metadata = AspectGenerationUtils.createSystemMetadata();

    // to ensure existence
    CorpUserInfo userInfoAspect = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    pairToIngest.add(getAspectRecordPair(userInfoAspect, CorpUserInfo.class));

    _entityServiceImpl.ingestAspects(
        opContext, noStatusUrn, pairToIngest, TEST_AUDIT_STAMP, metadata);

    Status statusExistsAspect = new Status().setRemoved(false);
    pairToIngest.add(getAspectRecordPair(statusExistsAspect, Status.class));

    _entityServiceImpl.ingestAspects(
        opContext, existentUrn, pairToIngest, TEST_AUDIT_STAMP, metadata);

    Status statusRemovedAspect = new Status().setRemoved(true);
    pairToIngest.set(1, getAspectRecordPair(statusRemovedAspect, Status.class));

    _entityServiceImpl.ingestAspects(
        opContext, softDeletedUrn, pairToIngest, TEST_AUDIT_STAMP, metadata);

    Set<Urn> inputUrns = Set.of(existentUrn, softDeletedUrn, nonExistentUrn, noStatusUrn);
    assertEquals(
        _entityServiceImpl.exists(opContext, inputUrns, false), Set.of(existentUrn, noStatusUrn));
    assertEquals(
        _entityServiceImpl.exists(opContext, inputUrns, true),
        Set.of(existentUrn, noStatusUrn, softDeletedUrn));
  }

  @Test
  public void testBatchDuplicate() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:batchDuplicateTest");
    SystemMetadata systemMetadata = AspectGenerationUtils.createSystemMetadata();
    ChangeItemImpl item1 =
        ChangeItemImpl.builder()
            .urn(entityUrn)
            .aspectName(STATUS_ASPECT_NAME)
            .recordTemplate(new Status().setRemoved(true))
            .systemMetadata(systemMetadata.copy())
            .auditStamp(TEST_AUDIT_STAMP)
            .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));
    ChangeItemImpl item2 =
        ChangeItemImpl.builder()
            .urn(entityUrn)
            .aspectName(STATUS_ASPECT_NAME)
            .recordTemplate(new Status().setRemoved(false))
            .systemMetadata(systemMetadata.copy())
            .auditStamp(TEST_AUDIT_STAMP)
            .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(List.of(item1, item2))
            .build(),
        false,
        true);

    // List aspects urns
    ListUrnsResult batch = _entityServiceImpl.listUrns(opContext, entityUrn.getEntityType(), 0, 2);

    assertEquals(batch.getStart().intValue(), 0);
    assertEquals(batch.getCount().intValue(), 1);
    assertEquals(batch.getTotal().intValue(), 1);
    assertEquals(batch.getEntities().size(), 1);
    assertEquals(entityUrn.toString(), batch.getEntities().get(0).toString());

    EnvelopedAspect envelopedAspect =
        _entityServiceImpl.getLatestEnvelopedAspect(
            opContext, CORP_USER_ENTITY_NAME, entityUrn, STATUS_ASPECT_NAME);
    assertEquals(
        envelopedAspect.getSystemMetadata().getVersion(),
        "2",
        "Expected version 2 after accounting for sequential duplicates");
    assertEquals(
        envelopedAspect.getValue().toString(),
        "{removed=false}",
        "Expected 2nd item to be the latest");
  }

  @Test
  public void testBatchPatchWithTrailingNoOp() throws Exception {
    Urn entityUrn =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,testBatchPatchWithTrailingNoOp,PROD)");
    TagUrn tag1 = TagUrn.createFromString("urn:li:tag:tag1");
    Urn tag2 = UrnUtils.getUrn("urn:li:tag:tag2");
    Urn tagOther = UrnUtils.getUrn("urn:li:tag:other");

    SystemMetadata systemMetadata = AspectGenerationUtils.createSystemMetadata();

    ChangeItemImpl initialAspectTag1 =
        ChangeItemImpl.builder()
            .urn(entityUrn)
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .recordTemplate(
                new GlobalTags()
                    .setTags(new TagAssociationArray(new TagAssociation().setTag(tag1))))
            .systemMetadata(systemMetadata.copy())
            .auditStamp(TEST_AUDIT_STAMP)
            .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));

    PatchItemImpl patchAdd2 =
        PatchItemImpl.builder()
            .urn(entityUrn)
            .entitySpec(_testEntityRegistry.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .aspectSpec(
                _testEntityRegistry
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .patch(
                GenericJsonPatch.builder()
                    .arrayPrimaryKeys(Map.of("properties", List.of("tag")))
                    .patch(List.of(tagPatchOp(PatchOperationType.ADD, tag2)))
                    .build()
                    .getJsonPatch())
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(_testEntityRegistry);

    PatchItemImpl patchRemoveNonExistent =
        PatchItemImpl.builder()
            .urn(entityUrn)
            .entitySpec(_testEntityRegistry.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .aspectSpec(
                _testEntityRegistry
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .patch(
                GenericJsonPatch.builder()
                    .arrayPrimaryKeys(Map.of("properties", List.of("tag")))
                    .patch(List.of(tagPatchOp(PatchOperationType.REMOVE, tagOther)))
                    .build()
                    .getJsonPatch())
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(_testEntityRegistry);

    // establish base entity
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(List.of(initialAspectTag1))
            .build(),
        false,
        true);

    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(List.of(patchAdd2, patchRemoveNonExistent))
            .build(),
        false,
        true);

    // List aspects urns
    ListUrnsResult batch = _entityServiceImpl.listUrns(opContext, entityUrn.getEntityType(), 0, 1);

    assertEquals(batch.getStart().intValue(), 0);
    assertEquals(batch.getCount().intValue(), 1);
    assertEquals(batch.getTotal().intValue(), 1);
    assertEquals(batch.getEntities().size(), 1);
    assertEquals(entityUrn.toString(), batch.getEntities().get(0).toString());

    EnvelopedAspect envelopedAspect =
        _entityServiceImpl.getLatestEnvelopedAspect(
            opContext, DATASET_ENTITY_NAME, entityUrn, GLOBAL_TAGS_ASPECT_NAME);
    assertEquals(
        envelopedAspect.getSystemMetadata().getVersion(),
        "2",
        "Expected version 3. 1 - Initial, + 1 add, 1 remove");
    assertEquals(
        new GlobalTags(envelopedAspect.getValue().data())
            .getTags().stream().map(TagAssociation::getTag).collect(Collectors.toSet()),
        Set.of(tag1, tag2),
        "Expected both tags");
  }

  @Test
  public void testBatchPatchAdd() throws Exception {
    Urn entityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,testBatchPatchAdd,PROD)");
    TagUrn tag1 = TagUrn.createFromString("urn:li:tag:tag1");
    TagUrn tag2 = TagUrn.createFromString("urn:li:tag:tag2");
    TagUrn tag3 = TagUrn.createFromString("urn:li:tag:tag3");

    SystemMetadata systemMetadata = AspectGenerationUtils.createSystemMetadata();

    ChangeItemImpl initialAspectTag1 =
        ChangeItemImpl.builder()
            .urn(entityUrn)
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .recordTemplate(
                new GlobalTags()
                    .setTags(new TagAssociationArray(new TagAssociation().setTag(tag1))))
            .systemMetadata(systemMetadata.copy())
            .auditStamp(TEST_AUDIT_STAMP)
            .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));

    PatchItemImpl patchAdd3 =
        PatchItemImpl.builder()
            .urn(entityUrn)
            .entitySpec(_testEntityRegistry.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .aspectSpec(
                _testEntityRegistry
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .patch(
                GenericJsonPatch.builder()
                    .arrayPrimaryKeys(Map.of("properties", List.of("tag")))
                    .patch(List.of(tagPatchOp(PatchOperationType.ADD, tag3)))
                    .build()
                    .getJsonPatch())
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(_testEntityRegistry);

    PatchItemImpl patchAdd2 =
        PatchItemImpl.builder()
            .urn(entityUrn)
            .entitySpec(_testEntityRegistry.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .aspectSpec(
                _testEntityRegistry
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .patch(
                GenericJsonPatch.builder()
                    .arrayPrimaryKeys(Map.of("properties", List.of("tag")))
                    .patch(List.of(tagPatchOp(PatchOperationType.ADD, tag2)))
                    .build()
                    .getJsonPatch())
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(_testEntityRegistry);

    PatchItemImpl patchAdd1 =
        PatchItemImpl.builder()
            .urn(entityUrn)
            .entitySpec(_testEntityRegistry.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .aspectSpec(
                _testEntityRegistry
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .patch(
                GenericJsonPatch.builder()
                    .arrayPrimaryKeys(Map.of("properties", List.of("tag")))
                    .patch(List.of(tagPatchOp(PatchOperationType.ADD, tag1)))
                    .build()
                    .getJsonPatch())
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(_testEntityRegistry);

    // establish base entity
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(List.of(initialAspectTag1))
            .build(),
        false,
        true);

    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(List.of(patchAdd3, patchAdd2, patchAdd1))
            .build(),
        false,
        true);

    // List aspects urns
    ListUrnsResult batch = _entityServiceImpl.listUrns(opContext, entityUrn.getEntityType(), 0, 1);

    assertEquals(batch.getStart().intValue(), 0);
    assertEquals(batch.getCount().intValue(), 1);
    assertEquals(batch.getTotal().intValue(), 1);
    assertEquals(batch.getEntities().size(), 1);
    assertEquals(entityUrn.toString(), batch.getEntities().get(0).toString());

    EnvelopedAspect envelopedAspect =
        _entityServiceImpl.getLatestEnvelopedAspect(
            opContext, DATASET_ENTITY_NAME, entityUrn, GLOBAL_TAGS_ASPECT_NAME);
    assertEquals(envelopedAspect.getSystemMetadata().getVersion(), "3", "Expected version 4");
    assertEquals(
        new GlobalTags(envelopedAspect.getValue().data())
            .getTags().stream().map(TagAssociation::getTag).collect(Collectors.toSet()),
        Set.of(tag1, tag2, tag3),
        "Expected all tags");
  }

  @Test
  public void testBatchPatchAddDuplicate() throws Exception {
    Urn entityUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,testBatchPatchAdd,PROD)");
    List<TagAssociation> initialTags =
        List.of(
                TagUrn.createFromString("urn:li:tag:__default_large_table"),
                TagUrn.createFromString("urn:li:tag:__default_low_queries"),
                TagUrn.createFromString("urn:li:tag:__default_low_changes"),
                TagUrn.createFromString("urn:li:tag:!10TB+ tables"))
            .stream()
            .map(tag -> new TagAssociation().setTag(tag))
            .collect(Collectors.toList());
    TagUrn tag2 = TagUrn.createFromString("urn:li:tag:$ 1TB+");

    SystemMetadata systemMetadata = AspectGenerationUtils.createSystemMetadata();

    SystemMetadata patchSystemMetadata = new SystemMetadata();
    patchSystemMetadata.setLastObserved(systemMetadata.getLastObserved() + 1);
    patchSystemMetadata.setProperties(new StringMap(Map.of(APP_SOURCE, METADATA_TESTS_SOURCE)));

    ChangeItemImpl initialAspectTag1 =
        ChangeItemImpl.builder()
            .urn(entityUrn)
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .recordTemplate(new GlobalTags().setTags(new TagAssociationArray(initialTags)))
            .systemMetadata(systemMetadata.copy())
            .auditStamp(TEST_AUDIT_STAMP)
            .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));

    PatchItemImpl patchAdd2 =
        PatchItemImpl.builder()
            .urn(entityUrn)
            .entitySpec(_testEntityRegistry.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .aspectSpec(
                _testEntityRegistry
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .patch(
                GenericJsonPatch.builder()
                    .arrayPrimaryKeys(Map.of("properties", List.of("tag")))
                    .patch(List.of(tagPatchOp(PatchOperationType.ADD, tag2)))
                    .build()
                    .getJsonPatch())
            .systemMetadata(patchSystemMetadata)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(_testEntityRegistry);

    // establish base entity
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(List.of(initialAspectTag1))
            .build(),
        false,
        true);

    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(List.of(patchAdd2, patchAdd2)) // duplicate
            .build(),
        false,
        true);

    // List aspects urns
    ListUrnsResult batch = _entityServiceImpl.listUrns(opContext, entityUrn.getEntityType(), 0, 1);

    assertEquals(batch.getStart().intValue(), 0);
    assertEquals(batch.getCount().intValue(), 1);
    assertEquals(batch.getTotal().intValue(), 1);
    assertEquals(batch.getEntities().size(), 1);
    assertEquals(entityUrn.toString(), batch.getEntities().get(0).toString());

    EnvelopedAspect envelopedAspect =
        _entityServiceImpl.getLatestEnvelopedAspect(
            opContext, DATASET_ENTITY_NAME, entityUrn, GLOBAL_TAGS_ASPECT_NAME);
    assertEquals(envelopedAspect.getSystemMetadata().getVersion(), "2", "Expected version 2");
    assertEquals(
        new GlobalTags(envelopedAspect.getValue().data())
            .getTags().stream().map(TagAssociation::getTag).collect(Collectors.toSet()),
        Stream.concat(initialTags.stream().map(TagAssociation::getTag), Stream.of(tag2))
            .collect(Collectors.toSet()),
        "Expected all tags");
  }

  @Test
  public void testPatchRemoveNonExistent() throws Exception {
    Urn entityUrn =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,testPatchRemoveNonExistent,PROD)");
    TagUrn tag1 = TagUrn.createFromString("urn:li:tag:tag1");

    PatchItemImpl patchRemove =
        PatchItemImpl.builder()
            .urn(entityUrn)
            .entitySpec(_testEntityRegistry.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .aspectSpec(
                _testEntityRegistry
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .patch(
                GenericJsonPatch.builder()
                    .arrayPrimaryKeys(Map.of("properties", List.of("tag")))
                    .patch(List.of(tagPatchOp(PatchOperationType.REMOVE, tag1)))
                    .build()
                    .getJsonPatch())
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(_testEntityRegistry);

    List<UpdateAspectResult> results =
        _entityServiceImpl.ingestAspects(
            opContext,
            AspectsBatchImpl.builder()
                .retrieverContext(opContext.getRetrieverContext())
                .items(List.of(patchRemove))
                .build(),
            false,
            true);

    assertEquals(results.size(), 4, "Expected default aspects + empty globalTags");

    // List aspects urns
    ListUrnsResult batch = _entityServiceImpl.listUrns(opContext, entityUrn.getEntityType(), 0, 1);

    assertEquals(batch.getStart().intValue(), 0);
    assertEquals(batch.getCount().intValue(), 1);
    assertEquals(batch.getTotal().intValue(), 1);
    assertEquals(batch.getEntities().size(), 1);
    assertEquals(entityUrn.toString(), batch.getEntities().get(0).toString());

    EnvelopedAspect envelopedAspect =
        _entityServiceImpl.getLatestEnvelopedAspect(
            opContext, DATASET_ENTITY_NAME, entityUrn, GLOBAL_TAGS_ASPECT_NAME);
    assertEquals(envelopedAspect.getSystemMetadata().getVersion(), "1", "Expected version 4");
    assertEquals(
        new GlobalTags(envelopedAspect.getValue().data())
            .getTags().stream().map(TagAssociation::getTag).collect(Collectors.toSet()),
        Set.of(),
        "Expected empty tags");
  }

  @Test
  public void testPatchAddNonExistent() throws Exception {
    Urn entityUrn =
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,testPatchAddNonExistent,PROD)");
    TagUrn tag1 = TagUrn.createFromString("urn:li:tag:tag1");

    PatchItemImpl patchAdd =
        PatchItemImpl.builder()
            .urn(entityUrn)
            .entitySpec(_testEntityRegistry.getEntitySpec(DATASET_ENTITY_NAME))
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .aspectSpec(
                _testEntityRegistry
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .patch(
                GenericJsonPatch.builder()
                    .arrayPrimaryKeys(Map.of("properties", List.of("tag")))
                    .patch(List.of(tagPatchOp(PatchOperationType.ADD, tag1)))
                    .build()
                    .getJsonPatch())
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(_testEntityRegistry);

    List<UpdateAspectResult> results =
        _entityServiceImpl.ingestAspects(
            opContext,
            AspectsBatchImpl.builder()
                .retrieverContext(opContext.getRetrieverContext())
                .items(List.of(patchAdd))
                .build(),
            false,
            true);

    assertEquals(results.size(), 4, "Expected default aspects + globalTags");

    // List aspects urns
    ListUrnsResult batch = _entityServiceImpl.listUrns(opContext, entityUrn.getEntityType(), 0, 1);

    assertEquals(batch.getStart().intValue(), 0);
    assertEquals(batch.getCount().intValue(), 1);
    assertEquals(batch.getTotal().intValue(), 1);
    assertEquals(batch.getEntities().size(), 1);
    assertEquals(entityUrn.toString(), batch.getEntities().get(0).toString());

    EnvelopedAspect envelopedAspect =
        _entityServiceImpl.getLatestEnvelopedAspect(
            opContext, DATASET_ENTITY_NAME, entityUrn, GLOBAL_TAGS_ASPECT_NAME);
    assertEquals(envelopedAspect.getSystemMetadata().getVersion(), "1", "Expected version 4");
    assertEquals(
        new GlobalTags(envelopedAspect.getValue().data())
            .getTags().stream().map(TagAssociation::getTag).collect(Collectors.toSet()),
        Set.of(tag1),
        "Expected all tags");
  }

  @Test
  public void testDeleteUrnWithRunIdFilterNonMatch() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:deleteWithFilterNonMatch");

    // Create aspects with different run IDs
    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata();
    metadata1.setRunId("run-123");

    SystemMetadata metadata2 = AspectGenerationUtils.createSystemMetadata();
    metadata2.setRunId("run-456"); // Different run ID

    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());

    // First ingest the aspect that should survive (run-456)
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("first@test.com");
    List<Pair<String, RecordTemplate>> firstPair = new ArrayList<>();
    firstPair.add(getAspectRecordPair(writeAspect1, CorpUserInfo.class));
    _entityServiceImpl.ingestAspects(opContext, entityUrn, firstPair, TEST_AUDIT_STAMP, metadata2);

    // Then ingest the aspect that should be deleted (run-123)
    CorpUserInfo writeAspect2 = AspectGenerationUtils.createCorpUserInfo("second@test.com");
    List<Pair<String, RecordTemplate>> secondPair = new ArrayList<>();
    secondPair.add(getAspectRecordPair(writeAspect2, CorpUserInfo.class));
    _entityServiceImpl.ingestAspects(opContext, entityUrn, secondPair, TEST_AUDIT_STAMP, metadata1);

    // When we try to delete with runId=run-123, the version with runId=run-456 should survive
    RollbackResult result =
        _entityServiceImpl.deleteAspectWithoutMCL(
            opContext,
            entityUrn.toString(),
            aspectName,
            Collections.singletonMap("runId", "run-123"),
            true);

    // The aspect with run-456 should still exist
    RecordTemplate survivingAspect =
        _entityServiceImpl.getLatestAspect(opContext, entityUrn, aspectName);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, survivingAspect));

    // Verify the RollbackResult details
    assertNotNull(result);
    assertEquals(result.getUrn(), entityUrn);
    assertEquals(result.getEntityName(), "corpuser");
    assertEquals(result.getAspectName(), aspectName);
  }

  @Test
  public void testDeleteUrnWithRunIdFilterNonMatchVersionGap() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:deleteWithFilterNonMatch");
    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());

    // Metadata that should be preserved (run-456)
    SystemMetadata metadata456 = AspectGenerationUtils.createSystemMetadata();
    metadata456.setRunId("run-456"); // Different run ID
    CorpUserInfo writeAspect456 = AspectGenerationUtils.createCorpUserInfo("first@test.com");
    List<Pair<String, RecordTemplate>> firstPair = new ArrayList<>();
    firstPair.add(getAspectRecordPair(writeAspect456, CorpUserInfo.class));
    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, firstPair, TEST_AUDIT_STAMP, metadata456);

    // Metadata that should be deleted (run-123)
    SystemMetadata metadata123 = AspectGenerationUtils.createSystemMetadata();
    metadata123.setRunId("run-123");
    CorpUserInfo writeAspect123 = AspectGenerationUtils.createCorpUserInfo("second@test.com");
    List<Pair<String, RecordTemplate>> secondPair = new ArrayList<>();
    secondPair.add(getAspectRecordPair(writeAspect123, CorpUserInfo.class));
    _entityServiceImpl.ingestAspects(
        opContext, entityUrn, secondPair, TEST_AUDIT_STAMP, metadata123);

    // Then insert another run-123 with version gap
    _aspectDao.saveAspect(
        null,
        entityUrn.toString(),
        aspectName,
        RecordUtils.toJsonString(writeAspect123),
        TEST_AUDIT_STAMP.getActor().toString(),
        null,
        Timestamp.from(Instant.ofEpochMilli(TEST_AUDIT_STAMP.getTime())),
        RecordUtils.toJsonString(metadata123),
        10L,
        true);

    // When we try to delete with runId=run-123, the version with runId=run-456 should survive
    RollbackResult result =
        _entityServiceImpl.deleteAspectWithoutMCL(
            opContext,
            entityUrn.toString(),
            aspectName,
            Collections.singletonMap("runId", "run-123"),
            true);

    // The aspect with run-456 should still exist
    RecordTemplate survivingAspect =
        _entityServiceImpl.getLatestAspect(opContext, entityUrn, aspectName);
    assertTrue(DataTemplateUtil.areEqual(writeAspect456, survivingAspect));

    // Verify the RollbackResult details
    assertNotNull(result);
    assertEquals(result.getUrn(), entityUrn);
    assertEquals(result.getEntityName(), "corpuser");
    assertEquals(result.getAspectName(), aspectName);
  }

  @Nonnull
  protected com.linkedin.entity.Entity createCorpUserEntity(Urn entityUrn, String email)
      throws Exception {
    CorpuserUrn corpuserUrn = CorpuserUrn.createFromUrn(entityUrn);
    com.linkedin.entity.Entity entity = new com.linkedin.entity.Entity();
    Snapshot snapshot = new Snapshot();
    CorpUserSnapshot corpUserSnapshot = new CorpUserSnapshot();
    List<CorpUserAspect> userAspects = new ArrayList<>();
    userAspects.add(CorpUserAspect.create(AspectGenerationUtils.createCorpUserInfo(email)));
    corpUserSnapshot.setAspects(new CorpUserAspectArray(userAspects));
    corpUserSnapshot.setUrn(corpuserUrn);
    snapshot.setCorpUserSnapshot(corpUserSnapshot);
    entity.setValue(snapshot);
    return entity;
  }

  protected <T extends RecordTemplate> Pair<String, RecordTemplate> getAspectRecordPair(
      T aspect, Class<T> clazz) throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    RecordTemplate recordTemplate =
        RecordUtils.toRecordTemplate(clazz, objectMapper.writeValueAsString(aspect));
    return new Pair<>(AspectGenerationUtils.getAspectName(aspect), recordTemplate);
  }

  private static GenericJsonPatch.PatchOp tagPatchOp(PatchOperationType op, Urn tagUrn) {
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(op.getValue());
    patchOp.setPath(String.format("/tags/%s", tagUrn));
    if (PatchOperationType.ADD.equals(op)) {
      patchOp.setValue(Map.of("tag", tagUrn.toString()));
    }
    return patchOp;
  }
}
