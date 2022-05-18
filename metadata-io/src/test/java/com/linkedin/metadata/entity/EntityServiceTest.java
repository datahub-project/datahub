package com.linkedin.metadata.entity;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.VersionBasedRetention;
import com.linkedin.util.Pair;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * A class to test {@link EntityService}
 *
 * This class is generic to allow same integration tests to be reused to test all supported storage backends.
 * If you're adding another storage backend - you should create a new test class that extends this one providing
 * hard implementations of {@link AspectDao} and {@link RetentionService} and implements {@code @BeforeMethod} etc
 * to set up and tear down state.
 *
 * If you realise that a feature you want to test, sadly, has divergent behaviours between different storage implementations,
 * that you can't rectify - you should make the test method abstract and implement it in all implementations of this class.
 *
 * @param <T_AD> {@link AspectDao} implementation.
 * @param <T_RS> {@link RetentionService} implementation.
 */
abstract public class EntityServiceTest<T_AD extends AspectDao, T_RS extends RetentionService> {

    protected EntityService _entityService;
    protected T_AD _aspectDao;
    protected T_RS _retentionService;

    protected static final AuditStamp TEST_AUDIT_STAMP = createTestAuditStamp();
    protected final EntityRegistry _snapshotEntityRegistry = new TestEntityRegistry();
    protected final EntityRegistry _configEntityRegistry =
        new ConfigEntityRegistry(Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    protected final EntityRegistry _testEntityRegistry =
        new MergedEntityRegistry(_snapshotEntityRegistry).apply(_configEntityRegistry);
    protected EventProducer _mockProducer;

    protected EntityServiceTest() throws EntityRegistryException {
    }

    // This test had to be split out because Cassandra relational databases have different result ordering restrictions
    @Test
    abstract public void testIngestListLatestAspects() throws Exception;

    // This test had to be split out because Cassandra relational databases have different result ordering restrictions
    @Test
    abstract public void testIngestListUrns() throws Exception;

    // This test had to be split out because Cassandra doesn't support nested transactions
    @Test
    abstract public void testNestedTransactions() throws Exception;

    @Test
    public void testIngestGetEntity() throws Exception {
        // Test Writing a CorpUser Entity
        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");
        com.linkedin.entity.Entity writeEntity = createCorpUserEntity(entityUrn, "tester@test.com");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        // 1. Ingest Entity
        _entityService.ingestEntity(writeEntity, TEST_AUDIT_STAMP, metadata1);

        // 2. Retrieve Entity
        com.linkedin.entity.Entity readEntity = _entityService.getEntity(entityUrn, Collections.emptySet());

        // 3. Compare Entity Objects
        assertEquals(readEntity.getValue().getCorpUserSnapshot().getAspects().size(), 2); // Key + Info aspect.
        assertTrue(DataTemplateUtil.areEqual(writeEntity.getValue().getCorpUserSnapshot().getAspects().get(0),
            readEntity.getValue().getCorpUserSnapshot().getAspects().get(1)));
        CorpUserKey expectedKey = new CorpUserKey();
        expectedKey.setUsername("test");
        assertTrue(DataTemplateUtil.areEqual(expectedKey,
            readEntity.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserKey())); // Key + Info aspect.

        ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());
        MetadataChangeLog mcl = mclCaptor.getValue();
        assertEquals(mcl.getEntityType(), "corpuser");
        assertNull(mcl.getPreviousAspectValue());
        assertNull(mcl.getPreviousSystemMetadata());
        assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

        verify(_mockProducer, times(2)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testAddKey() throws Exception {
        // Test Writing a CorpUser Key
        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");
        com.linkedin.entity.Entity writeEntity = createCorpUserEntity(entityUrn, "tester@test.com");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        // 1. Ingest Entity
        _entityService.ingestEntity(writeEntity, TEST_AUDIT_STAMP, metadata1);

        // 2. Retrieve Entity
        com.linkedin.entity.Entity readEntity = _entityService.getEntity(entityUrn, Collections.emptySet());

        // 3. Compare Entity Objects
        assertEquals(readEntity.getValue().getCorpUserSnapshot().getAspects().size(), 2); // Key + Info aspect.
        assertTrue(DataTemplateUtil.areEqual(writeEntity.getValue().getCorpUserSnapshot().getAspects().get(0),
            readEntity.getValue().getCorpUserSnapshot().getAspects().get(1)));
        CorpUserKey expectedKey = new CorpUserKey();
        expectedKey.setUsername("test");
        assertTrue(DataTemplateUtil.areEqual(expectedKey,
            readEntity.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserKey())); // Key + Info aspect.

        ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());
        MetadataChangeLog mcl = mclCaptor.getValue();
        assertEquals(mcl.getEntityType(), "corpuser");
        assertNull(mcl.getPreviousAspectValue());
        assertNull(mcl.getPreviousSystemMetadata());
        assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

        verify(_mockProducer, times(2)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testIngestGetEntities() throws Exception {
        // Test Writing a CorpUser Entity
        Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:tester1");
        com.linkedin.entity.Entity writeEntity1 = createCorpUserEntity(entityUrn1, "tester@test.com");

        Urn entityUrn2 = Urn.createFromString("urn:li:corpuser:tester2");
        com.linkedin.entity.Entity writeEntity2 = createCorpUserEntity(entityUrn2, "tester2@test.com");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        SystemMetadata metadata2 = new SystemMetadata();
        metadata2.setLastObserved(1625792690);
        metadata2.setRunId("run-123");

        // 1. Ingest Entities
        _entityService.ingestEntities(ImmutableList.of(writeEntity1, writeEntity2), TEST_AUDIT_STAMP,
            ImmutableList.of(metadata1, metadata2));

        // 2. Retrieve Entities
        Map<Urn, Entity> readEntities =
            _entityService.getEntities(ImmutableSet.of(entityUrn1, entityUrn2), Collections.emptySet());

        // 3. Compare Entity Objects

        // Entity 1
        com.linkedin.entity.Entity readEntity1 = readEntities.get(entityUrn1);
        assertEquals(readEntity1.getValue().getCorpUserSnapshot().getAspects().size(), 2); // Key + Info aspect.
        assertTrue(DataTemplateUtil.areEqual(writeEntity1.getValue().getCorpUserSnapshot().getAspects().get(0),
            readEntity1.getValue().getCorpUserSnapshot().getAspects().get(1)));
        CorpUserKey expectedKey1 = new CorpUserKey();
        expectedKey1.setUsername("tester1");
        assertTrue(DataTemplateUtil.areEqual(expectedKey1,
            readEntity1.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserKey())); // Key + Info aspect.

        // Entity 2
        com.linkedin.entity.Entity readEntity2 = readEntities.get(entityUrn2);
        assertEquals(readEntity2.getValue().getCorpUserSnapshot().getAspects().size(), 2); // Key + Info aspect.
        Optional<CorpUserAspect> writer2UserInfo = writeEntity2.getValue().getCorpUserSnapshot().getAspects()
            .stream().filter(CorpUserAspect::isCorpUserInfo).findAny();
        Optional<CorpUserAspect> reader2UserInfo = writeEntity2.getValue().getCorpUserSnapshot().getAspects()
            .stream().filter(CorpUserAspect::isCorpUserInfo).findAny();

        assertTrue(writer2UserInfo.isPresent(), "Writer2 user info exists");
        assertTrue(reader2UserInfo.isPresent(), "Reader2 user info exists");
        assertTrue(DataTemplateUtil.areEqual(writer2UserInfo.get(),  reader2UserInfo.get()), "UserInfo's are the same");
        CorpUserKey expectedKey2 = new CorpUserKey();
        expectedKey2.setUsername("tester2");
        assertTrue(DataTemplateUtil.areEqual(expectedKey2,
            readEntity2.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserKey())); // Key + Info aspect.

        ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn1), Mockito.any(),
            mclCaptor.capture());
        MetadataChangeLog mcl = mclCaptor.getValue();
        assertEquals(mcl.getEntityType(), "corpuser");
        assertNull(mcl.getPreviousAspectValue());
        assertNull(mcl.getPreviousSystemMetadata());
        assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn2), Mockito.any(),
            mclCaptor.capture());
        mcl = mclCaptor.getValue();
        assertEquals(mcl.getEntityType(), "corpuser");
        assertNull(mcl.getPreviousAspectValue());
        assertNull(mcl.getPreviousSystemMetadata());
        assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

        verify(_mockProducer, times(2)).produceMetadataAuditEvent(Mockito.eq(entityUrn1), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verify(_mockProducer, times(2)).produceMetadataAuditEvent(Mockito.eq(entityUrn2), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testIngestGetEntitiesV2() throws Exception {
        // Test Writing a CorpUser Entity
        Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:tester1");
        com.linkedin.entity.Entity writeEntity1 = createCorpUserEntity(entityUrn1, "tester@test.com");

        Urn entityUrn2 = Urn.createFromString("urn:li:corpuser:tester2");
        com.linkedin.entity.Entity writeEntity2 = createCorpUserEntity(entityUrn2, "tester2@test.com");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        SystemMetadata metadata2 = new SystemMetadata();
        metadata2.setLastObserved(1625792690);
        metadata2.setRunId("run-123");

        String aspectName = "corpUserInfo";
        String keyName = "corpUserKey";

        // 1. Ingest Entities
        _entityService.ingestEntities(ImmutableList.of(writeEntity1, writeEntity2), TEST_AUDIT_STAMP,
            ImmutableList.of(metadata1, metadata2));

        // 2. Retrieve Entities
        Map<Urn, EntityResponse> readEntities =
            _entityService.getEntitiesV2("corpuser", ImmutableSet.of(entityUrn1, entityUrn2), ImmutableSet.of(aspectName));

        // 3. Compare Entity Objects

        // Entity 1
        EntityResponse readEntityResponse1 = readEntities.get(entityUrn1);
        assertEquals(readEntityResponse1.getAspects().size(), 2); // Key + Info aspect.
        EnvelopedAspect envelopedAspect1 = readEntityResponse1.getAspects().get(aspectName);
        assertEquals(envelopedAspect1.getName(), aspectName);
        assertTrue(
            DataTemplateUtil.areEqual(writeEntity1.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserInfo(),
                new CorpUserInfo(envelopedAspect1.getValue().data())));
        CorpUserKey expectedKey1 = new CorpUserKey();
        expectedKey1.setUsername("tester1");
        EnvelopedAspect envelopedKey1 = readEntityResponse1.getAspects().get(keyName);
        assertTrue(DataTemplateUtil.areEqual(expectedKey1, new CorpUserKey(envelopedKey1.getValue().data())));

        // Entity 2
        EntityResponse readEntityResponse2 = readEntities.get(entityUrn2);
        assertEquals(readEntityResponse2.getAspects().size(), 2); // Key + Info aspect.
        EnvelopedAspect envelopedAspect2 = readEntityResponse2.getAspects().get(aspectName);
        assertEquals(envelopedAspect2.getName(), aspectName);
        assertTrue(
            DataTemplateUtil.areEqual(writeEntity2.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserInfo(),
                new CorpUserInfo(envelopedAspect2.getValue().data())));
        CorpUserKey expectedKey2 = new CorpUserKey();
        expectedKey2.setUsername("tester2");
        EnvelopedAspect envelopedKey2 = readEntityResponse2.getAspects().get(keyName);
        assertTrue(DataTemplateUtil.areEqual(expectedKey2, new CorpUserKey(envelopedKey2.getValue().data())));

        verify(_mockProducer, times(2)).produceMetadataAuditEvent(Mockito.eq(entityUrn1), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verify(_mockProducer, times(2)).produceMetadataAuditEvent(Mockito.eq(entityUrn2), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn1),
            Mockito.any(), Mockito.any());

        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn2),
            Mockito.any(), Mockito.any());

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testIngestGetEntitiesVersionedV2() throws Exception {
        // Test Writing a CorpUser Entity
        Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:tester1");
        VersionedUrn versionedUrn1 = new VersionedUrn().setUrn(entityUrn1).setVersionStamp("corpUserInfo:0");
        com.linkedin.entity.Entity writeEntity1 = createCorpUserEntity(entityUrn1, "tester@test.com");

        Urn entityUrn2 = Urn.createFromString("urn:li:corpuser:tester2");
        VersionedUrn versionedUrn2 = new VersionedUrn().setUrn(entityUrn2);
        com.linkedin.entity.Entity writeEntity2 = createCorpUserEntity(entityUrn2, "tester2@test.com");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        SystemMetadata metadata2 = new SystemMetadata();
        metadata2.setLastObserved(1625792690);
        metadata2.setRunId("run-123");

        String aspectName = "corpUserInfo";
        String keyName = "corpUserKey";

        // 1. Ingest Entities
        _entityService.ingestEntities(ImmutableList.of(writeEntity1, writeEntity2), TEST_AUDIT_STAMP,
            ImmutableList.of(metadata1, metadata2));

        // 2. Retrieve Entities
        Map<Urn, EntityResponse> readEntities =
            _entityService.getEntitiesVersionedV2(ImmutableSet.of(versionedUrn1, versionedUrn2), ImmutableSet.of(aspectName));

        // 3. Compare Entity Objects

        // Entity 1
        EntityResponse readEntityResponse1 = readEntities.get(entityUrn1);
        assertEquals(2, readEntityResponse1.getAspects().size()); // Key + Info aspect.
        EnvelopedAspect envelopedAspect1 = readEntityResponse1.getAspects().get(aspectName);
        assertEquals(envelopedAspect1.getName(), aspectName);
        assertTrue(
            DataTemplateUtil.areEqual(writeEntity1.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserInfo(),
                new CorpUserInfo(envelopedAspect1.getValue().data())));
        CorpUserKey expectedKey1 = new CorpUserKey();
        expectedKey1.setUsername("tester1");
        EnvelopedAspect envelopedKey1 = readEntityResponse1.getAspects().get(keyName);
        assertTrue(DataTemplateUtil.areEqual(expectedKey1, new CorpUserKey(envelopedKey1.getValue().data())));

        // Entity 2
        EntityResponse readEntityResponse2 = readEntities.get(entityUrn2);
        assertEquals(2, readEntityResponse2.getAspects().size()); // Key + Info aspect.
        EnvelopedAspect envelopedAspect2 = readEntityResponse2.getAspects().get(aspectName);
        assertEquals(envelopedAspect2.getName(), aspectName);
        assertTrue(
            DataTemplateUtil.areEqual(writeEntity2.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserInfo(),
                new CorpUserInfo(envelopedAspect2.getValue().data())));
        CorpUserKey expectedKey2 = new CorpUserKey();
        expectedKey2.setUsername("tester2");
        EnvelopedAspect envelopedKey2 = readEntityResponse2.getAspects().get(keyName);
        assertTrue(DataTemplateUtil.areEqual(expectedKey2, new CorpUserKey(envelopedKey2.getValue().data())));

        verify(_mockProducer, times(2)).produceMetadataAuditEvent(Mockito.eq(entityUrn1), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verify(_mockProducer, times(2)).produceMetadataAuditEvent(Mockito.eq(entityUrn2), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn1),
            Mockito.any(), Mockito.any());

        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn2),
            Mockito.any(), Mockito.any());

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testIngestAspectsGetLatestAspects() throws Exception {

        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");

        List<Pair<String, RecordTemplate>> pairToIngest = new ArrayList<>();

        Status writeAspect1 = new Status().setRemoved(false);
        String aspectName1 = getAspectName(writeAspect1);
        pairToIngest.add(getAspectRecordPair(writeAspect1, Status.class));

        CorpUserInfo writeAspect2 = createCorpUserInfo("email@test.com");
        String aspectName2 = getAspectName(writeAspect2);
        pairToIngest.add(getAspectRecordPair(writeAspect2, CorpUserInfo.class));

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        _entityService.ingestAspects(entityUrn, pairToIngest, TEST_AUDIT_STAMP, metadata1);

        Map<String, RecordTemplate> latestAspects = _entityService.getLatestAspectsForUrn(
            entityUrn,
            new HashSet<>(Arrays.asList(aspectName1, aspectName2))
        );
        assertTrue(DataTemplateUtil.areEqual(writeAspect1, latestAspects.get(aspectName1)));
        assertTrue(DataTemplateUtil.areEqual(writeAspect2, latestAspects.get(aspectName2)));

        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn),
            Mockito.any(), Mockito.any());
        verify(_mockProducer, times(2)).produceMetadataAuditEvent(Mockito.eq(entityUrn),
            Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testIngestTimeseriesAspect() throws Exception {
        Urn entityUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
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
        _entityService.ingestProposal(gmce, TEST_AUDIT_STAMP);
    }

    @Test
    public void testUpdateGetAspect() throws Exception {
        // Test Writing a CorpUser Entity
        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");

        String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());
        AspectSpec corpUserInfoSpec = _testEntityRegistry.getEntitySpec("corpuser").getAspectSpec("corpUserInfo");

        // Ingest CorpUserInfo Aspect #1
        CorpUserInfo writeAspect = createCorpUserInfo("email@test.com");

        // Validate retrieval of CorpUserInfo Aspect #1
        _entityService.updateAspect(entityUrn, "corpuser", aspectName, corpUserInfoSpec, writeAspect, TEST_AUDIT_STAMP, 1,
            true);
        RecordTemplate readAspect1 = _entityService.getAspect(entityUrn, aspectName, 1);
        assertTrue(DataTemplateUtil.areEqual(writeAspect, readAspect1));
        verify(_mockProducer, times(1)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.eq(corpUserInfoSpec),
            Mockito.any());

        // Ingest CorpUserInfo Aspect #2
        writeAspect.setEmail("newemail@test.com");

        // Validate retrieval of CorpUserInfo Aspect #2
        _entityService.updateAspect(entityUrn, "corpuser", aspectName, corpUserInfoSpec, writeAspect, TEST_AUDIT_STAMP, 1,
            false);
        RecordTemplate readAspect2 = _entityService.getAspect(entityUrn, aspectName, 1);
        assertTrue(DataTemplateUtil.areEqual(writeAspect, readAspect2));
        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testGetAspectAtVersion() throws Exception {
        // Test Writing a CorpUser Entity
        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");

        String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());
        AspectSpec corpUserInfoSpec = _testEntityRegistry.getEntitySpec("corpuser").getAspectSpec("corpUserInfo");

        // Ingest CorpUserInfo Aspect #1
        CorpUserInfo writeAspect = createCorpUserInfo("email@test.com");

        // Validate retrieval of CorpUserInfo Aspect #1
        _entityService.updateAspect(entityUrn, "corpuser", aspectName, corpUserInfoSpec, writeAspect, TEST_AUDIT_STAMP, 1,
            true);

        VersionedAspect writtenVersionedAspect = new VersionedAspect();
        writtenVersionedAspect.setAspect(Aspect.create(writeAspect));
        writtenVersionedAspect.setVersion(1);

        VersionedAspect readAspect1 = _entityService.getVersionedAspect(entityUrn, aspectName, 1);
        assertTrue(DataTemplateUtil.areEqual(writtenVersionedAspect, readAspect1));
        verify(_mockProducer, times(1)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.eq(corpUserInfoSpec),
            Mockito.any());

        VersionedAspect readAspect2 = _entityService.getVersionedAspect(entityUrn, aspectName, -1);
        assertTrue(DataTemplateUtil.areEqual(writtenVersionedAspect, readAspect2));

        VersionedAspect readAspectVersion0 = _entityService.getVersionedAspect(entityUrn, aspectName, 0);
        assertFalse(DataTemplateUtil.areEqual(writtenVersionedAspect, readAspectVersion0));

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testRollbackAspect() throws Exception {
        Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:test1");
        Urn entityUrn2 = Urn.createFromString("urn:li:corpuser:test2");
        Urn entityUrn3 = Urn.createFromString("urn:li:corpuser:test3");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        SystemMetadata metadata2 = new SystemMetadata();
        metadata2.setLastObserved(1635792689);
        metadata2.setRunId("run-456");

        String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());

        // Ingest CorpUserInfo Aspect #1
        CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
        _entityService.ingestAspect(entityUrn1, aspectName, writeAspect1, TEST_AUDIT_STAMP, metadata1);

        // Ingest CorpUserInfo Aspect #2
        CorpUserInfo writeAspect2 = createCorpUserInfo("email2@test.com");
        _entityService.ingestAspect(entityUrn2, aspectName, writeAspect2, TEST_AUDIT_STAMP, metadata1);

        // Ingest CorpUserInfo Aspect #3
        CorpUserInfo writeAspect3 = createCorpUserInfo("email3@test.com");
        _entityService.ingestAspect(entityUrn3, aspectName, writeAspect3, TEST_AUDIT_STAMP, metadata1);

        // Ingest CorpUserInfo Aspect #1 Overwrite
        CorpUserInfo writeAspect1Overwrite = createCorpUserInfo("email1.overwrite@test.com");
        _entityService.ingestAspect(entityUrn1, aspectName, writeAspect1Overwrite, TEST_AUDIT_STAMP, metadata2);

        // this should no-op since this run has been overwritten
        AspectRowSummary rollbackOverwrittenAspect = new AspectRowSummary();
        rollbackOverwrittenAspect.setRunId("run-123");
        rollbackOverwrittenAspect.setAspectName(aspectName);
        rollbackOverwrittenAspect.setUrn(entityUrn1.toString());

        _entityService.rollbackRun(ImmutableList.of(rollbackOverwrittenAspect), "run-123", true);

        // assert nothing was deleted
        RecordTemplate readAspectOriginal = _entityService.getAspect(entityUrn1, aspectName, 1);
        assertTrue(DataTemplateUtil.areEqual(writeAspect1, readAspectOriginal));

        RecordTemplate readAspectOverwrite = _entityService.getAspect(entityUrn1, aspectName, 0);
        assertTrue(DataTemplateUtil.areEqual(writeAspect1Overwrite, readAspectOverwrite));

        // this should delete the most recent aspect
        AspectRowSummary rollbackRecentAspect = new AspectRowSummary();
        rollbackRecentAspect.setRunId("run-456");
        rollbackRecentAspect.setAspectName(aspectName);
        rollbackRecentAspect.setUrn(entityUrn1.toString());

        _entityService.rollbackRun(ImmutableList.of(rollbackOverwrittenAspect), "run-456", true);

        // assert the new most recent aspect is the original one
        RecordTemplate readNewRecentAspect = _entityService.getAspect(entityUrn1, aspectName, 0);
        assertTrue(DataTemplateUtil.areEqual(writeAspect1, readNewRecentAspect));
    }

    @Test
    public void testRollbackKey() throws Exception {
        Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:test1");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        SystemMetadata metadata2 = new SystemMetadata();
        metadata2.setLastObserved(1635792689);
        metadata2.setRunId("run-456");

        String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());
        String keyAspectName = _entityService.getKeyAspectName(entityUrn1);

        // Ingest CorpUserInfo Aspect #1
        CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
        _entityService.ingestAspect(entityUrn1, aspectName, writeAspect1, TEST_AUDIT_STAMP, metadata1);

        RecordTemplate writeKey1 = _entityService.buildKeyAspect(entityUrn1);
        _entityService.ingestAspect(entityUrn1, keyAspectName, writeKey1, TEST_AUDIT_STAMP, metadata1);

        // Ingest CorpUserInfo Aspect #1 Overwrite
        CorpUserInfo writeAspect1Overwrite = createCorpUserInfo("email1.overwrite@test.com");
        _entityService.ingestAspect(entityUrn1, aspectName, writeAspect1Overwrite, TEST_AUDIT_STAMP, metadata2);

        // this should no-op since the key should have been written in the furst run
        AspectRowSummary rollbackKeyWithWrongRunId = new AspectRowSummary();
        rollbackKeyWithWrongRunId.setRunId("run-456");
        rollbackKeyWithWrongRunId.setAspectName("corpUserKey");
        rollbackKeyWithWrongRunId.setUrn(entityUrn1.toString());

        _entityService.rollbackRun(ImmutableList.of(rollbackKeyWithWrongRunId), "run-456", true);

        // assert nothing was deleted
        RecordTemplate readAspectOriginal = _entityService.getAspect(entityUrn1, aspectName, 1);
        assertTrue(DataTemplateUtil.areEqual(writeAspect1, readAspectOriginal));

        RecordTemplate readAspectOverwrite = _entityService.getAspect(entityUrn1, aspectName, 0);
        assertTrue(DataTemplateUtil.areEqual(writeAspect1Overwrite, readAspectOverwrite));

        // this should delete the most recent aspect
        AspectRowSummary rollbackKeyWithCorrectRunId = new AspectRowSummary();
        rollbackKeyWithCorrectRunId.setRunId("run-123");
        rollbackKeyWithCorrectRunId.setAspectName("corpUserKey");
        rollbackKeyWithCorrectRunId.setUrn(entityUrn1.toString());

        _entityService.rollbackRun(ImmutableList.of(rollbackKeyWithCorrectRunId), "run-123", true);

        // assert the new most recent aspect is null
        RecordTemplate readNewRecentAspect = _entityService.getAspect(entityUrn1, aspectName, 0);
        assertTrue(DataTemplateUtil.areEqual(null, readNewRecentAspect));
    }

    @Test
    public void testRollbackUrn() throws Exception {
        Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:test1");
        Urn entityUrn2 = Urn.createFromString("urn:li:corpuser:test2");
        Urn entityUrn3 = Urn.createFromString("urn:li:corpuser:test3");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        SystemMetadata metadata2 = new SystemMetadata();
        metadata2.setLastObserved(1635792689);
        metadata2.setRunId("run-456");

        String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());
        String keyAspectName = _entityService.getKeyAspectName(entityUrn1);

        // Ingest CorpUserInfo Aspect #1
        CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
        _entityService.ingestAspect(entityUrn1, aspectName, writeAspect1, TEST_AUDIT_STAMP, metadata1);

        RecordTemplate writeKey1 = _entityService.buildKeyAspect(entityUrn1);
        _entityService.ingestAspect(entityUrn1, keyAspectName, writeKey1, TEST_AUDIT_STAMP, metadata1);

        // Ingest CorpUserInfo Aspect #2
        CorpUserInfo writeAspect2 = createCorpUserInfo("email2@test.com");
        _entityService.ingestAspect(entityUrn2, aspectName, writeAspect2, TEST_AUDIT_STAMP, metadata1);

        // Ingest CorpUserInfo Aspect #3
        CorpUserInfo writeAspect3 = createCorpUserInfo("email3@test.com");
        _entityService.ingestAspect(entityUrn3, aspectName, writeAspect3, TEST_AUDIT_STAMP, metadata1);

        // Ingest CorpUserInfo Aspect #1 Overwrite
        CorpUserInfo writeAspect1Overwrite = createCorpUserInfo("email1.overwrite@test.com");
        _entityService.ingestAspect(entityUrn1, aspectName, writeAspect1Overwrite, TEST_AUDIT_STAMP, metadata2);

        // this should no-op since the key should have been written in the furst run
        AspectRowSummary rollbackKeyWithWrongRunId = new AspectRowSummary();
        rollbackKeyWithWrongRunId.setRunId("run-456");
        rollbackKeyWithWrongRunId.setAspectName("CorpUserKey");
        rollbackKeyWithWrongRunId.setUrn(entityUrn1.toString());

        // this should delete all related aspects
        _entityService.deleteUrn(Urn.createFromString("urn:li:corpuser:test1"));

        // assert the new most recent aspect is null
        RecordTemplate readNewRecentAspect = _entityService.getAspect(entityUrn1, aspectName, 0);
        assertTrue(DataTemplateUtil.areEqual(null, readNewRecentAspect));

        RecordTemplate deletedKeyAspect = _entityService.getAspect(entityUrn1, "corpUserKey", 0);
        assertTrue(DataTemplateUtil.areEqual(null, deletedKeyAspect));
    }

    @Test
    public void testIngestGetLatestAspect() throws Exception {
        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");

        // Ingest CorpUserInfo Aspect #1
        CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
        String aspectName = PegasusUtils.getAspectNameFromSchema(writeAspect1.schema());

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        SystemMetadata metadata2 = new SystemMetadata();
        metadata2.setLastObserved(1635792689);
        metadata2.setRunId("run-456");

        // Validate retrieval of CorpUserInfo Aspect #1
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect1, TEST_AUDIT_STAMP, metadata1);
        RecordTemplate readAspect1 = _entityService.getLatestAspect(entityUrn, aspectName);
        assertTrue(DataTemplateUtil.areEqual(writeAspect1, readAspect1));

        ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
        verify(_mockProducer, times(1)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());
        MetadataChangeLog mcl = mclCaptor.getValue();
        assertEquals(mcl.getEntityType(), "corpuser");
        assertNull(mcl.getPreviousAspectValue());
        assertNull(mcl.getPreviousSystemMetadata());
        assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

        verify(_mockProducer, times(1)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.any(), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verifyNoMoreInteractions(_mockProducer);

        reset(_mockProducer);

        // Ingest CorpUserInfo Aspect #2
        CorpUserInfo writeAspect2 = createCorpUserInfo("email2@test.com");

        // Validate retrieval of CorpUserInfo Aspect #2
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect2, TEST_AUDIT_STAMP, metadata2);
        RecordTemplate readAspect2 = _entityService.getLatestAspect(entityUrn, aspectName);
        EntityAspect readAspectDao1 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 1);
        EntityAspect readAspectDao2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

        assertTrue(DataTemplateUtil.areEqual(writeAspect2, readAspect2));
        assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata2));
        assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readAspectDao1.getSystemMetadata()), metadata1));

        verify(_mockProducer, times(1)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());
        mcl = mclCaptor.getValue();
        assertEquals(mcl.getEntityType(), "corpuser");
        assertNotNull(mcl.getPreviousAspectValue());
        assertNotNull(mcl.getPreviousSystemMetadata());
        assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

        verify(_mockProducer, times(1)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.notNull(), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testIngestGetLatestEnvelopedAspect() throws Exception {
        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");

        // Ingest CorpUserInfo Aspect #1
        CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
        String aspectName = PegasusUtils.getAspectNameFromSchema(writeAspect1.schema());

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        SystemMetadata metadata2 = new SystemMetadata();
        metadata2.setLastObserved(1635792689);
        metadata2.setRunId("run-456");

        // Validate retrieval of CorpUserInfo Aspect #1
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect1, TEST_AUDIT_STAMP, metadata1);
        EnvelopedAspect readAspect1 = _entityService.getLatestEnvelopedAspect("corpuser", entityUrn, aspectName);
        assertTrue(DataTemplateUtil.areEqual(writeAspect1, new CorpUserInfo(readAspect1.getValue().data())));

        // Ingest CorpUserInfo Aspect #2
        CorpUserInfo writeAspect2 = createCorpUserInfo("email2@test.com");

        // Validate retrieval of CorpUserInfo Aspect #2
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect2, TEST_AUDIT_STAMP, metadata2);
        EnvelopedAspect readAspect2 = _entityService.getLatestEnvelopedAspect("corpuser", entityUrn, aspectName);
        EntityAspect readAspectDao1 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 1);
        EntityAspect readAspectDao2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

        assertTrue(DataTemplateUtil.areEqual(writeAspect2, new CorpUserInfo(readAspect2.getValue().data())));
        assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata2));
        assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readAspectDao1.getSystemMetadata()), metadata1));

        verify(_mockProducer, times(1)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verify(_mockProducer, times(1)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.notNull(), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verify(_mockProducer, times(2)).produceMetadataChangeLog(Mockito.eq(entityUrn),
            Mockito.any(), Mockito.any());

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testIngestSameAspect() throws Exception {
        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");

        // Ingest CorpUserInfo Aspect #1
        CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
        String aspectName = PegasusUtils.getAspectNameFromSchema(writeAspect1.schema());

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        SystemMetadata metadata2 = new SystemMetadata();
        metadata2.setLastObserved(1635792689);
        metadata2.setRunId("run-456");

        // Validate retrieval of CorpUserInfo Aspect #1
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect1, TEST_AUDIT_STAMP, metadata1);
        RecordTemplate readAspect1 = _entityService.getLatestAspect(entityUrn, aspectName);
        assertTrue(DataTemplateUtil.areEqual(writeAspect1, readAspect1));

        ArgumentCaptor<MetadataChangeLog> mclCaptor = ArgumentCaptor.forClass(MetadataChangeLog.class);
        verify(_mockProducer, times(1)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());
        MetadataChangeLog mcl = mclCaptor.getValue();
        assertEquals(mcl.getEntityType(), "corpuser");
        assertNull(mcl.getPreviousAspectValue());
        assertNull(mcl.getPreviousSystemMetadata());
        assertEquals(mcl.getChangeType(), ChangeType.UPSERT);

        verify(_mockProducer, times(1)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.eq(null), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verifyNoMoreInteractions(_mockProducer);

        reset(_mockProducer);

        // Ingest CorpUserInfo Aspect #2
        CorpUserInfo writeAspect2 = createCorpUserInfo("email@test.com");

        // Validate retrieval of CorpUserInfo Aspect #2
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect2, TEST_AUDIT_STAMP, metadata2);
        RecordTemplate readAspect2 = _entityService.getLatestAspect(entityUrn, aspectName);
        EntityAspect readAspectDao2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

        assertTrue(DataTemplateUtil.areEqual(writeAspect2, readAspect2));
        assertFalse(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata2));
        assertFalse(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata1));

        SystemMetadata metadata3 = new SystemMetadata();
        metadata3.setLastObserved(1635792689);
        metadata3.setRunId("run-123");

        assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readAspectDao2.getSystemMetadata()), metadata3));

        verify(_mockProducer, times(0)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());

        verify(_mockProducer, times(0)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.notNull(), Mockito.any(),
            Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

        verifyNoMoreInteractions(_mockProducer);
    }

    @Test
    public void testRetention() throws Exception {
        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test1");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());

        // Ingest CorpUserInfo Aspect
        CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect1, TEST_AUDIT_STAMP, metadata1);
        CorpUserInfo writeAspect1a = createCorpUserInfo("email_a@test.com");
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect1a, TEST_AUDIT_STAMP, metadata1);
        CorpUserInfo writeAspect1b = createCorpUserInfo("email_b@test.com");
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect1b, TEST_AUDIT_STAMP, metadata1);

        String aspectName2 = PegasusUtils.getAspectNameFromSchema(new Status().schema());
        // Ingest Status Aspect
        Status writeAspect2 = new Status().setRemoved(true);
        _entityService.ingestAspect(entityUrn, aspectName2, writeAspect2, TEST_AUDIT_STAMP, metadata1);
        Status writeAspect2a = new Status().setRemoved(false);
        _entityService.ingestAspect(entityUrn, aspectName2, writeAspect2a, TEST_AUDIT_STAMP, metadata1);
        Status writeAspect2b = new Status().setRemoved(true);
        _entityService.ingestAspect(entityUrn, aspectName2, writeAspect2b, TEST_AUDIT_STAMP, metadata1);

        assertEquals(_entityService.getAspect(entityUrn, aspectName, 1), writeAspect1);
        assertEquals(_entityService.getAspect(entityUrn, aspectName2, 1), writeAspect2);

        _retentionService.setRetention(null, null, new DataHubRetentionConfig().setRetention(
            new Retention().setVersion(new VersionBasedRetention().setMaxVersions(2))));
        _retentionService.setRetention("corpuser", "status", new DataHubRetentionConfig().setRetention(
            new Retention().setVersion(new VersionBasedRetention().setMaxVersions(4))));

        // Ingest CorpUserInfo Aspect again
        CorpUserInfo writeAspect1c = createCorpUserInfo("email_c@test.com");
        _entityService.ingestAspect(entityUrn, aspectName, writeAspect1c, TEST_AUDIT_STAMP, metadata1);
        // Ingest Status Aspect again
        Status writeAspect2c = new Status().setRemoved(false);
        _entityService.ingestAspect(entityUrn, aspectName2, writeAspect2c, TEST_AUDIT_STAMP, metadata1);

        assertNull(_entityService.getAspect(entityUrn, aspectName, 1));
        assertEquals(_entityService.getAspect(entityUrn, aspectName2, 1), writeAspect2);

        // Reset retention policies
        _retentionService.setRetention(null, null, new DataHubRetentionConfig().setRetention(
            new Retention().setVersion(new VersionBasedRetention().setMaxVersions(1))));
        _retentionService.deleteRetention("corpuser", "status");
        // Invoke batch apply
        _retentionService.batchApplyRetention(null, null);
        assertEquals(_entityService.listLatestAspects(entityUrn.getEntityType(), aspectName, 0, 10).getTotalCount(), 1);
        assertEquals(_entityService.listLatestAspects(entityUrn.getEntityType(), aspectName2, 0, 10).getTotalCount(), 1);
    }

    @Test
    public void testIngestAspectIfNotPresent() throws Exception {
        Urn entityUrn = Urn.createFromString("urn:li:corpuser:test1");

        SystemMetadata metadata1 = new SystemMetadata();
        metadata1.setLastObserved(1625792689);
        metadata1.setRunId("run-123");

        String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());

        // Ingest CorpUserInfo Aspect
        CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
        _entityService.ingestAspectIfNotPresent(entityUrn, aspectName, writeAspect1, TEST_AUDIT_STAMP, metadata1);
        CorpUserInfo writeAspect1a = createCorpUserInfo("email_a@test.com");
        _entityService.ingestAspectIfNotPresent(entityUrn, aspectName, writeAspect1a, TEST_AUDIT_STAMP, metadata1);
        CorpUserInfo writeAspect1b = createCorpUserInfo("email_b@test.com");
        _entityService.ingestAspectIfNotPresent(entityUrn, aspectName, writeAspect1b, TEST_AUDIT_STAMP, metadata1);

        String aspectName2 = PegasusUtils.getAspectNameFromSchema(new Status().schema());
        // Ingest Status Aspect
        Status writeAspect2 = new Status().setRemoved(true);
        _entityService.ingestAspectIfNotPresent(entityUrn, aspectName2, writeAspect2, TEST_AUDIT_STAMP, metadata1);
        Status writeAspect2a = new Status().setRemoved(false);
        _entityService.ingestAspectIfNotPresent(entityUrn, aspectName2, writeAspect2a, TEST_AUDIT_STAMP, metadata1);
        Status writeAspect2b = new Status().setRemoved(true);
        _entityService.ingestAspectIfNotPresent(entityUrn, aspectName2, writeAspect2b, TEST_AUDIT_STAMP, metadata1);

        assertEquals(_entityService.getAspect(entityUrn, aspectName, 0), writeAspect1);
        assertEquals(_entityService.getAspect(entityUrn, aspectName2, 0), writeAspect2);

        assertNull(_entityService.getAspect(entityUrn, aspectName, 1));
        assertNull(_entityService.getAspect(entityUrn, aspectName2, 1));

        assertEquals(_entityService.listLatestAspects(entityUrn.getEntityType(), aspectName, 0, 10).getTotalCount(), 1);
        assertEquals(_entityService.listLatestAspects(entityUrn.getEntityType(), aspectName2, 0, 10).getTotalCount(), 1);
    }

    protected static AuditStamp createTestAuditStamp() {
        try {
            return new AuditStamp().setTime(123L).setActor(Urn.createFromString("urn:li:principal:tester"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create urn");
        }
    }

    @Nonnull
    protected com.linkedin.entity.Entity createCorpUserEntity(Urn entityUrn, String email) throws Exception {
        CorpuserUrn corpuserUrn = CorpuserUrn.createFromUrn(entityUrn);
        com.linkedin.entity.Entity entity = new com.linkedin.entity.Entity();
        Snapshot snapshot = new Snapshot();
        CorpUserSnapshot corpUserSnapshot = new CorpUserSnapshot();
        List<CorpUserAspect> userAspects = new ArrayList<>();
        userAspects.add(CorpUserAspect.create(createCorpUserInfo(email)));
        corpUserSnapshot.setAspects(new CorpUserAspectArray(userAspects));
        corpUserSnapshot.setUrn(corpuserUrn);
        snapshot.setCorpUserSnapshot(corpUserSnapshot);
        entity.setValue(snapshot);
        return entity;
    }

    @Nonnull
    protected RecordTemplate createCorpUserKey(Urn urn) throws Exception {
        return EntityKeyUtils.convertUrnToEntityKey(urn, new CorpUserKey().schema());
    }

    @Nonnull
    protected CorpUserInfo createCorpUserInfo(String email) throws Exception {
        CorpUserInfo corpUserInfo = new CorpUserInfo();
        corpUserInfo.setEmail(email);
        corpUserInfo.setActive(true);
        return corpUserInfo;
    }

    protected String getAspectName(RecordTemplate record) {
        return PegasusUtils.getAspectNameFromSchema(record.schema());
    }

    protected <T extends RecordTemplate> Pair<String, RecordTemplate> getAspectRecordPair(T aspect, Class<T> clazz)
        throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        RecordTemplate recordTemplate = RecordUtils.toRecordTemplate(clazz, objectMapper.writeValueAsString(aspect));
        return new Pair<>(getAspectName(aspect), recordTemplate);
    }
}
