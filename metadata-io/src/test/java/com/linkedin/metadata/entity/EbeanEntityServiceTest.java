package com.linkedin.metadata.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanEntityService;
import com.linkedin.metadata.event.EntityEventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class EbeanEntityServiceTest {

  private static final AuditStamp TEST_AUDIT_STAMP = createTestAuditStamp();
  private EbeanEntityService _entityService;
  private EbeanAspectDao _aspectDao;
  private EbeanServer _server;
  private EntityEventProducer _mockProducer;

  @BeforeMethod
  public void setupTest() {
    _server = EbeanServerFactory.create(createTestingH2ServerConfig());
    _mockProducer = mock(EntityEventProducer.class);
    _aspectDao = new EbeanAspectDao(_server);
    _aspectDao.setConnectionValidated(true);
    _entityService = new EbeanEntityService(
        _aspectDao,
        _mockProducer,
        new TestEntityRegistry());
  }

  @Test
  public void testIngestGetEntity() throws Exception {
    // Test Writing a CorpUser Entity
    Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");
    com.linkedin.entity.Entity writeEntity = createCorpUserEntity(entityUrn, "tester@test.com");

    // 1. Ingest Entity
    _entityService.ingestEntity(writeEntity, TEST_AUDIT_STAMP);

    // 2. Retrieve Entity
    com.linkedin.entity.Entity readEntity = _entityService.getEntity(entityUrn, Collections.emptySet());

    // 3. Compare Entity Objects
    assertEquals(2, readEntity.getValue().getCorpUserSnapshot().getAspects().size()); // Key + Info aspect.
    assertTrue(DataTemplateUtil.areEqual(
        writeEntity.getValue().getCorpUserSnapshot().getAspects().get(0),
        readEntity.getValue().getCorpUserSnapshot().getAspects().get(1)));
    CorpUserKey expectedKey = new CorpUserKey();
    expectedKey.setUsername("test");
    assertTrue(DataTemplateUtil.areEqual(
        expectedKey,
        readEntity.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserKey()
    )); // Key + Info aspect.

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(
        Mockito.eq(entityUrn),
        Mockito.eq(null),
        Mockito.any());
    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestGetEntities() throws Exception {
    // Test Writing a CorpUser Entity
    Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:tester1");
    com.linkedin.entity.Entity writeEntity1 = createCorpUserEntity(entityUrn1, "tester@test.com");

    Urn entityUrn2 = Urn.createFromString("urn:li:corpuser:tester2");
    com.linkedin.entity.Entity writeEntity2 = createCorpUserEntity(entityUrn2, "tester2@test.com");

    // 1. Ingest Entities
    _entityService.ingestEntities(ImmutableList.of(writeEntity1, writeEntity2), TEST_AUDIT_STAMP);

    // 2. Retrieve Entities
    Map<Urn, com.linkedin.entity.Entity> readEntities = _entityService
        .getEntities(ImmutableSet.of(entityUrn1, entityUrn2), Collections.emptySet());

    // 3. Compare Entity Objects

    // Entity 1
    com.linkedin.entity.Entity readEntity1 = readEntities.get(entityUrn1);
    assertEquals(2, readEntity1.getValue().getCorpUserSnapshot().getAspects().size()); // Key + Info aspect.
    assertTrue(DataTemplateUtil.areEqual(
        writeEntity1.getValue().getCorpUserSnapshot().getAspects().get(0),
        readEntity1.getValue().getCorpUserSnapshot().getAspects().get(1)));
    CorpUserKey expectedKey1 = new CorpUserKey();
    expectedKey1.setUsername("tester1");
    assertTrue(DataTemplateUtil.areEqual(
        expectedKey1,
        readEntity1.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserKey()
    )); // Key + Info aspect.

    // Entity 2
    com.linkedin.entity.Entity readEntity2 = readEntities.get(entityUrn2);
    assertEquals(2, readEntity2.getValue().getCorpUserSnapshot().getAspects().size()); // Key + Info aspect.
    assertTrue(DataTemplateUtil.areEqual(
        writeEntity2.getValue().getCorpUserSnapshot().getAspects().get(0),
        readEntity2.getValue().getCorpUserSnapshot().getAspects().get(1)));
    CorpUserKey expectedKey2 = new CorpUserKey();
    expectedKey2.setUsername("tester2");
    assertTrue(DataTemplateUtil.areEqual(
        expectedKey2,
        readEntity2.getValue().getCorpUserSnapshot().getAspects().get(0).getCorpUserKey()
    )); // Key + Info aspect.

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(
        Mockito.eq(entityUrn1),
        Mockito.eq(null),
        Mockito.any());

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(
        Mockito.eq(entityUrn2),
        Mockito.eq(null),
        Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestGetLatestAspect() throws Exception {
    Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
    String aspectName = PegasusUtils.getAspectNameFromSchema(writeAspect1.schema());

    // Validate retrieval of CorpUserInfo Aspect #1
    _entityService.ingestAspect(entityUrn, aspectName, writeAspect1, TEST_AUDIT_STAMP);
    RecordTemplate readAspect1 = _entityService.getLatestAspect(entityUrn, aspectName);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, readAspect1));

    // Ingest CorpUserInfo Aspect #2
    CorpUserInfo writeAspect2 = createCorpUserInfo("email2@test.com");

    // Validate retrieval of CorpUserInfo Aspect #2
    _entityService.ingestAspect(entityUrn, aspectName, writeAspect2, TEST_AUDIT_STAMP);
    RecordTemplate readAspect2 = _entityService.getLatestAspect(entityUrn, aspectName);
    assertTrue(DataTemplateUtil.areEqual(writeAspect2, readAspect2));

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(
        Mockito.eq(entityUrn),
        Mockito.eq(null),
        Mockito.any());

    verify(_mockProducer, times(1)).produceMetadataAuditEvent(
        Mockito.eq(entityUrn),
        Mockito.notNull(),
        Mockito.any());

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestListLatestAspects() throws Exception {
    Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:test1");
    Urn entityUrn2 = Urn.createFromString("urn:li:corpuser:test2");
    Urn entityUrn3 = Urn.createFromString("urn:li:corpuser:test3");

    String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = createCorpUserInfo("email@test.com");
    _entityService.ingestAspect(entityUrn1, aspectName, writeAspect1, TEST_AUDIT_STAMP);


    // Ingest CorpUserInfo Aspect #2
    CorpUserInfo writeAspect2 = createCorpUserInfo("email2@test.com");
    _entityService.ingestAspect(entityUrn2, aspectName, writeAspect2, TEST_AUDIT_STAMP);


    // Ingest CorpUserInfo Aspect #3
    CorpUserInfo writeAspect3 = createCorpUserInfo("email3@test.com");
    _entityService.ingestAspect(entityUrn3, aspectName, writeAspect3, TEST_AUDIT_STAMP);

    // List aspects
    ListResult<RecordTemplate> batch1 = _entityService.listLatestAspects(aspectName, 0, 2);

    assertEquals(2, batch1.getNextStart());
    assertEquals(2, batch1.getPageSize());
    assertEquals(3, batch1.getTotalCount());
    assertEquals(2, batch1.getTotalPageCount());
    assertEquals(2, batch1.getValues().size());
    assertTrue(DataTemplateUtil.areEqual(writeAspect1,  batch1.getValues().get(0)));
    assertTrue(DataTemplateUtil.areEqual(writeAspect2,  batch1.getValues().get(1)));

    ListResult<RecordTemplate> batch2 = _entityService.listLatestAspects(aspectName, 2, 2);
    assertEquals(1, batch2.getValues().size());
    assertTrue(DataTemplateUtil.areEqual(writeAspect3,  batch2.getValues().get(0)));
  }

  @Test
  public void testUpdateGetAspect() throws Exception {
    // Test Writing a CorpUser Entity
    Urn entityUrn = Urn.createFromString("urn:li:corpuser:test");

    String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect = createCorpUserInfo("email@test.com");

    // Validate retrieval of CorpUserInfo Aspect #1
    _entityService.updateAspect(entityUrn, aspectName, writeAspect, TEST_AUDIT_STAMP, 1, true);
    RecordTemplate readAspect1 = _entityService.getAspect(entityUrn, aspectName, 1);
    assertTrue(DataTemplateUtil.areEqual(writeAspect, readAspect1));
    verify(_mockProducer, times(1)).produceMetadataAuditEvent(
        Mockito.eq(entityUrn),
        Mockito.eq(null),
        Mockito.any());
    // Ingest CorpUserInfo Aspect #2
    writeAspect.setEmail("newemail@test.com");

    // Validate retrieval of CorpUserInfo Aspect #2
    _entityService.updateAspect(entityUrn, aspectName, writeAspect, TEST_AUDIT_STAMP, 1, false);
    RecordTemplate readAspect2 = _entityService.getAspect(entityUrn, aspectName, 1);
    assertTrue(DataTemplateUtil.areEqual(writeAspect, readAspect2));
    verifyNoMoreInteractions(_mockProducer);
  }

  @Nonnull
  private com.linkedin.entity.Entity createCorpUserEntity(Urn entityUrn, String email) throws Exception {
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
  private CorpUserInfo createCorpUserInfo(String email) throws Exception {
    CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setEmail(email);
    corpUserInfo.setActive(true);
    return corpUserInfo;
  }

  @Nonnull
  private static ServerConfig createTestingH2ServerConfig() {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    dataSourceConfig.setUrl("jdbc:h2:mem:;IGNORECASE=TRUE;");
    dataSourceConfig.setDriver("org.h2.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(true);
    serverConfig.setDdlRun(true);

    return serverConfig;
  }

  private static AuditStamp createTestAuditStamp() {
    try {
      return new AuditStamp()
          .setTime(123L)
          .setActor(Urn.createFromString("urn:li:principal:tester"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create urn");
    }
  }
}
