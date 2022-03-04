package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.EbeanEntityService;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.event.EntityEventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class EbeanEntityServiceTest extends EntityServiceTestBase<EbeanAspectDao, EbeanEntityService, EbeanRetentionService> {

  private EbeanServer _server;

  public EbeanEntityServiceTest() throws EntityRegistryException {
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

  @BeforeMethod
  public void setupTest() {
    _server = EbeanServerFactory.create(createTestingH2ServerConfig());
    _mockProducer = mock(EntityEventProducer.class);
    _aspectDao = new EbeanAspectDao(_server);
    _aspectDao.setConnectionValidated(true);
    _entityService = new EbeanEntityService(_aspectDao, _mockProducer, _testEntityRegistry);
    _retentionService = new EbeanRetentionService(_entityService, _server, 1000);
    _entityService.setRetentionService(_retentionService);
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
    EbeanAspectV2 readEbean1 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 1);
    EbeanAspectV2 readEbean2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

    assertTrue(DataTemplateUtil.areEqual(writeAspect2, readAspect2));
    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readEbean2.getSystemMetadata()), metadata2));
    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readEbean1.getSystemMetadata()), metadata1));

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
    EbeanAspectV2 readEbean1 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 1);
    EbeanAspectV2 readEbean2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

    assertTrue(DataTemplateUtil.areEqual(writeAspect2, new CorpUserInfo(readAspect2.getValue().data())));
    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readEbean2.getSystemMetadata()), metadata2));
    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readEbean1.getSystemMetadata()), metadata1));

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
    EbeanAspectV2 readEbean2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

    assertTrue(DataTemplateUtil.areEqual(writeAspect2, readAspect2));
    assertFalse(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readEbean2.getSystemMetadata()), metadata2));
    assertFalse(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readEbean2.getSystemMetadata()), metadata1));

    SystemMetadata metadata3 = new SystemMetadata();
    metadata3.setLastObserved(1635792689);
    metadata3.setRunId("run-123");

    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readEbean2.getSystemMetadata()), metadata3));

    verify(_mockProducer, times(0)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());

    verify(_mockProducer, times(0)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.notNull(), Mockito.any(),
        Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

    verifyNoMoreInteractions(_mockProducer);
  }
}
