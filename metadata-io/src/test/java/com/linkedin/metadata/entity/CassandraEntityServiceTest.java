package com.linkedin.metadata.entity;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.entity.cassandra.CassandraAspect;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.cassandra.CassandraEntityService;
import com.linkedin.metadata.entity.cassandra.CassandraRetentionService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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


public class CassandraEntityServiceTest extends EntityServiceTestBase<CassandraAspectDao, CassandraEntityService, CassandraRetentionService> {

  private CassandraContainer _cassandraContainer;
  private static final String KEYSPACE_NAME = "test";

  public CassandraEntityServiceTest() throws EntityRegistryException {
  }

  private CqlSession createTestSession() {
    Map<String, String> sessionConfig = createTestServerConfig();
    int port = Integer.parseInt(sessionConfig.get("port"));
    List<InetSocketAddress> addresses = Arrays.stream(sessionConfig.get("hosts").split(","))
        .map(host -> new InetSocketAddress(host, port))
        .collect(Collectors.toList());

    String dc = sessionConfig.get("datacenter");
    String ks = sessionConfig.get("keyspace");
    String username = sessionConfig.get("username");
    String password = sessionConfig.get("password");

    CqlSessionBuilder csb = CqlSession.builder()
        .addContactPoints(addresses)
        .withLocalDatacenter(dc)
        .withKeyspace(ks)
        .withAuthCredentials(username, password);

    if (sessionConfig.containsKey("useSsl") && sessionConfig.get("useSsl").equals("true")) {
      try {
        csb = csb.withSslContext(SSLContext.getDefault());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return csb.build();
  }

  private Map<String, String> createTestServerConfig() {
    return new HashMap<String, String>() {{
      put("keyspace", KEYSPACE_NAME);
      put("username", _cassandraContainer.getUsername());
      put("password", _cassandraContainer.getPassword());
      put("hosts", _cassandraContainer.getHost());
      put("port", _cassandraContainer.getMappedPort(9042).toString());
      put("datacenter", "datacenter1");
      put("useSsl", "false");
    }};
  }

  @BeforeClass
  public void setupContainer() {
    final DockerImageName imageName = DockerImageName
            .parse("cassandra:3.11")
            .asCompatibleSubstituteFor("cassandra");

    _cassandraContainer = new CassandraContainer(imageName);
    _cassandraContainer.withEnv("JVM_OPTS", "-Xms64M -Xmx64M");
    _cassandraContainer.start();

    try (Session session = _cassandraContainer.getCluster().connect()) {

      session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = \n"
              + "{'class':'SimpleStrategy','replication_factor':'1'};", KEYSPACE_NAME));
      session.execute(
              String.format("create table %s.%s (urn varchar, \n"
                              + "aspect varchar, \n"
                              + "systemmetadata varchar, \n"
                              + "version bigint, \n"
                              + "metadata text, \n"
                              + "createdon timestamp, \n"
                              + "createdby varchar, \n"
                              + "createdfor varchar, \n"
                              + "entity varchar, \n"
                              + "primary key ((urn), aspect, version)) \n"
                              + "with clustering order by (aspect asc, version asc);",
                      KEYSPACE_NAME,
                      CassandraAspect.TABLE_NAME));

      List<KeyspaceMetadata> keyspaces = session.getCluster().getMetadata().getKeyspaces();
      List<KeyspaceMetadata> filteredKeyspaces = keyspaces
              .stream()
              .filter(km -> km.getName().equals(KEYSPACE_NAME))
              .collect(Collectors.toList());

      assertEquals(filteredKeyspaces.size(), 1);
    }
  }

  @AfterClass
  public void tearDown() {
    _cassandraContainer.stop();
  }

  @BeforeMethod
  public void setupTest() {
    try (Session session = _cassandraContainer.getCluster().connect()) {
      session.execute(String.format("TRUNCATE %s.%s;", KEYSPACE_NAME, CassandraAspect.TABLE_NAME));
      List<Row> rs = session.execute(String.format("SELECT * FROM %s.%s;", KEYSPACE_NAME, CassandraAspect.TABLE_NAME))
              .all();
      assertEquals(rs.size(), 0);
    }

    CqlSession session = createTestSession();
    _aspectDao = new CassandraAspectDao(session);
    _mockProducer = mock(EventProducer.class);
    _entityService = new CassandraEntityService(_aspectDao, _mockProducer, _testEntityRegistry);
    _retentionService = new CassandraRetentionService(_entityService, session, 1000);
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
    CassandraAspect readCassandra1 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 1);
    CassandraAspect readCassandra2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

    assertTrue(DataTemplateUtil.areEqual(writeAspect2, readAspect2));
    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readCassandra2.getSystemMetadata()), metadata2));
    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readCassandra1.getSystemMetadata()), metadata1));

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
    CassandraAspect readCassandra1 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 1);
    CassandraAspect readCassandra2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

    assertTrue(DataTemplateUtil.areEqual(writeAspect2, new CorpUserInfo(readAspect2.getValue().data())));
    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readCassandra2.getSystemMetadata()), metadata2));
    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readCassandra1.getSystemMetadata()), metadata1));

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
    CassandraAspect readCassandra2 = _aspectDao.getAspect(entityUrn.toString(), aspectName, 0);

    assertTrue(DataTemplateUtil.areEqual(writeAspect2, readAspect2));
    assertFalse(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readCassandra2.getSystemMetadata()), metadata2));
    assertFalse(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readCassandra2.getSystemMetadata()), metadata1));

    SystemMetadata metadata3 = new SystemMetadata();
    metadata3.setLastObserved(1635792689);
    metadata3.setRunId("run-123");

    assertTrue(DataTemplateUtil.areEqual(EntityUtils.parseSystemMetadata(readCassandra2.getSystemMetadata()), metadata3));

    verify(_mockProducer, times(0)).produceMetadataChangeLog(Mockito.eq(entityUrn), Mockito.any(), mclCaptor.capture());

    verify(_mockProducer, times(0)).produceMetadataAuditEvent(Mockito.eq(entityUrn), Mockito.notNull(), Mockito.any(),
        Mockito.any(), Mockito.any(), Mockito.eq(MetadataAuditOperation.UPDATE));

    verifyNoMoreInteractions(_mockProducer);
  }

  @Test
  public void testIngestListLatestAspects() throws Exception {
    // TODO: this test should be in the base class but with current schema cassandra limitations make it difficult to achieve the same ordering as in ebean land
    Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:test1");
    Urn entityUrn2 = Urn.createFromString("urn:li:corpuser:test2");
    Urn entityUrn3 = Urn.createFromString("urn:li:corpuser:test3");

    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setLastObserved(1625792689);
    metadata1.setRunId("run-123");

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

    // List aspects
    ListResult<RecordTemplate> batch1 = _entityService.listLatestAspects(entityUrn1.getEntityType(), aspectName, 0, 2);

    assertEquals(2, batch1.getNextStart());
    assertEquals(2, batch1.getPageSize());
    assertEquals(3, batch1.getTotalCount());
    assertEquals(2, batch1.getTotalPageCount());
    assertEquals(2, batch1.getValues().size());
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, batch1.getValues().get(0)));
    assertTrue(DataTemplateUtil.areEqual(writeAspect3, batch1.getValues().get(1)));

    ListResult<RecordTemplate> batch2 = _entityService.listLatestAspects(entityUrn1.getEntityType(), aspectName, 2, 2);
    assertEquals(1, batch2.getValues().size());
    assertTrue(DataTemplateUtil.areEqual(writeAspect2, batch2.getValues().get(0)));
  }

  @Test
  public void testIngestListUrns() throws Exception {
    // TODO: this test should be in the base class but with current schema cassandra limitations make it difficult to achieve the same ordering as in ebean land
    Urn entityUrn1 = Urn.createFromString("urn:li:corpuser:test1");
    Urn entityUrn2 = Urn.createFromString("urn:li:corpuser:test2");
    Urn entityUrn3 = Urn.createFromString("urn:li:corpuser:test3");

    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setLastObserved(1625792689);
    metadata1.setRunId("run-123");

    String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserKey().schema());

    // Ingest CorpUserInfo Aspect #1
    RecordTemplate writeAspect1 = createCorpUserKey(entityUrn1);
    _entityService.ingestAspect(entityUrn1, aspectName, writeAspect1, TEST_AUDIT_STAMP, metadata1);

    // Ingest CorpUserInfo Aspect #2
    RecordTemplate writeAspect2 = createCorpUserKey(entityUrn2);
    _entityService.ingestAspect(entityUrn2, aspectName, writeAspect2, TEST_AUDIT_STAMP, metadata1);

    // Ingest CorpUserInfo Aspect #3
    RecordTemplate writeAspect3 = createCorpUserKey(entityUrn3);
    _entityService.ingestAspect(entityUrn3, aspectName, writeAspect3, TEST_AUDIT_STAMP, metadata1);

    // List aspects urns
    ListUrnsResult batch1 = _entityService.listUrns(entityUrn1.getEntityType(), 0, 2);

    assertEquals(0, (int) batch1.getStart());
    assertEquals(2, (int) batch1.getCount());
    assertEquals(3, (int) batch1.getTotal());
    assertEquals(2, batch1.getEntities().size());
    assertEquals(entityUrn1.toString(), batch1.getEntities().get(0).toString());
    assertEquals(entityUrn3.toString(), batch1.getEntities().get(1).toString());

    ListUrnsResult batch2 = _entityService.listUrns(entityUrn1.getEntityType(), 2, 2);

    assertEquals(2, (int) batch2.getStart());
    assertEquals(1, (int) batch2.getCount());
    assertEquals(3, (int) batch2.getTotal());
    assertEquals(1, batch2.getEntities().size());
    assertEquals(entityUrn2.toString(), batch2.getEntities().get(0).toString());
  }
}
