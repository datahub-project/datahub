package com.linkedin.metadata.entity;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.google.common.base.Equivalence;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.entity.datastax.DatastaxAspect;
import com.linkedin.metadata.entity.datastax.DatastaxAspectDao;
import com.linkedin.metadata.entity.datastax.DatastaxEntityService;
import com.linkedin.metadata.event.EntityEventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.mxe.SystemMetadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DatastaxEntityServiceTest extends EntityServiceTestBase {

  private CassandraContainer _cassandraContainer;

  @BeforeMethod
  public void setupTest() {

    final DockerImageName imageName = DockerImageName
            .parse("cassandra:3.11")
            .asCompatibleSubstituteFor("cassandra");

    _cassandraContainer = new CassandraContainer(imageName);
    _cassandraContainer.start();

    // Setup
    Cluster cluster = _cassandraContainer.getCluster();
    final String keyspaceName = "test";
    final String tableName = DatastaxAspect.TABLE_NAME;

    try (Session session = cluster.connect()) {

      session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = \n"
              + "{'class':'SimpleStrategy','replication_factor':'1'};", keyspaceName));
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
                      + "PRIMARY KEY (urn,aspect,version));",
              keyspaceName,
              tableName));

      List<KeyspaceMetadata> keyspaces = session.getCluster().getMetadata().getKeyspaces();
                List<KeyspaceMetadata> filteredKeyspaces = keyspaces
                  .stream()
                  .filter(km -> km.getName().equals(keyspaceName))
                  .collect(Collectors.toList());

                assertEquals(filteredKeyspaces.size(), 1);
    }

    Map<String, String> serverConfig = new HashMap<String, String>() {{
      put("keyspace", keyspaceName);
      put("username", _cassandraContainer.getUsername());
      put("password", _cassandraContainer.getPassword());
      put("hosts", _cassandraContainer.getHost());
      put("port", _cassandraContainer.getMappedPort(9042).toString());
      put("datacenter", "datacenter1");
      put("useSsl", "false");
    }};
    DatastaxAspectDao aspectDao = new DatastaxAspectDao(serverConfig);
    _aspectDao = aspectDao;
    _mockProducer = mock(EntityEventProducer.class);
    _entityService = new DatastaxEntityService(aspectDao, _mockProducer, _testEntityRegistry);
  }

  @Test
  public void testIngestListLatestAspects() throws Exception {
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

    assertEquals(batch1.getNextStart(), 2);
    assertEquals(batch1.getPageSize(), 2);
    assertEquals(batch1.getTotalCount(), 3);
    assertEquals(batch1.getTotalPageCount(), 2);
    assertEquals(batch1.getValues().size(), 2);


    ListResult<RecordTemplate> batch2 = _entityService.listLatestAspects(entityUrn1.getEntityType(), aspectName, 2, 2);
    assertEquals(1, batch2.getValues().size());

    // https://stackoverflow.com/questions/14880450/java-hashset-with-a-custom-equality-criteria
    Equivalence<RecordTemplate> recordTemplateEquivalence = new Equivalence<RecordTemplate>() {
      @Override
      protected boolean doEquivalent(RecordTemplate a, RecordTemplate b) {
        return DataTemplateUtil.areEqual(a, b);
      }

      @Override
      protected int doHash(RecordTemplate item) {
        return item.hashCode();
      }
    };

    Set<Equivalence.Wrapper<RecordTemplate>> expectedEntities = new HashSet<Equivalence.Wrapper<RecordTemplate>>() {{
      add(recordTemplateEquivalence.wrap(writeAspect1));
      add(recordTemplateEquivalence.wrap(writeAspect2));
      add(recordTemplateEquivalence.wrap(writeAspect3));
    }};

    Set<Equivalence.Wrapper<RecordTemplate>> actualEntities = new HashSet<Equivalence.Wrapper<RecordTemplate>>() {{
      add(recordTemplateEquivalence.wrap(batch1.getValues().get(0)));
      add(recordTemplateEquivalence.wrap(batch1.getValues().get(1)));
      add(recordTemplateEquivalence.wrap(batch2.getValues().get(0)));
    }};

    assertTrue(actualEntities.equals(expectedEntities));
  }

  @Test
  public void testIngestListUrns() throws Exception {
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
    ListUrnsResult batch1 = _entityService.listUrns(entityUrn1.getEntityType(),  0, 2);

    assertEquals(0, (int) batch1.getStart());
    assertEquals(2, (int) batch1.getCount());
    assertEquals(3, (int) batch1.getTotal());
    assertEquals(2, batch1.getEntities().size());

    final Set<String> urns = new HashSet<>();
    urns.add(entityUrn1.toString());
    urns.add(entityUrn2.toString());
    urns.add(entityUrn3.toString());

    urns.remove(batch1.getEntities().get(0).toString());
    urns.remove(batch1.getEntities().get(1).toString());

    ListUrnsResult batch2 = _entityService.listUrns(entityUrn1.getEntityType(),  2, 2);

    assertEquals(2, (int) batch2.getStart());
    assertEquals(1, (int) batch2.getCount());
    assertEquals(3, (int) batch2.getTotal());
    assertEquals(1, batch2.getEntities().size());
    urns.remove(batch2.getEntities().get(0).toString());

    assertEquals(0, urns.size());
  }

  @AfterTest
  public void tearDown() {
    _cassandraContainer.stop();
  }
}