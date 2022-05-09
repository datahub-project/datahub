package com.linkedin.metadata.timeline;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.cassandra.CassandraAspect;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
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
import static org.testng.Assert.assertEquals;

public class CassandraTimelineServiceTest extends TimelineServiceTestBase<CassandraAspectDao> {

  private CassandraContainer _cassandraContainer;
  private static final String KEYSPACE_NAME = "test";

  public CassandraTimelineServiceTest() throws EntityRegistryException {
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
      List<Row> rs = session.execute(String.format("SELECT * FROM %s.%s;", KEYSPACE_NAME, CassandraAspect.TABLE_NAME)).all();
      assertEquals(rs.size(), 0);
    }

    CqlSession session = createTestSession();
    _aspectDao = new CassandraAspectDao(session);
    _entityTimelineService = new TimelineServiceImpl(_aspectDao);
    _mockProducer = mock(EventProducer.class);
    _entityService = new EntityService(_aspectDao, _mockProducer, _testEntityRegistry);
  }

  @Test
  public void obligatoryTest() throws Exception {
    // We need this method to make test framework pick this class up.
    // All real tests are in the base class.
    Assert.assertTrue(true);
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
}
