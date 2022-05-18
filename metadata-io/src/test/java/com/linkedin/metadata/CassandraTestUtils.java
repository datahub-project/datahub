package com.linkedin.metadata;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.linkedin.metadata.entity.cassandra.CassandraAspect;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class CassandraTestUtils {

  private CassandraTestUtils() {
  }

  private static final String KEYSPACE_NAME = "test";
  private static final String IMAGE_NAME = "cassandra:3.11";

  public static CassandraContainer setupContainer() {
    final DockerImageName imageName = DockerImageName
        .parse(IMAGE_NAME)
        .asCompatibleSubstituteFor("cassandra");

    CassandraContainer container = new CassandraContainer(imageName);
    container.withEnv("JVM_OPTS", "-Xms64M -Xmx64M");
    container.start();

    try (Session session = container.getCluster().connect()) {
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
    return container;
  }

  @Nonnull
  public static CqlSession createTestSession(@Nonnull final CassandraContainer container) {
    Map<String, String> sessionConfig = createTestServerConfig(container);
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

  @Nonnull
  private static Map<String, String> createTestServerConfig(@Nonnull final CassandraContainer container) {
    return new HashMap<String, String>() {{
      put("keyspace", KEYSPACE_NAME);
      put("username", container.getUsername());
      put("password", container.getPassword());
      put("hosts", container.getHost());
      put("port", container.getMappedPort(9042).toString());
      put("datacenter", "datacenter1");
      put("useSsl", "false");
    }};
  }

  public static void purgeData(CassandraContainer container) {
    try (Session session = container.getCluster().connect()) {
      session.execute(String.format("TRUNCATE %s.%s;", KEYSPACE_NAME, CassandraAspect.TABLE_NAME));
      List<Row> rs = session.execute(String.format("SELECT * FROM %s.%s;", KEYSPACE_NAME, CassandraAspect.TABLE_NAME)).all();
      assertEquals(rs.size(), 0);
    }
  }
}
