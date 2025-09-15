package com.linkedin.metadata;

import static org.testng.Assert.assertEquals;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.linkedin.metadata.entity.cassandra.CassandraAspect;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class CassandraTestUtils {

  private CassandraTestUtils() {}

  private static final String KEYSPACE_NAME = "test";
  private static final String IMAGE_NAME = "cassandra:3.11";

  public static CassandraContainer setupContainer() {
    final DockerImageName imageName = DockerImageName.parse(IMAGE_NAME);

    CassandraContainer container =
        new CassandraContainer(imageName)
            .withEnv("JVM_OPTS", "-Xms128M -Xmx128M -Djava.net.preferIPv4Stack=true")
            .withStartupTimeout(Duration.ofMinutes(2))
            .withReuse(true);

    container.start();

    Cluster cluster = null;
    Session session = null;
    Exception lastException = null;

    for (int i = 0; i < 30; i++) { // Try for up to 60 seconds
      try {
        cluster =
            Cluster.builder()
                .addContactPointsWithPorts(container.getContactPoint())
                .withCredentials(container.getUsername(), container.getPassword())
                .withoutJMXReporting()
                .build();

        session = cluster.connect();
        break;
      } catch (Exception e) {
        lastException = e;
        if (cluster != null) {
          try {
            cluster.close();
          } catch (Exception ignored) {
          }
        }
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for Cassandra", ie);
        }
      }
    }

    if (session == null) {
      throw new RuntimeException("Could not connect to Cassandra container", lastException);
    }

    try {
      session.execute(
          String.format(
              "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = \n"
                  + "{'class':'SimpleStrategy','replication_factor':'1'};",
              KEYSPACE_NAME));

      session.execute(
          String.format(
              "create table %s.%s (urn varchar, \n"
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
              KEYSPACE_NAME, CassandraAspect.TABLE_NAME));

      List<KeyspaceMetadata> keyspaces = cluster.getMetadata().getKeyspaces();
      List<KeyspaceMetadata> filteredKeyspaces =
          keyspaces.stream()
              .filter(km -> km.getName().equals(KEYSPACE_NAME))
              .collect(Collectors.toList());

      assertEquals(filteredKeyspaces.size(), 1);
    } finally {
      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }

    return container;
  }

  @Nonnull
  public static CqlSession createTestSession(@Nonnull final CassandraContainer container) {
    Map<String, String> sessionConfig = createTestServerConfig(container);
    int port = Integer.parseInt(sessionConfig.get("port"));
    List<InetSocketAddress> addresses =
        Arrays.stream(sessionConfig.get("hosts").split(","))
            .map(host -> new InetSocketAddress(host, port))
            .collect(Collectors.toList());

    String dc = sessionConfig.get("datacenter");
    String ks = sessionConfig.get("keyspace");
    String username = sessionConfig.get("username");
    String password = sessionConfig.get("password");

    CqlSessionBuilder csb =
        CqlSession.builder()
            .addContactPoints(addresses)
            .withLocalDatacenter(dc)
            .withKeyspace(ks)
            .withAuthCredentials(username, password);

    if (sessionConfig.containsKey("useSsl") && sessionConfig.get("useSsl").equals("true")) {
      try {
        csb = csb.withSslContext(SSLContext.getDefault());
      } catch (Exception e) {
        log.error("Failed to connect using SSL", e);
      }
    }

    return csb.build();
  }

  @Nonnull
  private static Map<String, String> createTestServerConfig(
      @Nonnull final CassandraContainer container) {
    return new HashMap<String, String>() {
      {
        put("keyspace", KEYSPACE_NAME);
        put("username", container.getUsername());
        put("password", container.getPassword());
        put("hosts", container.getHost());
        put("port", container.getMappedPort(9042).toString());
        put("datacenter", container.getLocalDatacenter()); // Use the new method
        put("useSsl", "false");
      }
    };
  }

  public static void purgeData(CassandraContainer container) {
    // Create a new cluster connection for purging
    try (Cluster cluster =
            Cluster.builder()
                .addContactPointsWithPorts(container.getContactPoint())
                .withCredentials(container.getUsername(), container.getPassword())
                .withoutJMXReporting()
                .build();
        Session session = cluster.connect()) {

      session.execute(String.format("TRUNCATE %s.%s;", KEYSPACE_NAME, CassandraAspect.TABLE_NAME));
      List<Row> rs =
          session
              .execute(
                  String.format("SELECT * FROM %s.%s;", KEYSPACE_NAME, CassandraAspect.TABLE_NAME))
              .all();
      assertEquals(rs.size(), 0);
    }
  }
}
