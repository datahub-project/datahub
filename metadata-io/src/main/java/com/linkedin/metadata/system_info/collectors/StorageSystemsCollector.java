package com.linkedin.metadata.system_info.collectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.system_info.SystemInfoDtos.*;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.neo4j.driver.Driver;
import org.opensearch.client.Node;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StorageSystemsCollector {
  @Autowired(required = false)
  private Database ebeanDatabase;

  @Autowired(required = false)
  private RestClientBuilder elasticsearchClientBuilder;

  @Autowired(required = false)
  private Driver neo4jDriver;

  @Autowired(required = false)
  private CqlSession cqlSession;

  public StorageInfo collect(ExecutorService executorService) {
    Map<String, CompletableFuture<StorageSystemInfo>> futures = new HashMap<>();

    futures.put("mysql", CompletableFuture.supplyAsync(this::getMySQLInfo, executorService));
    futures.put(
        "postgres", CompletableFuture.supplyAsync(this::getPostgreSQLInfo, executorService));
    futures.put(
        "elasticsearch",
        CompletableFuture.supplyAsync(this::getElasticsearchInfo, executorService));
    futures.put(
        "opensearch", CompletableFuture.supplyAsync(this::getOpenSearchInfo, executorService));
    futures.put("neo4j", CompletableFuture.supplyAsync(this::getNeo4jInfo, executorService));
    futures.put(
        "cassandra", CompletableFuture.supplyAsync(this::getCassandraInfo, executorService));

    try {
      CompletableFuture<Void> allFutures =
          CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]));
      allFutures.get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.error("Timeout waiting for storage info", e);
    }

    return StorageInfo.builder()
        .mysql(getOrDefault(futures.get("mysql"), createUnavailableStorage("MySQL")))
        .postgres(getOrDefault(futures.get("postgres"), createUnavailableStorage("PostgreSQL")))
        .elasticsearch(
            getOrDefault(futures.get("elasticsearch"), createUnavailableStorage("Elasticsearch")))
        .opensearch(getOrDefault(futures.get("opensearch"), createUnavailableStorage("OpenSearch")))
        .neo4j(getOrDefault(futures.get("neo4j"), createUnavailableStorage("Neo4j")))
        .cassandra(getOrDefault(futures.get("cassandra"), createUnavailableStorage("Cassandra")))
        .build();
  }

  private StorageSystemInfo getMySQLInfo() {
    return getRelationalDatabaseInfo("mysql");
  }

  private StorageSystemInfo getPostgreSQLInfo() {
    return getRelationalDatabaseInfo("postgresql");
  }

  private StorageSystemInfo getRelationalDatabaseInfo(String dbType) {
    try {
      if (ebeanDatabase != null) {
        DataSource dataSource = ebeanDatabase.dataSource();
        if (dataSource != null) {
          try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            String productName = metaData.getDatabaseProductName();

            if (productName != null && productName.toLowerCase().contains(dbType)) {
              String displayName = dbType.equals("mysql") ? "MySQL" : "PostgreSQL";
              return StorageSystemInfo.builder()
                  .type(displayName)
                  .available(true)
                  .version(metaData.getDatabaseProductVersion())
                  .driverVersion(metaData.getDriverVersion())
                  .connectionUrl(metaData.getURL())
                  .metadata(
                      Map.of(
                          "driverName", metaData.getDriverName(),
                          "databaseProductName", productName,
                          "maxConnections", metaData.getMaxConnections(),
                          "defaultTransactionIsolation", metaData.getDefaultTransactionIsolation(),
                          "supportsTransactions", metaData.supportsTransactions()))
                  .build();
            }
          }
        }
      }
    } catch (SQLException e) {
      log.debug("Error checking {}: {}", dbType, e.getMessage());
      String displayName = dbType.equals("mysql") ? "MySQL" : "PostgreSQL";
      return createErrorStorage(displayName, "Connection error: " + e.getMessage());
    } catch (Exception e) {
      log.error("Unexpected error checking {}", dbType, e);
      String displayName = dbType.equals("mysql") ? "MySQL" : "PostgreSQL";
      return createErrorStorage(displayName, e.getMessage());
    }

    String displayName = dbType.equals("mysql") ? "MySQL" : "PostgreSQL";
    return createUnavailableStorage(displayName);
  }

  private StorageSystemInfo getElasticsearchInfo() {
    try {
      if (elasticsearchClientBuilder != null) {
        try (RestClient restClient = elasticsearchClientBuilder.build()) {
          List<Node> nodes = restClient.getNodes();
          if (nodes.isEmpty()) {
            return createUnavailableStorage("Elasticsearch");
          }

          // Make a raw HTTP request to check if it's actually Elasticsearch
          String firstNodeUrl = nodes.get(0).getHost().toURI();
          try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(firstNodeUrl + "/");
            request.setHeader("Accept", "application/json");

            try (CloseableHttpResponse response = httpClient.execute(request)) {
              if (response.getCode() == 200) {
                String json = EntityUtils.toString(response.getEntity());
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> responseBody =
                    mapper.readValue(json, new TypeReference<Map<String, Object>>() {});

                Map<String, Object> version = (Map<String, Object>) responseBody.get("version");
                if (version != null) {
                  // Check if it's NOT OpenSearch
                  String distribution =
                      version.get("distribution") != null
                          ? version.get("distribution").toString()
                          : "elasticsearch";

                  if (!"opensearch".equalsIgnoreCase(distribution)) {
                    // It's Elasticsearch
                    String connectionUrl =
                        nodes.stream()
                            .map(node -> node.getHost().toURI())
                            .collect(Collectors.joining(", "));

                    return StorageSystemInfo.builder()
                        .type("Elasticsearch")
                        .available(true)
                        .version(version.get("number").toString())
                        .connectionUrl(connectionUrl)
                        .metadata(
                            Map.of(
                                "clusterName", responseBody.get("cluster_name").toString(),
                                "clusterUuid", responseBody.get("cluster_uuid").toString(),
                                "buildType",
                                    version.getOrDefault("build_type", "unknown").toString(),
                                "buildHash",
                                    version.getOrDefault("build_hash", "unknown").toString()))
                        .build();
                  }
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      log.debug("Not Elasticsearch or error: {}", e.getMessage());
    }

    return createUnavailableStorage("Elasticsearch");
  }

  private StorageSystemInfo getOpenSearchInfo() {
    try {
      if (elasticsearchClientBuilder != null) {
        // First, check if this is OpenSearch by making a raw HTTP request
        try (RestClient restClient = elasticsearchClientBuilder.build()) {
          List<Node> nodes = restClient.getNodes();
          if (nodes.isEmpty()) {
            return createUnavailableStorage("OpenSearch");
          }

          // Make a raw HTTP request to check the response
          String firstNodeUrl = nodes.get(0).getHost().toURI();
          try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(firstNodeUrl + "/");
            request.setHeader("Accept", "application/json");

            try (CloseableHttpResponse response = httpClient.execute(request)) {
              if (response.getCode() == 200) {
                String json = EntityUtils.toString(response.getEntity());
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> responseBody =
                    mapper.readValue(json, new TypeReference<Map<String, Object>>() {});

                Map<String, Object> version = (Map<String, Object>) responseBody.get("version");
                if (version != null
                    && version.containsKey("distribution")
                    && "opensearch".equalsIgnoreCase(version.get("distribution").toString())) {

                  // It's OpenSearch
                  String connectionUrl =
                      nodes.stream()
                          .map(node -> node.getHost().toURI())
                          .collect(Collectors.joining(", "));

                  return StorageSystemInfo.builder()
                      .type("OpenSearch")
                      .available(true)
                      .version(version.get("number").toString())
                      .connectionUrl(connectionUrl)
                      .metadata(
                          Map.of(
                              "distribution",
                                  version.getOrDefault("distribution", "opensearch").toString(),
                              "build_type",
                                  version.getOrDefault("build_type", "unknown").toString(),
                              "build_hash",
                                  version.getOrDefault("build_hash", "unknown").toString()))
                      .build();
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      log.debug("Not OpenSearch or error: {}", e.getMessage());
    }

    return createUnavailableStorage("OpenSearch");
  }

  private StorageSystemInfo getNeo4jInfo() {
    try {
      if (neo4jDriver != null) {
        try (var session = neo4jDriver.session()) {
          var result =
              session.run("CALL dbms.components() YIELD name, versions RETURN name, versions");
          if (result.hasNext()) {
            var record = result.next();
            return StorageSystemInfo.builder()
                .type("Neo4j")
                .available(true)
                .version(record.get("versions").asList().get(0).toString())
                .metadata(Map.of("component", record.get("name").asString()))
                .build();
          }
        }
      }
    } catch (Exception e) {
      return createErrorStorage("Neo4j", e.getMessage());
    }

    return createUnavailableStorage("Neo4j");
  }

  private StorageSystemInfo getCassandraInfo() {
    // Implementation would depend on Cassandra driver if available
    return createUnavailableStorage("Cassandra");
  }

  private <T> T getOrDefault(CompletableFuture<T> future, T defaultValue) {
    try {
      return future.getNow(defaultValue);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  private StorageSystemInfo createUnavailableStorage(String type) {
    return StorageSystemInfo.builder()
        .type(type)
        .available(false)
        .errorMessage(type + " not configured")
        .build();
  }

  private StorageSystemInfo createErrorStorage(String type, String errorMessage) {
    return StorageSystemInfo.builder()
        .type(type)
        .available(false)
        .errorMessage(errorMessage)
        .build();
  }
}
