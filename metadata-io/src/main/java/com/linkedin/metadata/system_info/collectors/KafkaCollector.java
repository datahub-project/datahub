package com.linkedin.metadata.system_info.collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.system_info.SystemInfoDtos.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaCollector {

  @Autowired(required = false)
  @Qualifier("schemaRegistryConfig")
  private KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig;

  @Autowired(required = false)
  @Qualifier("systemInfoKafkaProducerConfig")
  private Map<String, Object> kafkaProducerConfig;

  @Autowired(required = false)
  @Qualifier("systemInfoKafkaConsumerConfig")
  private Map<String, Object> kafkaConsumerConfig;

  public KafkaInfo collect(ExecutorService executorService) {
    // Check if Kafka is configured by looking for configs
    if (kafkaProducerConfig == null && kafkaConsumerConfig == null) {
      return KafkaInfo.builder().available(false).errorMessage("Kafka not configured").build();
    }

    try {
      // Execute operations in parallel
      CompletableFuture<SchemaRegistryInfo> schemaRegistryFuture =
          CompletableFuture.supplyAsync(this::getSchemaRegistryInfo, executorService);

      // Wait for schema registry future
      schemaRegistryFuture.get(10, TimeUnit.SECONDS);

      // Extract and merge configurations - using TreeMap for sorted output
      Map<String, Object> mergedConfig = new TreeMap<>();
      Map<String, Object> metadata = new TreeMap<>();

      // Add producer configuration
      if (kafkaProducerConfig != null) {
        kafkaProducerConfig.forEach(
            (key, value) -> mergedConfig.put("producer." + key, sanitizeValue(key, value)));

        // Extract key producer metadata
        extractProducerMetadata(kafkaProducerConfig, metadata);
      }

      // Add consumer configuration
      if (kafkaConsumerConfig != null) {
        kafkaConsumerConfig.forEach(
            (key, value) -> {
              // Avoid duplicating common properties like bootstrap.servers
              if (!isCommonProperty(key) || !mergedConfig.containsKey("producer." + key)) {
                mergedConfig.put("consumer." + key, sanitizeValue(key, value));
              }
            });

        // Extract key consumer metadata
        extractConsumerMetadata(kafkaConsumerConfig, metadata);
      }

      // Extract bootstrap servers and broker info
      String bootstrapServers = extractBootstrapServers();
      List<BrokerInfo> brokers = extractBrokerInfo(bootstrapServers);

      return KafkaInfo.builder()
          .available(true)
          .version(extractKafkaVersion())
          .brokers(brokers)
          .schemaRegistry(
              schemaRegistryFuture.getNow(SchemaRegistryInfo.builder().available(false).build()))
          .topics(new ArrayList<>()) // Topics can't be listed without admin client
          .configuration(mergedConfig)
          .metadata(metadata)
          .build();

    } catch (Exception e) {
      log.error("Failed to get Kafka info", e);
      return KafkaInfo.builder()
          .available(false)
          .errorMessage("Failed to collect Kafka info: " + e.getMessage())
          .build();
    }
  }

  private void extractProducerMetadata(Map<String, Object> config, Map<String, Object> metadata) {
    // Extract key producer settings that are interesting for monitoring
    metadata.put("producer.compressionType", config.get("compression.type"));
    metadata.put("producer.acks", config.get("acks"));
    metadata.put("producer.retries", config.get("retries"));
    metadata.put("producer.batchSize", config.get("batch.size"));
    metadata.put("producer.lingerMs", config.get("linger.ms"));
    metadata.put("producer.bufferMemory", config.get("buffer.memory"));
    metadata.put("producer.maxRequestSize", config.get("max.request.size"));
    metadata.put("producer.deliveryTimeoutMs", config.get("delivery.timeout.ms"));
    metadata.put("producer.requestTimeoutMs", config.get("request.timeout.ms"));
    metadata.put("producer.retryBackoffMs", config.get("retry.backoff.ms"));
    metadata.put(
        "producer.maxInFlightRequests", config.get("max.in.flight.requests.per.connection"));
    metadata.put("producer.enableIdempotence", config.get("enable.idempotence"));
  }

  private void extractConsumerMetadata(Map<String, Object> config, Map<String, Object> metadata) {
    // Extract key consumer settings
    metadata.put("consumer.groupId", config.get("group.id"));
    metadata.put("consumer.autoOffsetReset", config.get("auto.offset.reset"));
    metadata.put("consumer.enableAutoCommit", config.get("enable.auto.commit"));
    metadata.put("consumer.autoCommitIntervalMs", config.get("auto.commit.interval.ms"));
    metadata.put("consumer.maxPollRecords", config.get("max.poll.records"));
    metadata.put("consumer.maxPollIntervalMs", config.get("max.poll.interval.ms"));
    metadata.put("consumer.sessionTimeoutMs", config.get("session.timeout.ms"));
    metadata.put("consumer.heartbeatIntervalMs", config.get("heartbeat.interval.ms"));
    metadata.put("consumer.fetchMinBytes", config.get("fetch.min.bytes"));
    metadata.put("consumer.fetchMaxWaitMs", config.get("fetch.max.wait.ms"));
    metadata.put("consumer.maxPartitionFetchBytes", config.get("max.partition.fetch.bytes"));
    metadata.put("consumer.isolationLevel", config.get("isolation.level"));
  }

  private String extractKafkaVersion() {
    // Try to infer version from client version in properties
    if (kafkaProducerConfig != null && kafkaProducerConfig.containsKey("client.id")) {
      // Sometimes the client includes version info
      String clientId = String.valueOf(kafkaProducerConfig.get("client.id"));
      if (clientId.contains("kafka") && clientId.contains(".")) {
        return clientId;
      }
    }

    // Check for any version-related properties
    Map<String, Object> config =
        kafkaProducerConfig != null ? kafkaProducerConfig : kafkaConsumerConfig;
    if (config != null) {
      // Some deployments include version in metadata
      if (config.containsKey("kafka.version")) {
        return String.valueOf(config.get("kafka.version"));
      }
      if (config.containsKey("interceptor.classes")) {
        // Sometimes version is embedded in interceptor class names
        String interceptors = String.valueOf(config.get("interceptor.classes"));
        if (interceptors.contains("kafka") && interceptors.contains(".")) {
          // Try to extract version from class name
          return "inferred from interceptors";
        }
      }
    }

    return "unknown";
  }

  private Object sanitizeValue(String key, Object value) {
    // Redact sensitive information
    if (key.toLowerCase().contains("password")
        || key.toLowerCase().contains("auth")
        || key.toLowerCase().contains("secret")
        || key.toLowerCase().contains("credential")
        || key.toLowerCase().contains("token")) {
      return "***REDACTED***";
    }

    // Handle byte arrays and other non-serializable types
    if (value instanceof byte[]) {
      return "***BINARY***";
    }

    // Convert Class objects to their names
    if (value instanceof Class<?>) {
      return ((Class<?>) value).getName();
    }

    return value;
  }

  private boolean isCommonProperty(String key) {
    // Properties that are typically the same for producers and consumers
    return key.equals("bootstrap.servers")
        || key.equals("security.protocol")
        || key.equals("client.id")
        || key.startsWith("ssl.")
        || key.startsWith("sasl.")
        || key.startsWith("schema.registry.")
        || key.equals("metrics.recording.level")
        || key.equals("metric.reporters");
  }

  private String extractBootstrapServers() {
    // Try producer config first
    if (kafkaProducerConfig != null && kafkaProducerConfig.containsKey("bootstrap.servers")) {
      return String.valueOf(kafkaProducerConfig.get("bootstrap.servers"));
    }

    // Fall back to consumer config
    if (kafkaConsumerConfig != null && kafkaConsumerConfig.containsKey("bootstrap.servers")) {
      return String.valueOf(kafkaConsumerConfig.get("bootstrap.servers"));
    }

    return null;
  }

  private List<BrokerInfo> extractBrokerInfo(String bootstrapServers) {
    List<BrokerInfo> brokers = new ArrayList<>();

    if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
      // Handle both List and String formats
      String serversString = bootstrapServers;
      if (bootstrapServers.startsWith("[") && bootstrapServers.endsWith("]")) {
        // Remove brackets and parse as list
        serversString = bootstrapServers.substring(1, bootstrapServers.length() - 1);
      }

      String[] servers = serversString.split(",");
      for (int i = 0; i < servers.length; i++) {
        String server = servers[i].trim();
        String[] parts = server.split(":");
        if (parts.length == 2) {
          try {
            brokers.add(
                BrokerInfo.builder()
                    .id(i)
                    .host(parts[0])
                    .port(Integer.parseInt(parts[1]))
                    .controller(false) // Can't determine without admin client
                    .build());
          } catch (NumberFormatException e) {
            log.debug("Failed to parse port for broker: {}", server);
          }
        }
      }
    }

    return brokers;
  }

  private SchemaRegistryInfo getSchemaRegistryInfo() {
    String registryUrl = getSchemaRegistryUrl();

    if (registryUrl == null || registryUrl.isEmpty()) {
      return SchemaRegistryInfo.builder()
          .available(false)
          .errorMessage("Schema Registry URL not configured")
          .build();
    }

    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectionRequestTimeout(Timeout.ofSeconds(5))
            .setResponseTimeout(Timeout.ofSeconds(5))
            .build();

    try (CloseableHttpClient httpClient =
        HttpClients.custom().setDefaultRequestConfig(requestConfig).build()) {

      HttpGet subjectsRequest = new HttpGet(registryUrl + "/subjects");
      subjectsRequest.setHeader("Accept", "application/json");
      addSchemaRegistryAuth(subjectsRequest);

      try (CloseableHttpResponse subjectsResponse = httpClient.execute(subjectsRequest)) {
        if (subjectsResponse.getCode() == 200) {
          String subjectsJson = EntityUtils.toString(subjectsResponse.getEntity());
          ObjectMapper mapper = new ObjectMapper();
          List<String> subjects =
              mapper.readValue(subjectsJson, new TypeReference<List<String>>() {});

          String version = getSchemaRegistryVersion(httpClient, registryUrl);
          Map<String, Object> additionalInfo =
              getSchemaRegistryAdditionalInfo(httpClient, registryUrl);

          return SchemaRegistryInfo.builder()
              .available(true)
              .url(registryUrl)
              .version(version)
              .registeredSchemas(subjects.size())
              .metadata(
                  new TreeMap<>(
                      Map.of(
                          "subjects",
                          subjects.size() > 10 ? subjects.subList(0, 10) : subjects,
                          "totalSubjects",
                          subjects.size(),
                          "additionalInfo",
                          additionalInfo)))
              .build();
        } else {
          return SchemaRegistryInfo.builder()
              .available(false)
              .url(registryUrl)
              .errorMessage("Schema Registry returned status: " + subjectsResponse.getCode())
              .build();
        }
      }
    } catch (Exception e) {
      log.debug("Failed to connect to Schema Registry: {}", e.getMessage());
      return SchemaRegistryInfo.builder()
          .available(false)
          .url(registryUrl)
          .errorMessage("Connection failed: " + e.getMessage())
          .build();
    }
  }

  private String getSchemaRegistryUrl() {
    // First priority: SerDeKeyValueConfig properties map using public method
    if (schemaRegistryConfig != null) {
      Map<String, String> props = schemaRegistryConfig.getProperties(null);
      if (props != null) {
        String url = props.get("schema.registry.url");
        if (url != null && !url.isEmpty()) {
          return url;
        }
      }
    }

    // Second priority: Producer config
    if (kafkaProducerConfig != null && kafkaProducerConfig.containsKey("schema.registry.url")) {
      return String.valueOf(kafkaProducerConfig.get("schema.registry.url"));
    }

    // Third priority: Consumer config
    if (kafkaConsumerConfig != null && kafkaConsumerConfig.containsKey("schema.registry.url")) {
      return String.valueOf(kafkaConsumerConfig.get("schema.registry.url"));
    }

    return null;
  }

  private String getSchemaRegistryVersion(CloseableHttpClient httpClient, String registryUrl) {
    try {
      HttpGet infoRequest = new HttpGet(registryUrl + "/");
      addSchemaRegistryAuth(infoRequest);

      try (CloseableHttpResponse infoResponse = httpClient.execute(infoRequest)) {
        if (infoResponse.getCode() == 200) {
          String infoJson = EntityUtils.toString(infoResponse.getEntity());
          ObjectMapper mapper = new ObjectMapper();
          Map<String, Object> info =
              mapper.readValue(infoJson, new TypeReference<Map<String, Object>>() {});
          if (info.containsKey("version")) {
            return info.get("version").toString();
          }
        }
      }
    } catch (Exception e) {
      log.debug("Failed to get schema registry version: {}", e.getMessage());
    }
    return "unknown";
  }

  private Map<String, Object> getSchemaRegistryAdditionalInfo(
      CloseableHttpClient httpClient, String registryUrl) {
    Map<String, Object> additionalInfo = new TreeMap<>();

    try {
      HttpGet configRequest = new HttpGet(registryUrl + "/config");
      addSchemaRegistryAuth(configRequest);

      try (CloseableHttpResponse configResponse = httpClient.execute(configRequest)) {
        if (configResponse.getCode() == 200) {
          String configJson = EntityUtils.toString(configResponse.getEntity());
          ObjectMapper mapper = new ObjectMapper();
          Map<String, Object> config =
              mapper.readValue(configJson, new TypeReference<Map<String, Object>>() {});
          additionalInfo.put("compatibility", config.getOrDefault("compatibilityLevel", "unknown"));
        }
      }
    } catch (Exception e) {
      log.debug("Failed to get schema registry config: {}", e.getMessage());
    }

    return additionalInfo;
  }

  private void addSchemaRegistryAuth(HttpGet request) {
    String userInfo = null;

    // First priority: SerDeKeyValueConfig properties map using public method
    if (schemaRegistryConfig != null) {
      Map<String, String> props = schemaRegistryConfig.getProperties(null);
      if (props != null) {
        userInfo = props.get("basic.auth.user.info");
        if (userInfo == null) {
          userInfo = props.get("schema.registry.basic.auth.user.info");
        }
      }
    }

    // Second priority: Check configs for auth info
    if (userInfo == null) {
      // Check producer config
      if (kafkaProducerConfig != null) {
        if (kafkaProducerConfig.containsKey("basic.auth.user.info")) {
          userInfo = String.valueOf(kafkaProducerConfig.get("basic.auth.user.info"));
        } else if (kafkaProducerConfig.containsKey("schema.registry.basic.auth.user.info")) {
          userInfo =
              String.valueOf(kafkaProducerConfig.get("schema.registry.basic.auth.user.info"));
        }
      }

      // Check consumer config if still not found
      if (userInfo == null && kafkaConsumerConfig != null) {
        if (kafkaConsumerConfig.containsKey("basic.auth.user.info")) {
          userInfo = String.valueOf(kafkaConsumerConfig.get("basic.auth.user.info"));
        } else if (kafkaConsumerConfig.containsKey("schema.registry.basic.auth.user.info")) {
          userInfo =
              String.valueOf(kafkaConsumerConfig.get("schema.registry.basic.auth.user.info"));
        }
      }
    }

    // Apply authentication if found
    if (userInfo != null && !userInfo.isEmpty()) {
      String encoded =
          Base64.getEncoder().encodeToString(userInfo.getBytes(StandardCharsets.UTF_8));
      request.setHeader("Authorization", "Basic " + encoded);
    }
  }
}
