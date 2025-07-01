package datahub.spark.consumer.impl;

import com.typesafe.config.Config;
import datahub.client.Emitter;
import datahub.client.kafka.KafkaEmitter;
import datahub.client.kafka.KafkaEmitterConfig;
import datahub.client.rest.RestEmitter;
import datahub.client.rest.RestEmitterConfig;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.model.LineageConsumer;
import datahub.spark.model.LineageEvent;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class McpEmitter implements LineageConsumer {

  private final String emitterType;
  private Optional<RestEmitterConfig> restEmitterConfig;
  private Optional<KafkaEmitterConfig> kafkaEmitterConfig;

  private static final String TRANSPORT_KEY = "transport";
  private static final String GMS_URL_KEY = "rest.server";
  private static final String GMS_AUTH_TOKEN = "rest.token";
  private static final String DISABLE_SSL_VERIFICATION_KEY = "rest.disable_ssl_verification";
  private static final String KAFKA_BOOTSTRAP = "kafka.connection.bootstrap";
  private static final String KAFKA_PRODUCER_CONFIG = "kafka.connection.producer_config";
  private static final String KAFKA_SCHEMA_REGISTRY_URL = "kafka.connection.schema_registry_url";
  private static final String KAFKA_SCHEMA_REGISTRY_CONFIG =
      "kafka.connection.schema_registry_config";

  private Optional<Emitter> getEmitter() {
    Optional<Emitter> emitter = Optional.empty();
    switch (emitterType) {
      case "rest":
        if (restEmitterConfig.isPresent()) {
          emitter = Optional.of(new RestEmitter(restEmitterConfig.get()));
        }
        break;
      case "kafka":
        if (kafkaEmitterConfig.isPresent()) {
          try {
            emitter = Optional.of(new KafkaEmitter(kafkaEmitterConfig.get()));
          } catch (IOException e) {
            log.error("Failed to create KafkaEmitter", e);
          }
        }
        break;

      default:
        log.error(
            "DataHub Transport {} not recognized. DataHub Lineage emission will not work",
            emitterType);
        break;
    }
    return emitter;
  }

  protected void emit(List<MetadataChangeProposalWrapper> mcpws) {
    Optional<Emitter> emitter = getEmitter();
    if (emitter.isPresent()) {
      mcpws.stream()
          .map(
              mcpw -> {
                try {
                  log.debug("emitting mcpw: " + mcpw);
                  return emitter.get().emit(mcpw);
                } catch (IOException ioException) {
                  log.error("Failed to emit metadata to DataHub", ioException);
                  return null;
                }
              })
          .filter(Objects::nonNull)
          .collect(Collectors.toList())
          .forEach(
              future -> {
                try {
                  log.info(future.get().toString());
                } catch (InterruptedException | ExecutionException e) {
                  // log error, but don't impact thread
                  log.error("Failed to emit metadata to DataHub", e);
                }
              });
      try {
        emitter.get().close();
      } catch (IOException e) {
        log.error("Issue while closing emitter" + e);
      }
    }
  }

  public McpEmitter(Config datahubConf) {
    emitterType =
        datahubConf.hasPath(TRANSPORT_KEY) ? datahubConf.getString(TRANSPORT_KEY) : "rest";
    switch (emitterType) {
      case "rest":
        String gmsUrl =
            datahubConf.hasPath(GMS_URL_KEY)
                ? datahubConf.getString(GMS_URL_KEY)
                : "http://localhost:8080";
        String token =
            datahubConf.hasPath(GMS_AUTH_TOKEN) ? datahubConf.getString(GMS_AUTH_TOKEN) : null;
        boolean disableSslVerification =
            datahubConf.hasPath(DISABLE_SSL_VERIFICATION_KEY)
                && datahubConf.getBoolean(DISABLE_SSL_VERIFICATION_KEY);
        log.info(
            "REST Emitter Configuration: GMS url {}{}",
            gmsUrl,
            (datahubConf.hasPath(GMS_URL_KEY) ? "" : "(default)"));
        if (token != null) {
          log.info("REST Emitter Configuration: Token {}", "XXXXX");
        }
        if (disableSslVerification) {
          log.warn("REST Emitter Configuration: ssl verification will be disabled.");
        }
        restEmitterConfig =
            Optional.of(
                RestEmitterConfig.builder()
                    .server(gmsUrl)
                    .token(token)
                    .disableSslVerification(disableSslVerification)
                    .build());

        break;
      case "kafka":
        String kafkaBootstrap =
            datahubConf.hasPath(KAFKA_BOOTSTRAP)
                ? datahubConf.getString(KAFKA_BOOTSTRAP)
                : "localhost:9092";
        Map<String, String> producerConfig =
            datahubConf.hasPath(KAFKA_PRODUCER_CONFIG)
                ? datahubConf.getConfig(KAFKA_PRODUCER_CONFIG).entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey, entry -> entry.getValue().unwrapped().toString()))
                : null;
        String schemaRegistryUrl =
            datahubConf.hasPath(KAFKA_SCHEMA_REGISTRY_URL)
                ? datahubConf.getString(KAFKA_SCHEMA_REGISTRY_URL)
                : null;
        Map<String, String> schemaRegistryConfig =
            datahubConf.hasPath(KAFKA_SCHEMA_REGISTRY_CONFIG)
                ? datahubConf.getConfig(KAFKA_SCHEMA_REGISTRY_CONFIG).entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey, entry -> entry.getValue().unwrapped().toString()))
                : null;
        log.info(
            "Kafka Emitter Configuration: Kafka bootstrap {}{}",
            kafkaBootstrap,
            (datahubConf.hasPath(KAFKA_BOOTSTRAP) ? "" : "(default)"));
        kafkaEmitterConfig =
            Optional.of(
                KafkaEmitterConfig.builder()
                    .bootstrap(kafkaBootstrap)
                    .producerConfig(producerConfig)
                    .schemaRegistryUrl(schemaRegistryUrl)
                    .schemaRegistryConfig(schemaRegistryConfig)
                    .build());
      default:
        log.error(
            "DataHub Transport {} not recognized. DataHub Lineage emission will not work",
            emitterType);
        break;
    }
  }

  @Override
  public void accept(LineageEvent evt) {
    emit(evt.asMetadataEvents());
  }

  @Override
  public void close() throws IOException {
    // Nothing to close at this point

  }
}
