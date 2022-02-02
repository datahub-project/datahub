package datahub.spark.consumer.impl;

import datahub.spark.model.LineageConsumer;
import datahub.spark.model.LineageEvent;
import datahub.client.Emitter;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import com.typesafe.config.Config;


@Slf4j
public class McpEmitter implements LineageConsumer {

  private final Optional<Emitter> emitter;
  //private static final String CONF_PREFIX = "spark.datahub.";
  private static final String TRANSPORT_KEY = "transport";
  private static final String GMS_URL_KEY = "rest.server";
  private static final String GMS_AUTH_TOKEN = "rest.token";

  private void emit(List<MetadataChangeProposalWrapper> mcpws) {
    if (emitter.isPresent()) {
      mcpws.stream().map(mcpw -> {
        try {
          return emitter.get().emit(mcpw);
        } catch (IOException ioException) {
          log.error("Failed to emit metadata to DataHub", ioException);
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList()).forEach(future -> {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          // log error, but don't impact thread
          log.error("Failed to emit metadata to DataHub", e);
        }
      });
    }
  }

  public McpEmitter(Config datahub_conf) {
    String emitterType = datahub_conf.hasPath(TRANSPORT_KEY) ? datahub_conf.getString(TRANSPORT_KEY) : "rest";
    switch(emitterType) {
      case "rest":
    	String gmsUrl = datahub_conf.hasPath(GMS_URL_KEY) ? datahub_conf.getString(GMS_URL_KEY) : "http://localhost:8080";
        String token = datahub_conf.hasPath(GMS_AUTH_TOKEN) ? datahub_conf.getString(GMS_AUTH_TOKEN) : null;
        log.info("REST Emitter Configuration: GMS url {}{}", gmsUrl, (datahub_conf.hasPath(GMS_URL_KEY) ? "" : "(default)"));
        if (token != null) {
          log.info("REST Emitter Configuration: Token {}", (token != null) ? "XXXXX" : "(empty)");
        }
        emitter = Optional.of(RestEmitter.create($ -> $.server(gmsUrl).token(token)));
        break;
      default:
        emitter = Optional.empty();
        log.error("DataHub Transport {} not recognized. DataHub Lineage emission will not work", emitterType);
    }
  }

  @Override
  public void accept(LineageEvent evt) {
    emit(evt.asMetadataEvents());
  }

  @Override
  public void close() throws IOException {
    if (emitter.isPresent()) {
      emitter.get().close();
    }
  }
}
