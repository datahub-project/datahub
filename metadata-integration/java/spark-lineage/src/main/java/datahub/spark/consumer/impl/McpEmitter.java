package datahub.spark.consumer.impl;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.typesafe.config.Config;

import datahub.client.Emitter;
import datahub.client.rest.RestEmitter;
import datahub.client.rest.RestEmitterConfig;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.model.LineageConsumer;
import datahub.spark.model.LineageEvent;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class McpEmitter implements LineageConsumer {

  private String emitterType;
  private Optional<RestEmitterConfig> restEmitterConfig;
  private static final String TRANSPORT_KEY = "transport";
  private static final String GMS_URL_KEY = "rest.server";
  private static final String GMS_AUTH_TOKEN = "rest.token";
  
  private Optional<Emitter> getEmitter() {
    Optional<Emitter> emitter = Optional.empty();
    switch (emitterType) {
    case "rest":
      if (restEmitterConfig.isPresent()) {
        emitter = Optional.of(new RestEmitter(restEmitterConfig.get()));
      }
      break;
      
    default:
      log.error("DataHub Transport {} not recognized. DataHub Lineage emission will not work", emitterType);
      break;
      
    }
    return emitter;
  }

  private void emit(List<MetadataChangeProposalWrapper> mcpws) {
    Optional<Emitter> emitter = getEmitter();
    if (emitter.isPresent()) {
      mcpws.stream().map(mcpw -> {
        try {
          log.debug("emitting mcpw: " + mcpw);
          return emitter.get().emit(mcpw);
        } catch (IOException ioException) {
          log.error("Failed to emit metadata to DataHub", ioException);
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList()).forEach(future -> {
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
      emitterType = datahubConf.hasPath(TRANSPORT_KEY) ? datahubConf.getString(TRANSPORT_KEY) : "rest";
      switch (emitterType) {
      case "rest":
          String gmsUrl = datahubConf.hasPath(GMS_URL_KEY) ? datahubConf.getString(GMS_URL_KEY)
                  : "http://localhost:8080";
          String token = datahubConf.hasPath(GMS_AUTH_TOKEN) ? datahubConf.getString(GMS_AUTH_TOKEN) : null;
          log.info("REST Emitter Configuration: GMS url {}{}", gmsUrl,
                  (datahubConf.hasPath(GMS_URL_KEY) ? "" : "(default)"));
          if (token != null) {
              log.info("REST Emitter Configuration: Token {}", (token != null) ? "XXXXX" : "(empty)");
          }
          restEmitterConfig = Optional.of(RestEmitterConfig.builder().server(gmsUrl).token(token).build());
          
          break;
      default:
          log.error("DataHub Transport {} not recognized. DataHub Lineage emission will not work", emitterType);
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