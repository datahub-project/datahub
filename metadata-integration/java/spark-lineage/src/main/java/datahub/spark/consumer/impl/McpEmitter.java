package datahub.spark.consumer.impl;

import datahub.spark.model.LineageConsumer;
import datahub.spark.model.LineageEvent;
import datahub.client.Emitter;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;


@Slf4j
public class McpEmitter implements LineageConsumer, Closeable {

  private final Optional<Emitter> emitter;
  private static final String CONF_PREFIX = "spark.datahub.";
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

  // TODO ideally the impl here should not be tied to Spark; the LineageConsumer
  // API needs tweaking to include configs
  public McpEmitter() {
    SparkConf sparkConf = SparkEnv.get().conf();
    Map<String, String> conf =
        Arrays.stream(sparkConf.getAllWithPrefix("spark.datahub.")).collect(Collectors.toMap(x -> x._1, x -> x._2));

    String emitterType = conf.getOrDefault(TRANSPORT_KEY, "rest");
    if (emitterType.toLowerCase(Locale.ROOT).equals("rest")) {
      String gmsUrl = conf.getOrDefault(GMS_URL_KEY, "http://localhost:8080");
      String token = conf.getOrDefault(GMS_AUTH_TOKEN, null);
      log.info("REST Emitter Configuration: GMS url {}{}", gmsUrl, (conf.containsKey(GMS_URL_KEY) ? "" : "(default)"));
      if (token != null) {
        log.info("REST Emitter Configuration: Token {}", (token != null) ? "XXXXX" : "(empty)");
      }
      emitter = Optional.of(RestEmitter.create($ -> $.server(gmsUrl).token(token)));
    } else {
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
