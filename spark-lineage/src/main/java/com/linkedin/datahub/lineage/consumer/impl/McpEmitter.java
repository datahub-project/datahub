package com.linkedin.datahub.lineage.consumer.impl;

import datahub.client.Emitter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.ExecutionException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;

import com.linkedin.datahub.lineage.spark.model.LineageConsumer;
import com.linkedin.datahub.lineage.spark.model.LineageEvent;
import com.linkedin.mxe.MetadataChangeProposal;

import datahub.client.RestEmitter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class McpEmitter implements LineageConsumer {

  private static final String GMS_URL_KEY = "spark.datahub.rest.server";
  private static final String SENTINEL = "moot";

  private ConcurrentHashMap<String, RestEmitter> singleton = new ConcurrentHashMap<>();

  private void emit(List<MetadataChangeProposal> mcps) {
    Emitter emitter = emitter();
    if (emitter != null) {
      mcps.forEach(mcp -> {
        log.debug("Emitting {}", mcp);
        try {
          emitter.emit(mcp, null).get();
        } catch (IOException | InterruptedException | ExecutionException e) {
          // log error, but don't impact thread
          StringWriter s = new StringWriter();
          PrintWriter p = new PrintWriter(s);
          e.printStackTrace(p);
          log.error(s.toString());
          p.close();
        }
      });
    }
  }

  // TODO ideally the impl here should not be tied to Spark; the LineageConsumer
  // API needs tweaking to include configs
  private Emitter emitter() {
    singleton.computeIfAbsent(SENTINEL, x -> {
      SparkConf conf = SparkEnv.get().conf();
      if (conf.contains(GMS_URL_KEY)) {
        String gmsUrl = conf.get(GMS_URL_KEY);
        log.debug("REST emitter configured with GMS url " + gmsUrl);
        return RestEmitter.create($ -> $
        .gmsUrl(gmsUrl));
      }

      log.error("GMS URL not configured.");
      return null;
    });

    return singleton.get(SENTINEL);
  }

  @Override
  public void accept(LineageEvent evt) {
    emit(evt.toMcps());
  }
}
