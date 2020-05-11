package com.linkedin.metadata.utils.elasticsearch;

import java.util.Arrays;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

@Slf4j
public class ElasticsearchConnectorFactory {

  private static final int DEFAULT_ES_THREAD_COUNT = 1;
  private static final int DEFAULT_ES_BULK_REQUESTS_LIMIT = 10000;
  private static final int DEFAULT_ES_BULK_FLUSH_PERIOD = 1;

  private ElasticsearchConnectorFactory() {
  }

  public static ElasticsearchConnector createInstance(@Nonnull String host, @Nonnull int port) {
    return new ElasticsearchConnector(Arrays.asList(host), port,
        DEFAULT_ES_THREAD_COUNT,
        DEFAULT_ES_BULK_REQUESTS_LIMIT,
        DEFAULT_ES_BULK_FLUSH_PERIOD);
  }
}