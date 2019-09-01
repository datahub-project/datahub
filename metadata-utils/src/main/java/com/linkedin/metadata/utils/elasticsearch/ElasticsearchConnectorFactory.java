package com.linkedin.metadata.utils.elasticsearch;

import java.util.Arrays;
import java.util.Properties;

import com.linkedin.util.Configuration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchConnectorFactory {

  private static final Properties CFG = Configuration.loadProperties("elasticsearch.properties");

  public static ElasticsearchConnector createInstance() {

    return new ElasticsearchConnector(
            Arrays.asList(CFG.getProperty("hosts")),
            Integer.valueOf(CFG.getProperty("port")),
            Integer.valueOf(CFG.getProperty("threadCount", "1")),
            Integer.valueOf(CFG.getProperty("bulkRequestsLimit", "10000")),
            Integer.valueOf(CFG.getProperty("bulkFlushPeriod", "1")));
  }
}