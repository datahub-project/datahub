package com.linkedin.metadata.examples.configs;

import java.io.IOException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class ZooKeeperConfig {
  @Value("${ZOOKEEPER:localhost:2181}")
  private String zookeeper;

  @Value("${ZOOKEEPER_TIMEOUT_MILLIS:3000}")
  private int timeoutMillis;

  @Bean(name = "zooKeeper")
  public ZooKeeper zooKeeperFactory() throws IOException {
    Watcher noopWatcher = event -> {
    };

    return new ZooKeeper(zookeeper, timeoutMillis, noopWatcher);
  }
}
