package com.linkedin.metadata.kafka.config;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.search.BaseIndexBuilder;
import com.linkedin.metadata.builders.search.ChartIndexBuilder;
import com.linkedin.metadata.builders.search.CorpGroupIndexBuilder;
import com.linkedin.metadata.builders.search.DashboardIndexBuilder;
import com.linkedin.metadata.builders.search.DataProcessIndexBuilder;
import com.linkedin.metadata.builders.search.DatasetIndexBuilder;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.restli.client.Client;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Slf4j
@Configuration
@RequiredArgsConstructor
public class IndexBuildersConfig {

  @Value("${GMS_HOST:localhost}")
  private String gmsHost;
  @Value("${GMS_PORT:8080}")
  private int gmsPort;

  @Bean
  public Set<BaseIndexBuilder<? extends RecordTemplate>> getIndexBuilders(@Nonnull Client restliClient) {
    log.debug("restli client {}", restliClient);
    final Set<BaseIndexBuilder<? extends RecordTemplate>> builders = new HashSet<>();
    builders.add(new CorpGroupIndexBuilder());
    builders.add(new ChartIndexBuilder());
    builders.add(new DatasetIndexBuilder());
    builders.add(new DataProcessIndexBuilder());
    builders.add(new DashboardIndexBuilder());
    return builders;
  }

  @Bean
  public Client remoteReaderDAO() {
    return DefaultRestliClientFactory.getRestLiClient(gmsHost, gmsPort);
  }
}
