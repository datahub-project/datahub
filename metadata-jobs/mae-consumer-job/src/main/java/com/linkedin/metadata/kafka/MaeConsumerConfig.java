package com.linkedin.metadata.kafka;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.search.EntitySearchService;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MaeConsumerConfig {

  @Autowired
  @Qualifier("cachingAspectRetriever")
  private AspectRetriever aspectRetriever;

  @Autowired private EntitySearchService entitySearchService;

  @PostConstruct
  protected void postConstruct() {
    entitySearchService.postConstruct(aspectRetriever);
  }
}
