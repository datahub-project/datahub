package com.linkedin.gms.factory.usage;

import com.linkedin.metadata.usage.UsageService;
import com.linkedin.metadata.usage.elasticsearch.ElasticUsageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.Nonnull;


@Configuration
@Import({ElasticUsageServiceFactory.class})
public class UsageServiceFactory {
  @Autowired
  @Qualifier("elasticUsageService")
  private ElasticUsageService elasticUsageService;

  @Bean(name = "usageService")
  @Nonnull
  protected UsageService getInstance() {
    return elasticUsageService;
  }
}
