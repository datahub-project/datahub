package com.linkedin.gms.factory.temporal;

import com.linkedin.metadata.temporal.TemporalAspectService;
import com.linkedin.metadata.temporal.elastic.ElasticSearchTemporalAspectService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;


@Configuration
@Import({ElasticSearchTemporalAspectServiceFactory.class})
public class TemporalAspectServiceFactory {
  @Autowired
  @Qualifier("elasticSearchTemporalAspectService")
  private ElasticSearchTemporalAspectService _elasticSearchTemporalAspectService;

  @Bean(name = "temporalAspectService")
  @Primary
  @Nonnull
  protected TemporalAspectService getInstance() {
    return _elasticSearchTemporalAspectService;
  }
}
