package com.linkedin.gms.factory.timeseries;

import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({ElasticSearchTimeseriesAspectServiceFactory.class})
public class TimeseriesAspectServiceFactory {
  @Autowired
  @Qualifier("elasticSearchTimeseriesAspectService")
  private ElasticSearchTimeseriesAspectService _elasticSearchTimeseriesAspectService;

  @Bean(name = "timeseriesAspectService")
  @Primary
  @Nonnull
  protected TimeseriesAspectService getInstance() {
    return _elasticSearchTimeseriesAspectService;
  }
}
