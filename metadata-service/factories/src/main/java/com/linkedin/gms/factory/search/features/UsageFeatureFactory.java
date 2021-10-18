package com.linkedin.gms.factory.search.features;

import com.linkedin.gms.factory.timeseries.TimeseriesAspectServiceFactory;
import com.linkedin.metadata.search.features.UsageFeature;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import(TimeseriesAspectServiceFactory.class)
public class UsageFeatureFactory {
  @Autowired
  private TimeseriesAspectService timeseriesAspectService;

  @Value("${USAGE_FEATURE_BATCH_SIZE:500}")
  private Integer batchSize;

  @Bean(name = "usageFeature")
  @Nonnull
  protected UsageFeature getInstance() {
    return new UsageFeature(timeseriesAspectService, batchSize);
  }
}
