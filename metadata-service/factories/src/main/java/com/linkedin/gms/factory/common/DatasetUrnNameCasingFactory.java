package com.linkedin.gms.factory.common;

import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DatasetUrnNameCasingFactory {
  @Nonnull
  @Bean(name = "datasetUrnNameCasing")
  protected Boolean getInstance() {
    String datasetUrnNameCasingEnv = System.getenv("DATAHUB_DATASET_URN_TO_LOWER");
    return Boolean.parseBoolean(datasetUrnNameCasingEnv);
  }
}