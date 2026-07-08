package com.linkedin.metadata.config.usage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import javax.annotation.Nonnull;

/** Shared snake_case YAML mapper for usage and billing manifest loaders. */
public final class UsageYamlMapper {

  private UsageYamlMapper() {}

  @Nonnull
  public static ObjectMapper create() {
    YAMLMapper mapper = new YAMLMapper();
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return mapper;
  }
}
