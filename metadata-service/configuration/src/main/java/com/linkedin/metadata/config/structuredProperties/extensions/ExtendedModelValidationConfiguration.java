package com.linkedin.metadata.config.structuredProperties.extensions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
@JsonDeserialize(
    builder =
        ExtendedModelValidationConfiguration.ExtendedModelValidationConfigurationBuilder.class)
public class ExtendedModelValidationConfiguration {
  List<EntityConfiguration> entities;
  List<StructuredPropertyConfiguration> structuredProperties;

  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ExtendedModelValidationConfigurationBuilder {}
}
