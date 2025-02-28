package com.linkedin.metadata.config.structuredProperties.extensions;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Map;
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
@JsonDeserialize(builder = FieldConfiguration.FieldConfigurationBuilder.class)
public class FieldConfiguration {

  private String fieldName;
  private OperationType operation;
  private Map<String, String> config;

  public enum OperationType {
    ADD_STRUCTURED_PROPERTY;
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class FieldConfigurationBuilder {}
}
