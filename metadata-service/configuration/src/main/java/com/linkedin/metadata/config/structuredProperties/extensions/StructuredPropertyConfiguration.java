package com.linkedin.metadata.config.structuredProperties.extensions;

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
    builder = StructuredPropertyConfiguration.StructuredPropertyConfigurationBuilder.class)
public class StructuredPropertyConfiguration {

  private String qualifiedName;
  private String type;
  private Cardinality cardinality;
  private String displayName;
  private String description;
  private List<PropertyValue> allowedValues;

  public enum Cardinality {
    SINGLE,
    MULTIPLE
  }

  @Slf4j
  @Builder(toBuilder = true)
  @Getter
  @ToString
  @EqualsAndHashCode
  @JsonDeserialize(
      builder = StructuredPropertyConfiguration.PropertyValue.PropertyValueBuilder.class)
  public static class PropertyValue {
    private String value;
    private String description;

    @JsonPOJOBuilder(withPrefix = "")
    public static class PropertyValueBuilder {}
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class StructuredPropertyConfigurationBuilder {}
}
