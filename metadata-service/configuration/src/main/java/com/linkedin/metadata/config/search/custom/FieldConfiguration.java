package com.linkedin.metadata.config.search.custom;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Builder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
@JsonDeserialize(builder = FieldConfiguration.FieldConfigurationBuilder.class)
public class FieldConfiguration {

  private SearchFields searchFields;

  private HighlightFields highlightFields;

  @JsonPOJOBuilder(withPrefix = "")
  public static class FieldConfigurationBuilder {}
}
