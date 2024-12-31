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
@JsonDeserialize(builder = BoolQueryConfiguration.BoolQueryConfigurationBuilder.class)
public class BoolQueryConfiguration {
  private Object must;
  private Object should;
  // CHECKSTYLE:OFF
  private Object must_not;
  // CHECKSTYLE:ON
  private Object filter;

  @JsonPOJOBuilder(withPrefix = "")
  public static class BoolQueryConfigurationBuilder {}
}
