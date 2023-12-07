package com.linkedin.metadata.config.search.custom;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@JsonDeserialize(builder = CustomSearchConfiguration.CustomSearchConfigurationBuilder.class)
public class CustomSearchConfiguration {

  private List<QueryConfiguration> queryConfigurations;

  @JsonPOJOBuilder(withPrefix = "")
  public static class CustomSearchConfigurationBuilder {}
}
