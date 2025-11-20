package com.linkedin.metadata.config.search.custom;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
@JsonDeserialize(builder = CustomSearchConfiguration.CustomSearchConfigurationBuilder.class)
public class CustomSearchConfiguration {

  @Builder.Default
  private Map<String, FieldConfiguration> fieldConfigurations = Collections.emptyMap();

  @Builder.Default private List<QueryConfiguration> queryConfigurations = Collections.emptyList();

  @Builder.Default
  private List<AutocompleteConfiguration> autocompleteConfigurations = Collections.emptyList();

  @JsonPOJOBuilder(withPrefix = "")
  public static class CustomSearchConfigurationBuilder {}
}
