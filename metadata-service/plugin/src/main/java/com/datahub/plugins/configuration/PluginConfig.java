package com.datahub.plugins.configuration;

import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.common.Validator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.nio.file.Path;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonDeserialize(builder = PluginConfig.CustomBuilder.class)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class PluginConfig {
  @EqualsAndHashCode.Include
  private String name; // In list of plugin, the name should be unique

  private PluginType type;

  private Boolean enabled;

  private Map<String, Object> params;

  @JsonIgnore
  private Path pluginDirectory; // It is derived attribute

  public static CustomBuilder builder() {
    return new CustomBuilder();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class CustomBuilder extends PluginConfigBuilder {
    public PluginConfig build() {
      Validator.whiteSpacesValidation("name", super.name);
      Validator.mapShouldNotBeEmpty("params", super.params);

      return super.build();
    }
  }
}
