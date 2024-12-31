package com.datahub.plugins.configuration;

import com.datahub.plugins.common.ConfigValidationUtils;
import com.datahub.plugins.common.PluginType;
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

/** POJO to map YAML section present in config.yml at plugins[] */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonDeserialize(builder = PluginConfig.CustomBuilder.class)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class PluginConfig {
  /** name of the plugin. It should be unique in plugins[] list */
  @EqualsAndHashCode.Include private String name; // In list of plugin, the name should be unique

  /** Type of plugin. Supported types are {@link PluginType} */
  private PluginType type;

  /**
   * Whether to load the plugin in GMS. If set to true plugin will be loaded in GMS take
   * authentication/authorization decisions.
   */
  private Boolean enabled;

  /**
   * Attributes in params should be as per POJO {@link
   * com.datahub.plugins.auth.configuration.AuthParam}
   */
  private Map<String, Object> params;

  /**
   * It is always set to <plugin-base-directory>/<plugin-name>. For example if plugin-name is
   * ranger-authorizer and plugin-base-directory is /etc/datahub/plugins/auth then pluginDirectory
   * would be /etc/datahub/plugins/auth/ranger-authorizer
   */
  @JsonIgnore private Path pluginHomeDirectory;

  public static CustomBuilder builder() {
    return new CustomBuilder();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class CustomBuilder extends PluginConfigBuilder {
    public PluginConfig build() {
      ConfigValidationUtils.whiteSpacesValidation("name", super.name);
      ConfigValidationUtils.mapShouldNotBeEmpty("params", super.params);

      return super.build();
    }
  }
}
