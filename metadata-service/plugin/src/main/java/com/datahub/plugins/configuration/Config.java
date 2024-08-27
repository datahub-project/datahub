package com.datahub.plugins.configuration;

import com.datahub.plugins.common.ConfigValidationUtils;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;

/** {@link Config} is getting loaded from /etc/datahub/plugins/auth/config.yaml */
@Getter
@Builder
@JsonDeserialize(builder = Config.CustomBuilder.class)
public class Config {
  public static final String FIELD_PLUGINS = "plugins"; // for validation error messages
  @Nonnull private List<PluginConfig> plugins;

  public static CustomBuilder builder() {
    return new CustomBuilder();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class CustomBuilder extends ConfigBuilder {
    public Config build() {
      ConfigValidationUtils.listShouldNotBeEmpty(
          FIELD_PLUGINS, Collections.singletonList(super.plugins));

      List<String> list = new ArrayList<>(super.plugins.size());
      super.plugins.forEach(
          (pluginConfig) -> {
            list.add(pluginConfig.getName());
          });

      ConfigValidationUtils.listShouldNotHaveDuplicate(FIELD_PLUGINS, list);

      return super.build();
    }
  }
}
