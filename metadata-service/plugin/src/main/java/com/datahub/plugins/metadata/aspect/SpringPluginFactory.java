package com.datahub.plugins.metadata.aspect;

import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

@Slf4j
public class SpringPluginFactory extends PluginFactory {

  @Nonnull private final ApplicationContext springApplicationContext;

  public SpringPluginFactory(
      @Nonnull ApplicationContext springApplicationContext,
      @Nullable PluginConfiguration pluginConfiguration,
      @Nonnull List<ClassLoader> classLoaders) {
    super(pluginConfiguration, classLoaders);
    this.springApplicationContext = springApplicationContext;
  }

  /**
   * Override to inject classes from Spring
   *
   * @param baseClazz
   * @param configs
   * @param packageNames
   * @return
   * @param <T>
   */
  protected <T extends PluginSpec> List<T> build(
      Class<?> baseClazz, List<AspectPluginConfig> configs, String... packageNames) {

    // load non-spring
    List<T> result = new ArrayList<>(super.build(baseClazz, configs, packageNames));

    // consider Spring dependency injection
    Set<AspectPluginConfig> springConfigs =
        configs.stream()
            .filter(config -> config.getSpring() != null && config.getSpring().isEnabled())
            .collect(Collectors.toSet());

    for (AspectPluginConfig config : springConfigs) {
      boolean loaded = false;

      for (ClassLoader classLoader : getClassLoaders()) {
        try {
          Class<?> clazz = classLoader.loadClass(config.getClassName());

          final T plugin;
          if (config.getSpring().getName() == null) {
            plugin = (T) springApplicationContext.getBean(clazz);
          } else {
            plugin = (T) springApplicationContext.getBean(config.getSpring().getName(), clazz);
          }

          if (plugin.enabled()) {
            result.add((T) plugin.setConfig(config));
          }

          loaded = true;
          break;
        } catch (ClassNotFoundException e) {
          log.warn(
              "Failed to load class {} from loader {}",
              config.getClassName(),
              classLoader.getName());
        }
      }

      if (!loaded) {
        log.error("Failed to load Spring plugin {}!", config.getClassName());
      }
    }

    return result;
  }
}
