package com.datahub.plugins.metadata.aspect;

import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.config.PluginConfiguration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@Slf4j
public class SpringPluginFactory extends PluginFactory {

  @Nullable private final ApplicationContext springApplicationContext;

  public SpringPluginFactory(
      @Nullable ApplicationContext springApplicationContext,
      @Nullable PluginConfiguration pluginConfiguration,
      @Nonnull List<ClassLoader> classLoaders) {
    super(pluginConfiguration, classLoaders);

    try {
      String[] packageScan =
          extractPackageScan(
                  Optional.ofNullable(pluginConfiguration)
                      .map(PluginConfiguration::streamAll)
                      .orElse(Stream.of()))
              .toArray(String[]::new);

      if (springApplicationContext != null || packageScan.length == 0) {
        this.springApplicationContext = springApplicationContext;
      } else {
        AnnotationConfigApplicationContext rootContext = null;

        for (ClassLoader classLoader : classLoaders) {
          AnnotationConfigApplicationContext applicationContext =
              new AnnotationConfigApplicationContext();
          applicationContext.setId("custom-plugin");
          if (rootContext != null) {
            applicationContext.setParent(rootContext);
          }
          applicationContext.setClassLoader(classLoader);
          applicationContext.scan(packageScan);
          rootContext = applicationContext;
        }
        rootContext.refresh();
        this.springApplicationContext = rootContext;
      }

      loadPlugins();
    } catch (Exception e) {
      log.error("Error loading Spring Plugins!", e);
      throw e;
    }
  }

  private static Stream<String> extractPackageScan(Stream<AspectPluginConfig> configStream) {
    return filterSpringConfigs(configStream)
        .map(AspectPluginConfig::getPackageScan)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .distinct();
  }

  private static Stream<AspectPluginConfig> filterSpringConfigs(
      Stream<AspectPluginConfig> configStream) {
    return configStream.filter(
        config -> config.getSpring() != null && config.getSpring().isEnabled());
  }

  @Nonnull
  @Override
  public List<ClassLoader> getClassLoaders() {
    if (!super.getClassLoaders().isEmpty()) {
      return super.getClassLoaders();
    }
    return List.of(SpringPluginFactory.class.getClassLoader());
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
  @Override
  protected <T extends PluginSpec> List<T> build(
      Class<?> baseClazz, List<String> packageNames, List<AspectPluginConfig> configs) {

    // load non-spring
    List<T> result = new ArrayList<>(super.build(baseClazz, packageNames, configs));

    if (springApplicationContext == null) {
      return result;
    }

    // consider Spring dependency injection
    for (AspectPluginConfig config :
        filterSpringConfigs(configs.stream()).collect(Collectors.toSet())) {
      boolean loaded = false;

      for (ClassLoader classLoader : getClassLoaders()) {
        try {
          Class<?> clazz = classLoader.loadClass(config.getClassName());

          final List<T> plugins;
          if (config.getSpring().getName() == null) {
            plugins =
                BeanFactoryUtils.beansOfTypeIncludingAncestors(springApplicationContext, clazz)
                    .values()
                    .stream()
                    .map(plugin -> (T) plugin)
                    .collect(Collectors.toList());
          } else {
            plugins =
                List.of((T) springApplicationContext.getBean(config.getSpring().getName(), clazz));
          }

          plugins.stream()
              .filter(plugin -> plugin.enabled())
              .forEach(
                  plugin -> {
                    if (plugin.getConfig() != null) {
                      result.add(plugin);
                    } else {
                      result.add((T) plugin.setConfig(config));
                    }
                  });

          loaded = true;
          break;
        } catch (ClassNotFoundException e) {
          log.warn(
              "Failed to load class {} from loader {}",
              config.getClassName(),
              classLoader.getName(),
              e);
        }
      }

      if (!loaded) {
        log.error("Failed to load Spring plugin {}!", config.getClassName());
      }
    }

    return result;
  }
}
