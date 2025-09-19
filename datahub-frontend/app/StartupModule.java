package modules;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import play.Environment;

/** Module to dump configuration on application startup */
public class StartupModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ConfigDumper.class).asEagerSingleton();
  }

  @Slf4j
  @Singleton
  public static class ConfigDumper {

    @Inject
    public ConfigDumper(Config config, Environment environment) {
      // Dump configuration on startup (constructor is called during app initialization)
      dumpConfiguration(config, environment);
    }

    private void dumpConfiguration(Config config, Environment environment) {
      log.info("=== DataHub Frontend Configuration ===");
      log.info("Environment: {}", environment.mode());
      log.info("Env root path: {}", environment.rootPath());
      log.info("Base Path: {}", config.getString("datahub.basePath"));
      log.info("GMS Host: {}", config.getString("metadataService.host"));
      log.info("GMS Port: {}", config.getInt("metadataService.port"));
      log.info("GMS Base Path: {}", config.getString("metadataService.basePath"));

      // Dump all datahub.* configuration if available
      if (config.hasPath("datahub")) {
        Config datahubConfig = config.getConfig("datahub");
        String datahubConfigStr =
            datahubConfig.root().render(ConfigRenderOptions.defaults().setOriginComments(false));
        log.info("DataHub Configuration:\n{}", datahubConfigStr);
      }

      // Optionally dump full configuration (be careful - may contain secrets)
      if (config.hasPath("debug.dumpFullConfig") && config.getBoolean("debug.dumpFullConfig")) {
        log.warn("=== FULL CONFIGURATION DUMP (may contain secrets) ===");
        String fullConfig =
            config.root().render(ConfigRenderOptions.defaults().setOriginComments(false));
        log.info("Full Configuration:\n{}", fullConfig);
      }

      log.info("=== End Configuration Dump ===");
    }
  }
}
