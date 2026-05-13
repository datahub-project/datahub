package controllers;

import auth.Authenticator;
import com.typesafe.config.Config;
import java.nio.file.Files;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import play.Environment;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Results;
import play.mvc.Security;

/**
 * Controller for serving MFE (Micro Frontend) configuration. The configuration is read once at
 * startup and cached in-memory to avoid repeated file I/O. HTTP cache headers are set to allow
 * browser caching for 5 minutes.
 */
@Slf4j
@Singleton
public class MfeConfigController extends Controller {

  private static final int CACHE_MAX_AGE_SECONDS = 300; // 5 minutes

  private final String configFilePath;
  private final String cachedConfigContent;

  @Inject
  public MfeConfigController(@Nonnull Config configs, Environment environment) {
    String path = configs.getString("mfeConfigFilePath");
    if (path == null) {
      throw new IllegalArgumentException("MfeConfigFilePath is null or not set!");
    }
    this.configFilePath = path;
    // Read and cache the config at startup
    this.cachedConfigContent = readFromYMLConfig(path, environment);
    if (cachedConfigContent.isEmpty()) {
      log.warn("MfeConfigController initialized but config file could not be read from: {}", path);
    } else {
      log.info("MfeConfigController initialized with config from: {}", path);
    }
  }

  @Security.Authenticated(Authenticator.class)
  public Result getMfeConfig() {
    if (cachedConfigContent.isEmpty()) {
      log.error("MfeConfigController: No cached config available (file: {})", configFilePath);
      return Results.internalServerError("MfeConfigController error! Unable to read config file!");
    }
    log.debug("MfeConfigController: Serving cached config");
    return Results.ok(cachedConfigContent)
        .as("application/yaml")
        .withHeader("Cache-Control", "private, max-age=" + CACHE_MAX_AGE_SECONDS);
  }

  /**
   * Reads the YAML config file from disk. This is called once at startup.
   *
   * @param configFilePath path to the config file
   * @param environment Play environment for file resolution
   * @return the file contents, or empty string on error
   */
  private String readFromYMLConfig(String configFilePath, Environment environment) {
    log.debug("Reading YAML config from: {}", configFilePath);
    try {
      String content = Files.readString(environment.getFile(configFilePath).toPath());
      log.debug("Successfully read YAML config ({} bytes)", content.length());
      return content;
    } catch (Exception e) {
      log.error(
          "MfeConfigController error! Failed to read YAML config file at: {}. Error: {}.",
          configFilePath,
          e.getMessage());
      return "";
    }
  }
}
