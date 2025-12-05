package controllers;

import auth.Authenticator;
import com.typesafe.config.Config;
import java.nio.file.Files;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import play.Environment;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Results;
import play.mvc.Security;

@Slf4j
public class MfeConfigController extends Controller {

  private final String configFilePath;
  private final Environment environment;

  @Inject
  public MfeConfigController(@Nonnull Config configs, Environment environment) {
    String path = configs.getString("mfeConfigFilePath");
    if (path == null) {
      throw new IllegalArgumentException("MfeConfigFilePath is null or not set!");
    }
    this.configFilePath = path;
    this.environment = environment;
  }

  @Security.Authenticated(Authenticator.class)
  public Result getMfeConfig() {
    if (configFilePath == null) {
      log.error("MfeConfigController Config File Path not set!");
      return Results.internalServerError("MfeConfigController error! Config File Path not set!");
    }
    String yamlContent = readFromYMLConfig(configFilePath);
    if (yamlContent.isEmpty()) {
      log.error("MFEConfigController unable to read config file at: {}", configFilePath);
      return Results.internalServerError("MFEConfigController error! Unable to read config file!");
    } else {
      log.info("MfeConfigController read yaml success");
      return Results.ok(yamlContent).as("application/yaml");
    }
  }

  public String readFromYMLConfig(String configFilePath) {
    log.info("Attempting to read YAML config from: {}", configFilePath);
    try {
      String content = Files.readString(environment.getFile(configFilePath).toPath());
      log.info("Successfully read YAML config");
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
