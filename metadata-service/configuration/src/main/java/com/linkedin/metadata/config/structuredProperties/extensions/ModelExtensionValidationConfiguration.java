package com.linkedin.metadata.config.structuredProperties.extensions;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Data
@Slf4j
public class ModelExtensionValidationConfiguration {
  private boolean enabled;
  private String configFile;

  /**
   * Materialize the search configuration from a location external to main application.yaml
   *
   * @param mapper yaml enabled jackson mapper
   * @return search configuration class
   * @throws IOException
   */
  public ExtendedModelValidationConfiguration resolve(ObjectMapper mapper) throws IOException {
    if (enabled) {
      log.info("Custom model extension validation configuration enabled.");
      try (InputStream stream = new ClassPathResource(configFile).getInputStream()) {
        log.info(
            "Custom model extension alternate validation mapping configuration found in classpath: {}",
            configFile);
        return mapper.readValue(stream, ExtendedModelValidationConfiguration.class);
      } catch (FileNotFoundException e) {
        log.info(
            "Custom model extension alternate validation configuration was NOT found in the classpath.");
        try (InputStream stream = new FileSystemResource(configFile).getInputStream()) {
          log.info(
              "Custom model extension alternate validation configuration found in filesystem: {}",
              configFile);
          return mapper.readValue(stream, ExtendedModelValidationConfiguration.class);
        } catch (Exception e2) {
          log.warn(
              "Custom model extension alternate validation enabled, however there was an error loading configuration: "
                  + configFile,
              e2);
          return null;
        }
      }
    } else {
      log.info("Custom model extension alternate validation configuration disabled.");
      return null;
    }
  }
}
