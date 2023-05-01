package com.linkedin.metadata.config.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;


@Data
@AllArgsConstructor
@Slf4j
public class CustomConfiguration {

  private boolean configEnabled;
  private String configFile;

  /**
   * Materialize the search configuration from a location external to main application.yml
   * @param mapper yaml enabled jackson mapper
   * @return search configuration class
   * @throws IOException
   */
  public CustomSearchConfiguration customSearchConfiguration(ObjectMapper mapper) throws IOException {
    if (configEnabled) {
      log.info("Custom search configuration enabled.");
      try (InputStream stream = new ClassPathResource(configFile).getInputStream()) {
        log.info("Custom search configuration found in classpath: {}", configFile);
        return mapper.readValue(stream, CustomSearchConfiguration.class);
      } catch (FileNotFoundException e) {
        try (InputStream stream = new FileSystemResource(configFile).getInputStream()) {
          log.info("Custom search configuration found in filesystem: {}", configFile);
          return mapper.readValue(stream, CustomSearchConfiguration.class);
        }
      }
    } else {
      log.info("Custom search configuration disabled.");
      return null;
    }
  }
}
