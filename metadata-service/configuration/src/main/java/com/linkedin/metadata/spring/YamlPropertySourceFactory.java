package com.linkedin.metadata.spring;

import java.io.IOException;
import java.util.Properties;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;

/** Required for Spring to parse the application.yaml provided by this module */
public class YamlPropertySourceFactory implements PropertySourceFactory {

  @Override
  public PropertySource<?> createPropertySource(String name, EncodedResource encodedResource)
      throws IOException {
    YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
    factory.setResources(encodedResource.getResource());

    Properties properties = factory.getObject();

    // Honor an explicit @PropertySource(name=...) when given so callers can disambiguate sources
    // that share a filename (e.g. a mounted override named the same as the bundled default).
    // Falls back to the filename to preserve existing behavior for nameless @PropertySource usage.
    String sourceName =
        (name != null && !name.isEmpty()) ? name : encodedResource.getResource().getFilename();
    return new PropertiesPropertySource(sourceName, properties);
  }
}
