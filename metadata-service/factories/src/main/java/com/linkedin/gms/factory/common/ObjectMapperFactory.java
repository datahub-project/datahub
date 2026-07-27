package com.linkedin.gms.factory.common;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.linkedin.metadata.utils.JacksonStreamConstraints;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class ObjectMapperFactory {

  /**
   * Isolated ObjectMapper for API request parsing. Disables {@link
   * StreamReadFeature#INCLUDE_SOURCE_IN_LOCATION} so that Jackson parse errors never include raw
   * request content — defense-in-depth against CWE-200 information disclosure. Not a Spring bean to
   * avoid ambiguity with the primary ObjectMapper.
   */
  public static final ObjectMapper API_SANITIZING_MAPPER;

  static {
    JsonFactory factory =
        JsonFactory.builder()
            .streamReadConstraints(JacksonStreamConstraints.streamReadConstraints())
            .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
            .build();
    API_SANITIZING_MAPPER = new ObjectMapper(factory);
    API_SANITIZING_MAPPER.registerModule(new Jdk8Module());
  }

  @Bean
  @Primary
  public ObjectMapper objectMapper() {
    ObjectMapper objectMapper = JacksonStreamConstraints.createObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    return objectMapper;
  }
}
