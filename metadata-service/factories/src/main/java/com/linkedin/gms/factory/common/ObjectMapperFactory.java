package com.linkedin.gms.factory.common;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
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
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    JsonFactory factory =
        JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build())
            .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
            .build();
    API_SANITIZING_MAPPER = new ObjectMapper(factory);
    API_SANITIZING_MAPPER.registerModule(new Jdk8Module());
  }

  @Bean
  @Primary
  public ObjectMapper objectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    objectMapper.registerModule(new Jdk8Module());
    return objectMapper;
  }
}
