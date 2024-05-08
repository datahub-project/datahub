package com.linkedin.gms.factory.common;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ObjectMapperFactory {
  @Bean
  public ObjectMapper objectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    return objectMapper;
  }
}
