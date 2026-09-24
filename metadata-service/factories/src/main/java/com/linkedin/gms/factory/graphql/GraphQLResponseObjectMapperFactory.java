package com.linkedin.gms.factory.graphql;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_NAME_LENGTH;
import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_NAME_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * ObjectMapper for GraphQL responses, shared by the controller's buffered path and {@link
 * GraphQLResponseBodyConverter}'s streaming path. Distinct from the primary mapper because it must
 * NOT use {@code JsonInclude.NON_NULL}: GraphQL emits a requested-but-null field as an explicit
 * JSON {@code null}, not an omitted key. Named (non-primary), so unqualified {@code ObjectMapper}
 * injections still resolve to the primary one.
 */
@Configuration
public class GraphQLResponseObjectMapperFactory {

  public static final String GRAPHQL_RESPONSE_OBJECT_MAPPER = "graphQLResponseObjectMapper";

  @Bean(name = GRAPHQL_RESPONSE_OBJECT_MAPPER)
  @Nonnull
  public ObjectMapper graphQLResponseObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    int maxNameLength =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_NAME_LENGTH, MAX_JACKSON_NAME_LENGTH));
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder()
                .maxStringLength(maxSize)
                .maxNameLength(maxNameLength)
                .build());
    mapper.registerModule(new Jdk8Module());
    return mapper;
  }
}
