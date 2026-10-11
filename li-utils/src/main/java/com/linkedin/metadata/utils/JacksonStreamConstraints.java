package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_NAME_LENGTH;
import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_NAME_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.annotation.Nonnull;

/**
 * Shared Jackson stream-read limits from ingestion env overrides. Prefer this over duplicating the
 * env-var list at each ObjectMapper construction site.
 */
public final class JacksonStreamConstraints {

  private JacksonStreamConstraints() {}

  @Nonnull
  public static StreamReadConstraints streamReadConstraints() {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    int maxNameLength =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_NAME_LENGTH, MAX_JACKSON_NAME_LENGTH));
    return StreamReadConstraints.builder()
        .maxStringLength(maxSize)
        .maxNameLength(maxNameLength)
        .build();
  }

  /** Apply {@link #streamReadConstraints()} to {@code mapper}'s factory. */
  public static void applyTo(@Nonnull ObjectMapper mapper) {
    mapper.getFactory().setStreamReadConstraints(streamReadConstraints());
  }

  /** New {@link ObjectMapper} with ingestion stream-read constraints applied. */
  @Nonnull
  public static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    applyTo(mapper);
    return mapper;
  }
}
