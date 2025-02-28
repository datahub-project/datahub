package com.linkedin.metadata.config.search.custom;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
@JsonDeserialize(builder = QueryConfiguration.QueryConfigurationBuilder.class)
public class QueryConfiguration {

  private String queryRegex;
  @Builder.Default private boolean simpleQuery = true;

  /**
   * Used to determine if standard structured query logic should be applied when relevant, i.e.
   * fullText flag is false. Will not be added in cases where simpleQuery would be the standard.
   */
  @Builder.Default private boolean structuredQuery = true;

  @Builder.Default private boolean exactMatchQuery = true;
  @Builder.Default private boolean prefixMatchQuery = true;
  private BoolQueryConfiguration boolQuery;
  @Builder.Default private Map<String, Object> functionScore = Collections.emptyMap();

  @JsonPOJOBuilder(withPrefix = "")
  public static class QueryConfigurationBuilder {}
}
