/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
