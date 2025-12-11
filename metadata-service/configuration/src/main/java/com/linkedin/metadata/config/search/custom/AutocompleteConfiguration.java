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
@JsonDeserialize(builder = AutocompleteConfiguration.AutocompleteConfigurationBuilder.class)
public class AutocompleteConfiguration {
  // match this configuration based on query string regex match
  private String queryRegex;
  // include the default autocomplete query
  @Builder.Default private boolean defaultQuery = true;
  // override or extend default autocomplete query
  private BoolQueryConfiguration boolQuery;
  // inherit the query configuration's function score (disabled if functionScore exists)
  @Builder.Default private boolean inheritFunctionScore = true;

  // additional function scores to apply for ranking
  @Builder.Default private Map<String, Object> functionScore = Collections.emptyMap();

  @JsonPOJOBuilder(withPrefix = "")
  public static class AutocompleteConfigurationBuilder {}
}
