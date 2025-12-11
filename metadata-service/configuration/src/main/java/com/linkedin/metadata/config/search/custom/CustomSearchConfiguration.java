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
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
@JsonDeserialize(builder = CustomSearchConfiguration.CustomSearchConfigurationBuilder.class)
public class CustomSearchConfiguration {

  @Builder.Default
  private Map<String, FieldConfiguration> fieldConfigurations = Collections.emptyMap();

  @Builder.Default private List<QueryConfiguration> queryConfigurations = Collections.emptyList();

  @Builder.Default
  private List<AutocompleteConfiguration> autocompleteConfigurations = Collections.emptyList();

  @JsonPOJOBuilder(withPrefix = "")
  public static class CustomSearchConfigurationBuilder {}
}
