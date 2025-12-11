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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Builder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
@JsonDeserialize(builder = BoolQueryConfiguration.BoolQueryConfigurationBuilder.class)
public class BoolQueryConfiguration {
  private Object must;
  private Object should;
  // CHECKSTYLE:OFF
  private Object must_not;
  // CHECKSTYLE:ON
  private Object filter;

  @JsonPOJOBuilder(withPrefix = "")
  public static class BoolQueryConfigurationBuilder {}
}
