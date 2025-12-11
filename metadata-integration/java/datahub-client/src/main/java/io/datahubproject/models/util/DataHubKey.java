/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.models.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class DataHubKey {
  // Static ObjectMapper instance since it's thread-safe and expensive to create
  protected static final ObjectMapper MAPPER = new ObjectMapper();
  // Static TypeReference instance since it doesn't change
  private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, String>>() {};

  static {
    MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  public Map<String, String> guidDict() {
    return MAPPER.convertValue(this, MAP_TYPE_REFERENCE);
  }

  public String guid() {
    Map<String, String> bag = guidDict();
    return DataHubGuidGenerator.dataHubGuid(bag);
  }
}
