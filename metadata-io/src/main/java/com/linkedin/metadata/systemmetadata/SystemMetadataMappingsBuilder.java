/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.systemmetadata;

import static io.datahubproject.metadata.context.SystemTelemetryContext.TELEMETRY_TRACE_KEY;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemMetadataMappingsBuilder {

  private SystemMetadataMappingsBuilder() {}

  public static Map<String, Object> getMappings() {
    Map<String, Object> mappings = new HashMap<>();
    mappings.put("urn", getMappingsForKeyword());
    mappings.put("aspect", getMappingsForKeyword());
    mappings.put("runId", getMappingsForKeyword());
    mappings.put("lastUpdated", getMappingsForLastUpdated());
    mappings.put("registryVersion", getMappingsForKeyword());
    mappings.put("registryName", getMappingsForKeyword());
    mappings.put("removed", getMappingsForRemoved());
    mappings.put(TELEMETRY_TRACE_KEY, getMappingsForKeyword());
    return ImmutableMap.of("properties", mappings);
  }

  private static Map<String, Object> getMappingsForKeyword() {
    return ImmutableMap.<String, Object>builder().put("type", "keyword").build();
  }

  private static Map<String, Object> getMappingsForLastUpdated() {
    return ImmutableMap.<String, Object>builder().put("type", "long").build();
  }

  private static Map<String, Object> getMappingsForRemoved() {
    return ImmutableMap.<String, Object>builder().put("type", "boolean").build();
  }
}
