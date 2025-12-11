/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.utils.elasticsearch.responses;

import java.util.List;
import java.util.Map;
import lombok.Data;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;

@Data
public class GetIndexResponse {
  private final String[] indices;
  private final Map<String, MappingMetadata> mappings;
  private final Map<String, List<AliasMetadata>> aliases;
  private final Map<String, Settings> settings;
  private final Map<String, Settings> defaultSettings;
  private final Map<String, String> dataStreams;

  public String getSetting(String index, String setting) {
    Settings indexSettings = this.settings.get(index);
    String settingValue = null;
    if (setting != null) {
      if (indexSettings != null) {
        settingValue = indexSettings.get(setting);
      }
      if (settingValue == null) {
        Settings defaultIndexSettings = this.defaultSettings.get(index);
        settingValue = defaultIndexSettings != null ? defaultIndexSettings.get(setting) : null;
      }
    }
    return settingValue;
  }
}
