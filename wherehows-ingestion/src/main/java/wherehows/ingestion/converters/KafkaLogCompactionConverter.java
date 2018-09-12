/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.ingestion.converters;

import com.linkedin.events.metadata.DeploymentDetail;
import com.linkedin.events.metadata.MetadataChangeEvent;


/**
 * Convert to change kafka log compaction events to kafka-lc platform.
 * Check additionalDeploymentInfo map to see if contains "compact" retention policy.
 */
public class KafkaLogCompactionConverter implements KafkaMessageConverter<MetadataChangeEvent> {

  private final String kafkaPlatformUrn = "urn:li:dataPlatform:kafka";

  private final String kafkaLogCompactionPlatformUrn = "urn:li:dataPlatform:kafka-lc";

  public MetadataChangeEvent convert(MetadataChangeEvent event) {
    if (!kafkaPlatformUrn.equalsIgnoreCase(event.datasetIdentifier.dataPlatformUrn.toString())
        || event.deploymentInfo == null) {
      return event;
    }

    boolean logCompact = false;
    for (DeploymentDetail deployment : event.deploymentInfo) {
      if (deployment.additionalDeploymentInfo.containsValue("compact")) {
        logCompact = true;
        break;
      }
    }

    if (logCompact) {
      event.datasetIdentifier.dataPlatformUrn = kafkaLogCompactionPlatformUrn;
    }
    return event;
  }
}
