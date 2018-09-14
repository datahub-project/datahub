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

import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.DeploymentDetail;
import com.linkedin.events.metadata.MetadataChangeEvent;
import java.util.Collections;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class KafkaLogCompactionConverterTest {

  @Test
  public void testConvert() {
    MetadataChangeEvent event = new MetadataChangeEvent();
    event.datasetIdentifier = new DatasetIdentifier();
    event.datasetIdentifier.dataPlatformUrn = "urn:li:dataPlatform:kafka";

    DeploymentDetail deployment = new DeploymentDetail();
    deployment.additionalDeploymentInfo = Collections.singletonMap("EI", "compact");
    event.deploymentInfo = Collections.singletonList(deployment);

    MetadataChangeEvent newEvent = new KafkaLogCompactionConverter().convert(event);
    assertEquals(newEvent.datasetIdentifier.dataPlatformUrn, "urn:li:dataPlatform:kafka-lc");
  }

  @Test
  public void testNotConvert() {
    KafkaLogCompactionConverter converter = new KafkaLogCompactionConverter();

    MetadataChangeEvent event = new MetadataChangeEvent();
    event.datasetIdentifier = new DatasetIdentifier();
    event.datasetIdentifier.dataPlatformUrn = "foo";

    DeploymentDetail deployment = new DeploymentDetail();
    deployment.additionalDeploymentInfo = Collections.singletonMap("EI", "compact");
    event.deploymentInfo = Collections.singletonList(deployment);

    MetadataChangeEvent newEvent = converter.convert(event);
    assertEquals(newEvent.datasetIdentifier.dataPlatformUrn, "foo");

    event.datasetIdentifier.dataPlatformUrn = "urn:li:dataPlatform:kafka";
    event.deploymentInfo = null;

    newEvent = converter.convert(event);
    assertEquals(newEvent.datasetIdentifier.dataPlatformUrn, "urn:li:dataPlatform:kafka");

    event.datasetIdentifier.dataPlatformUrn = "urn:li:dataPlatform:kafka";
    deployment.additionalDeploymentInfo = Collections.singletonMap("EI", "delete");
    event.deploymentInfo = Collections.singletonList(deployment);

    newEvent = converter.convert(event);
    assertEquals(newEvent.datasetIdentifier.dataPlatformUrn, "urn:li:dataPlatform:kafka");
  }
}
