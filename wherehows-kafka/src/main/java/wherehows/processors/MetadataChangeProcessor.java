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
package wherehows.processors;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import wherehows.service.MetadataChangeService;


@Slf4j
@RequiredArgsConstructor
public class MetadataChangeProcessor extends KafkaConsumerProcessor {

  private final MetadataChangeService metadataChangeService;
  private final String[] CHANGE_ITEMS =
      {"schema", "owners", "datasetProperties", "references", "partitionSpec", "deploymentInfo", "tags", "constraints", "indices", "capacity", "privacyCompliancePolicy", "securitySpecification"};

  /**
   * Process a MetadataChangeEvent record
   * @param record GenericData.Record
   * @param topic String
   * @throws Exception
   * @return null
   */
  public void process(GenericData.Record record, String topic) throws Exception {
    if (record != null) {
      log.debug("Processing Metadata Change Event record. ");

      final GenericData.Record auditHeader = (GenericData.Record) record.get("auditHeader");
      if (auditHeader == null || auditHeader.get("time") == null) {
        log.info("MetadataChangeEvent without auditHeader, abort process. " + record.toString());
        return;
      }

      final GenericData.Record datasetIdentifier = (GenericData.Record) record.get("datasetIdentifier");
      final GenericData.Record datasetProperties = (GenericData.Record) record.get("datasetProperties");
      final String urn = String.valueOf(record.get("urn"));

      if (urn == null && (datasetProperties == null || datasetProperties.get("uri") == null)
          && datasetIdentifier == null) {
        log.info("Can't identify dataset from uri/urn/datasetIdentifier, abort process. " + record.toString());
        return;
      } else if (urn != null) {
        log.debug("URN: " + urn);
      } else if (datasetProperties != null && datasetProperties.get("uri") != null) {
        log.debug("URI: " + datasetProperties.get("uri"));
      } else {
        log.debug(
            "Dataset Identifier: " + datasetIdentifier.get("dataPlatformUrn") + datasetIdentifier.get("nativeName"));
      }

      final JsonNode rootNode = new ObjectMapper().readTree(record.toString());

      for (String itemName : CHANGE_ITEMS) {
        // check if the corresponding change field has content
        if (record.get(itemName) == null) {
          continue;
        }

        switch (itemName) {
          case "schema":
            try {
              metadataChangeService.updateDatasetSchema(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: schema ", ex);
            }
            break;
          case "owners":
            try {
              metadataChangeService.updateDatasetOwner(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: owner ", ex);
            }
            break;
          case "datasetProperties":
            try {
              metadataChangeService.updateDatasetCaseSensitivity(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: case sensitivity ", ex);
            }
            break;
          case "references":
            try {
              metadataChangeService.updateDatasetReference(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: reference ", ex);
            }
            break;
          case "partitionSpec":
            try {
              metadataChangeService.updateDatasetPartition(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: partition ", ex);
            }
            break;
          case "deploymentInfo":
            try {
              metadataChangeService.updateDatasetDeployment(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: deployment ", ex);
            }
            break;
          case "tags":
            try {
              metadataChangeService.updateDatasetTags(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: tag ", ex);
            }
            break;
          case "constraints":
            try {
              metadataChangeService.updateDatasetConstraint(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: constraint ", ex);
            }
            break;
          case "indices":
            try {
              metadataChangeService.updateDatasetIndex(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: index ", ex);
            }
            break;
          case "capacity":
            try {
              metadataChangeService.updateDatasetCapacity(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: capacity ", ex);
            }
            break;
          case "privacyCompliancePolicy":
            try {
              metadataChangeService.updateDatasetCompliance(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: compliance ", ex);
            }
            break;
          case "securitySpecification":
            try {
              metadataChangeService.updateDatasetSecurity(rootNode);
            } catch (Exception ex) {
              log.debug("Metadata change exception: security ", ex);
            }
            break;
          default:
            break;
        }
      }
    }
  }
}
