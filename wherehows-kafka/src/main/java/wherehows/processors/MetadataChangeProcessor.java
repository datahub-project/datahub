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
import com.linkedin.events.KafkaAuditHeader;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.DatasetProperty;
import com.linkedin.events.metadata.MetadataChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import wherehows.dao.DaoFactory;
import wherehows.service.MetadataChangeService;


@Slf4j
public class MetadataChangeProcessor extends KafkaMessageProcessor {

  private final MetadataChangeService metadataChangeService;

  public MetadataChangeProcessor(DaoFactory daoFactory, KafkaProducer<String, IndexedRecord> producer) {
    super(daoFactory, producer);
    metadataChangeService =
        new MetadataChangeService(DAO_FACTORY.getDictDatasetDao(), DAO_FACTORY.getDictFieldDetailDao(),
            DAO_FACTORY.getDatasetSchemaInfoDao());
  }

  private final String[] CHANGE_ITEMS =
      {"schema", "owners", "datasetProperties", "references", "partitionSpec", "deploymentInfo", "tags", "constraints", "indices", "capacity", "privacyCompliancePolicy", "securitySpecification"};

  /**
   * Process a MetadataChangeEvent record
   * @param indexedRecord IndexedRecord
   * @throws Exception
   */
  public void process(IndexedRecord indexedRecord) throws Exception {

    if (indexedRecord != null && indexedRecord.getClass() == MetadataChangeEvent.class) {
      log.debug("Processing Metadata Change Event record. ");

      MetadataChangeEvent record = (MetadataChangeEvent) indexedRecord;

      final KafkaAuditHeader auditHeader = record.auditHeader;
      if (auditHeader == null) {
        log.info("MetadataChangeEvent without auditHeader, abort process. " + record.toString());
        return;
      }

      final DatasetIdentifier datasetIdentifier = record.datasetIdentifier;
      final DatasetProperty datasetProperties = record.datasetProperty;
      //final String urn = String.valueOf(record.get("urn"));

      final JsonNode rootNode = new ObjectMapper().readTree(record.toString());

      for (String itemName : CHANGE_ITEMS) {

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
