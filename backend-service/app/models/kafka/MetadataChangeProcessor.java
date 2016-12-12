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
package models.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import models.daos.DatasetInfoDao;
import org.apache.avro.generic.GenericData;
import play.Logger;
import wherehows.common.schemas.Record;


public class MetadataChangeProcessor {

  private final String[] CHANGE_ITEMS =
      {"schema", "owners", "datasetProperties", "references", "partitionSpec", "deploymentInfo", "tags",
          "constraints", "indices", "capacity", "privacyCompliancePolicy", "securitySpecification"};

  /**
   * Process a MetadataChangeEvent record
   * @param record GenericData.Record
   * @param topic String
   * @throws Exception
   * @return null
   */
  public Record process(GenericData.Record record, String topic)
      throws Exception {
    if (record != null) {
      Logger.debug("Processing Metadata Change Event record. ");

      final GenericData.Record auditHeader = (GenericData.Record) record.get("auditHeader");
      if (auditHeader == null || auditHeader.get("time") == null) {
        Logger.info("MetadataChangeEvent without auditHeader, abort process. " + record.toString());
        return null;
      }

      final GenericData.Record datasetIdentifier = (GenericData.Record) record.get("datasetIdentifier");
      final GenericData.Record datasetProperties = (GenericData.Record) record.get("datasetProperties");
      final String urn = String.valueOf(record.get("urn"));

      if (urn == null && (datasetProperties == null || datasetProperties.get("uri") == null)
          && datasetIdentifier == null) {
        Logger.info("Can't identify dataset from uri/urn/datasetIdentifier, abort process. " + record.toString());
        return null;
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
              DatasetInfoDao.updateDatasetSchema(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: schema ", ex);
            }
            break;
          case "owners":
            try {
              DatasetInfoDao.updateDatasetOwner(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: owner ", ex);
            }
            break;
          case "datasetProperties":
            try {
              DatasetInfoDao.updateDatasetCaseSensitivity(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: case sensitivity ", ex);
            }
            break;
          case "references":
            try {
              DatasetInfoDao.updateDatasetReference(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: reference ", ex);
            }
            break;
          case "partitionSpec":
            try {
              DatasetInfoDao.updateDatasetPartition(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: partition ", ex);
            }
            break;
          case "deploymentInfo":
            try {
              DatasetInfoDao.updateDatasetDeployment(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: deployment ", ex);
            }
            break;
          case "tags":
            try {
              DatasetInfoDao.updateDatasetTags(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: tag ", ex);
            }
            break;
          case "constraints":
            try {
              DatasetInfoDao.updateDatasetConstraint(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: constraint ", ex);
            }
            break;
          case "indices":
            try {
              DatasetInfoDao.updateDatasetIndex(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: index ", ex);
            }
            break;
          case "capacity":
            try {
              DatasetInfoDao.updateDatasetCapacity(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: capacity ", ex);
            }
            break;
          case "privacyCompliancePolicy":
            try {
              DatasetInfoDao.updateDatasetCompliance(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: compliance ", ex);
            }
            break;
          case "securitySpecification":
            try {
              DatasetInfoDao.updateDatasetSecurity(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: security ", ex);
            }
            break;
          default:
            break;
        }
      }
    }
    return null;
  }
}
