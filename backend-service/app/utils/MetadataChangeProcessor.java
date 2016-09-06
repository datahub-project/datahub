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
package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import models.daos.DatasetInfoDao;
import org.apache.avro.generic.GenericData;
import play.Logger;
import wherehows.common.schemas.Record;


public class MetadataChangeProcessor {

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
      // Logger.info("Processing Metadata Change Event record. ");

      final GenericData.Record auditHeader = (GenericData.Record) record.get("auditHeader");
      final GenericData.Array<GenericData.Record> changedItems =
          (GenericData.Array<GenericData.Record>) record.get("changedItems");

      if (auditHeader == null || auditHeader.get("time") == null) {
        Logger.info("MetadataChangeEvent without auditHeader, abort process. " + record.toString());
        return null;
      }

      // list change items, schema first
      List<String> changes = new ArrayList<>();
      for (GenericData.Record item : changedItems) {
        switch (item.get("changeScope").toString().toLowerCase()) { // can't be null
          case "*":
            Collections.addAll(changes, "schemas", "owners", "references", "partitionSpec", "deploymentInfo",
                "caseSensitivity", "tags", "constraints", "indices", "capacity", "securitySpec");
            break;
          case "schema":
            if (!changes.contains("schemas")) {
              changes.add(0, "schemas"); // add to front
            }
            break;
          case "owner":
            if (!changes.contains("owners")) {
              changes.add("owners");
            }
            break;
          case "reference":
            if (!changes.contains("references")) {
              changes.add("references");
            }
            break;
          case "partition":
            if (!changes.contains("partitionSpec")) {
              changes.add("partitionSpec");
            }
            break;
          case "deployment":
            if (!changes.contains("deploymentInfo")) {
              changes.add("deploymentInfo");
            }
            break;
          case "casesensitivity":
            if (!changes.contains("caseSensitivity")) {
              changes.add("caseSensitivity");
            }
            break;
          case "tag":
            if (!changes.contains("tags")) {
              changes.add("tags");
            }
            break;
          case "constraint":
            if (!changes.contains("constraints")) {
              changes.add("constraints");
            }
            break;
          case "index":
            if (!changes.contains("indices")) {
              changes.add("indices");
            }
            break;
          case "capacity":
            if (!changes.contains("capacity")) {
              changes.add("capacity");
            }
            break;
          case "security":
            if (!changes.contains("securitySpec")) {
              changes.add("securitySpec");
            }
            break;
          default:
            break;
        }
      }
      Logger.debug("Updating dataset " + changedItems.toString());

      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readTree(record.toString());

      for (String itemName : changes) {
        // check if the corresponding change field has content
        if (record.get(itemName) == null) {
          continue;
        }

        switch (itemName) {
          case "schemas":
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
          case "caseSensitivity":
            try {
              DatasetInfoDao.updateDatasetCaseSensitivity(rootNode);
            } catch (Exception ex) {
              Logger.debug("Metadata change exception: case sensitivity ", ex);
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
          case "securitySpec":
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
