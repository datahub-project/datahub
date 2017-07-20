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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import models.daos.DatasetDao;
import models.daos.DatasetInfoDao;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import wherehows.common.schemas.DatasetRecord;
import wherehows.common.schemas.Record;
import wherehows.common.utils.StringUtil;
import play.libs.Json;
import play.Logger;


public class GobblinTrackingAuditProcessor extends KafkaConsumerProcessor {

  private static final String DALI_LIMITED_RETENTION_AUDITOR = "DaliLimitedRetentionAuditor";
  private static final String DALI_AUTOPURGED_AUDITOR = "DaliAutoPurgeAuditor";
  private static final String DS_IGNORE_IDPC_AUDITOR = "DsIgnoreIDPCAuditor";
  private static final String METADATA_FILE_CLASSIFIER = "MetadataFileClassifier";
  private static final String DATASET_URN_PREFIX = "hdfs://";
  private static final String DATASET_OWNER_SOURCE = "IDPC";

  // TODO: Make these regex patterns part of job file
  private static final Pattern LOCATION_PREFIX_PATTERN = Pattern.compile("/[^/]+(/[^/]+)?");

  private static final Pattern SHORT_NAME_PATTERN = Pattern.compile("(/[^/]+/[^/]+)$");

  private static final List<Pattern> PARENT_PATTERNS = ImmutableList.<Pattern>builder()
      .add(Pattern.compile("/data/external/gobblin/(.+)"))
      .add(Pattern.compile("/data/(databases|dbchange|external)/.+"))
      .add(Pattern.compile("/([^/]*data)/tracking/.+"))
      .add(Pattern.compile("/([^/]*data)/derived/.+"))
      .add(Pattern.compile("/(data)/service/.+"))
      .add(Pattern.compile("/([^/]+)/.+"))
      .build();

  private static final List<Pattern> BLACKLISTED_DATASET_PATTERNS = ImmutableList.<Pattern>builder()
      .add(Pattern.compile("(\\b|_)temporary(\\b|_)"))
      .add(Pattern.compile("(\\b|_)temp(\\b|_)"))
      .add(Pattern.compile("(\\b|_)tmp(\\b|_)"))
      .add(Pattern.compile("(\\b|_)staging(\\b|_)"))
      .add(Pattern.compile("(\\b|_)stg(\\b|_)"))
      .add(Pattern.compile("_distcp_"))
      .add(Pattern.compile("/output/"))
      .build();

  /**
   * Process a Gobblin tracking event audit record
   * @param record
   * @param topic
   * @return null
   * @throws Exception
   */
  public Record process(GenericData.Record record, String topic) throws Exception {

    if (record == null || record.get("name") == null) {
      return null;
    }

    final String name = record.get("name").toString();
    // only handle "DaliLimitedRetentionAuditor","DaliAutoPurgeAuditor" and "DsIgnoreIDPCAuditor"
    if (name.equals(DALI_LIMITED_RETENTION_AUDITOR) || name.equals(DALI_AUTOPURGED_AUDITOR) || name.equals(
        DS_IGNORE_IDPC_AUDITOR)) {
      // TODO: Re-enable this once it's fixed.
      //updateKafkaDatasetOwner(record);
    } else if (name.equals(METADATA_FILE_CLASSIFIER)) {
      updateHdfsDatasetSchema(record);
    }

    return null;
  }

  private void updateKafkaDatasetOwner(GenericData.Record record) throws Exception {
    Long timestamp = (Long) record.get("timestamp");
    Map<String, String> metadata = StringUtil.convertObjectMapToStringMap(record.get("metadata"));

    String hasError = metadata.get("HasError");
    if (!hasError.equalsIgnoreCase("true")) {
      String datasetPath = metadata.get("DatasetPath");
      String datasetUrn = DATASET_URN_PREFIX + (datasetPath.startsWith("/") ? "" : "/") + datasetPath;
      String ownerUrns = metadata.get("OwnerURNs");
      DatasetInfoDao.updateKafkaDatasetOwner(datasetUrn, ownerUrns, DATASET_OWNER_SOURCE, timestamp);
    }
  }

  private void updateHdfsDatasetSchema(GenericData.Record record) throws Exception {
    Long timestamp = (Long) record.get("timestamp");
    Map<String, String> metadata = StringUtil.convertObjectMapToStringMap(record.get("metadata"));

    String datasetName = metadata.get("dataset");
    if (StringUtils.isEmpty(datasetName) || isDatasetNameBlacklisted(datasetName)) {
      Logger.info("Skipped processing metadata event for dataset {}", datasetName);
      return;
    }

    DatasetRecord dataset = new DatasetRecord();
    dataset.setName(getShortName(datasetName));
    dataset.setUrn("hdfs://" + datasetName);
    dataset.setSchema(metadata.get("schema"));
    dataset.setSchemaType("JSON");
    dataset.setSource("Hdfs");
    dataset.setParentName(getParentName(datasetName));
    dataset.setDatasetType("hdfs");
    dataset.setIsActive(true);
    dataset.setSourceModifiedTime(getsourceModifiedTime(metadata.get("modificationTime")));

    Matcher matcher = LOCATION_PREFIX_PATTERN.matcher(datasetName);
    if (matcher.lookingAt()) {
      dataset.setLocationPrefix(matcher.group());
    }

    ObjectNode properties = Json.newObject();
    properties.put("owner", metadata.get("owner"));
    properties.put("group", metadata.get("group"));
    properties.put("file_permission", metadata.get("permission"));
    properties.put("codec", metadata.get("codec"));
    properties.put("storage", metadata.get("storage"));
    properties.put("cluster", metadata.get("cluster"));
    properties.put("abstract_path", metadata.get("abstractPath"));
    dataset.setProperties(new ObjectMapper().writeValueAsString(properties));

    Logger.info("Updating dataset {}", datasetName);
    DatasetDao.setDatasetRecord(dataset);
  }

  private boolean isDatasetNameBlacklisted(String datasetName) {
    for (Pattern pattern : BLACKLISTED_DATASET_PATTERNS) {
      if (pattern.matcher(datasetName).find()) {
        return true;
      }
    }
    return false;
  }

  private String getShortName(String datasetName) {
    Matcher matcher = SHORT_NAME_PATTERN.matcher(datasetName);
    if (matcher.find()) {
      return matcher.group();
    }
    return "";
  }

  private String getParentName(String datasetName) {
    for (Pattern pattern : PARENT_PATTERNS) {
      Matcher matcher = pattern.matcher(datasetName);
      if (matcher.matches()) {
        return matcher.group();
      }
    }
    return "";
  }

  private String getsourceModifiedTime(String hdfsModifiedTime) {
    if (hdfsModifiedTime == null) {
      return null;
    }
    return Long.toString(Long.parseLong(hdfsModifiedTime) / 1000);
  }
}
