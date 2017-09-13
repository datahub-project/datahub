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
package wherehows.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import gobblin.metrics.GobblinTrackingEvent_audit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.persistence.NoResultException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import wherehows.common.utils.StringUtil;
import wherehows.dao.table.DatasetClassificationDao;
import wherehows.dao.table.DictDatasetDao;
import wherehows.models.table.DatasetClassification;
import wherehows.models.table.DictDataset;


@Slf4j
@RequiredArgsConstructor
public class GobblinTrackingAuditService {

  private static final String DATASET_URN_PREFIX = "hdfs://";

  private final DatasetClassificationDao datasetClassificationDao;
  private final DictDatasetDao dictDatasetDao;

  // TODO: Make these regex patterns part of job file
  private static final Pattern LOCATION_PREFIX_PATTERN = Pattern.compile("/[^/]+(/[^/]+)?");

  private static final Pattern SHORT_NAME_PATTERN = Pattern.compile("(/[^/]+/[^/]+)$");

  private static final List<Pattern> PARENT_PATTERNS =
      ImmutableList.<Pattern>builder().add(Pattern.compile("/data/external/gobblin/(.+)"))
          .add(Pattern.compile("/data/(databases|dbchange|external)/.+"))
          .add(Pattern.compile("/([^/]*data)/tracking/.+"))
          .add(Pattern.compile("/([^/]*data)/derived/.+"))
          .add(Pattern.compile("/(data)/service/.+"))
          .add(Pattern.compile("/([^/]+)/.+"))
          .build();

  private static final List<Pattern> BLACKLISTED_DATASET_PATTERNS =
      ImmutableList.<Pattern>builder().add(Pattern.compile("(\\b|_)temporary(\\b|_)"))
          .add(Pattern.compile("(\\b|_)temp(\\b|_)"))
          .add(Pattern.compile("(\\b|_)tmp(\\b|_)"))
          .add(Pattern.compile("(\\b|_)staging(\\b|_)"))
          .add(Pattern.compile("(\\b|_)stg(\\b|_)"))
          .add(Pattern.compile("_distcp_"))
          .add(Pattern.compile("/output/"))
          .build();

  public void updateHdfsDatasetSchema(GobblinTrackingEvent_audit record) throws Exception {
    Long timestamp = record.timestamp;
    Map<String, String> metadata = StringUtil.toStringMap(record.metadata);

    String datasetName = metadata.get("dataset");
    if (StringUtils.isEmpty(datasetName) || isDatasetNameBlacklisted(datasetName)) {
      log.info("Skipped processing metadata event for dataset {}", datasetName);
      return;
    }

    String urn = DATASET_URN_PREFIX + datasetName;
    DictDataset dataset;
    try {
      dataset = dictDatasetDao.findByUrn(urn);
    } catch (NoResultException e) {
      dataset = new DictDataset();
    }
    dataset.setName(getShortName(datasetName));
    dataset.setUrn(urn);
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

    ObjectNode properties = new ObjectMapper().createObjectNode();
    properties.put("owner", metadata.get("owner"));
    properties.put("group", metadata.get("group"));
    properties.put("file_permission", metadata.get("permission"));
    properties.put("codec", metadata.get("codec"));
    properties.put("storage", metadata.get("storage"));
    properties.put("cluster", metadata.get("cluster"));
    properties.put("abstract_path", metadata.get("abstractPath"));
    dataset.setProperties(new ObjectMapper().writeValueAsString(properties));

    log.info("Updating dataset {}", datasetName);
    dictDatasetDao.update(dataset);

    String classificationResult = metadata.get("classificationResult");
    if (classificationResult != null && !classificationResult.equals("null")) {
      updateDatasetClassificationResult(urn, classificationResult);
    } else {
      log.warn("skip insertion since classification result is empty");
    }
  }

  private void updateDatasetClassificationResult(String urn, String classificationResult) {
    try {
      DatasetClassification record = new DatasetClassification(urn, classificationResult, new Date());
      datasetClassificationDao.update(record);
    } catch (Exception e) {
      log.warn("unable to update classification result due to {}", e.getMessage());
    }
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

  //TODO the return time should be timeStamp
  private int getsourceModifiedTime(String hdfsModifiedTime) {
    long result = Long.parseLong(hdfsModifiedTime) / 1000;
    if (hdfsModifiedTime == null || result > Integer.MAX_VALUE) {
      return 0;
    }
    return (int) result;
  }
}
