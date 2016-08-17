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
package models.daos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import utils.JdbcUtil;
import wherehows.common.schemas.DatasetCapacityRecord;
import wherehows.common.schemas.DatasetCaseSensitiveRecord;
import wherehows.common.schemas.DatasetDeploymentRecord;
import wherehows.common.schemas.DatasetOwnerRecord;
import wherehows.common.schemas.DatasetPartitionRecord;
import wherehows.common.schemas.DatasetReferenceRecord;
import wherehows.common.schemas.DatasetSecurityRecord;
import wherehows.common.schemas.DatasetTagRecord;
import wherehows.common.utils.StringUtil;
import wherehows.common.writers.DatabaseWriter;


public class DatasetInfoDao {

  private static final String DATASET_DEPLOYMENT_TABLE = "dataset_deployment";
  private static final String DATASET_CAPACITY_TABLE = "dataset_capacity";
  private static final String DATASET_TAG_TABLE = "dataset_tag";
  private static final String DATASET_CASE_SENSITIVE_TABLE = "dataset_case_sensitivity";
  private static final String DATASET_REFERENCE_TABLE = "dataset_reference";
  private static final String DATASET_PARTITION_TABLE = "dataset_partition";
  private static final String DATASET_SECURITY_TABLE = "dataset_security";
  private static final String DATASET_OWNER_TABLE = "dataset_owner_info";

  private static final DatabaseWriter DEPLOYMENT_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_DEPLOYMENT_TABLE);
  private static final DatabaseWriter CAPACITY_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_CAPACITY_TABLE);
  private static final DatabaseWriter TAG_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_TAG_TABLE);
  private static final DatabaseWriter CASE_SENSITIVE_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_CASE_SENSITIVE_TABLE);
  private static final DatabaseWriter REFERENCE_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_REFERENCE_TABLE);
  private static final DatabaseWriter PARTITION_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_PARTITION_TABLE);
  private static final DatabaseWriter SECURITY_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_SECURITY_TABLE);
  private static final DatabaseWriter OWNER_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_OWNER_TABLE);

  public static final String GET_DATASET_DEPLOYMENT_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_DEPLOYMENT_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_DEPLOYMENT_BY_URN =
      "SELECT * FROM " + DATASET_DEPLOYMENT_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_DEPLOYMENT_BY_URN =
      "DELETE FROM " + DATASET_DEPLOYMENT_TABLE + " WHERE dataset_urn=?";

  public static final String GET_DATASET_CAPACITY_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_CAPACITY_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_CAPACITY_BY_URN =
      "SELECT * FROM " + DATASET_CAPACITY_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_CAPACITY_BY_URN =
      "DELETE FROM " + DATASET_CAPACITY_TABLE + " WHERE dataset_urn=?";

  public static final String GET_DATASET_TAG_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_TAG_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_TAG_BY_URN =
      "SELECT * FROM " + DATASET_TAG_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_TAG_BY_URN = "DELETE FROM " + DATASET_TAG_TABLE + " WHERE dataset_urn=?";

  public static final String GET_DATASET_CASE_SENSITIVE_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_CASE_SENSITIVE_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_CASE_SENSITIVE_BY_URN =
      "SELECT * FROM " + DATASET_CASE_SENSITIVE_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String GET_DATASET_REFERENCE_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_REFERENCE_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_REFERENCE_BY_URN =
      "SELECT * FROM " + DATASET_REFERENCE_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_REFERENCE_BY_URN =
      "DELETE FROM " + DATASET_REFERENCE_TABLE + " WHERE dataset_urn=?";

  public static final String GET_DATASET_PARTITION_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_PARTITION_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_PARTITION_BY_URN =
      "SELECT * FROM " + DATASET_PARTITION_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String GET_DATASET_SECURITY_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_SECURITY_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_SECURITY_BY_URN =
      "SELECT * FROM " + DATASET_SECURITY_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String GET_DATASET_OWNER_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_OWNER_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_OWNER_BY_URN =
      "SELECT * FROM " + DATASET_OWNER_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_OWNER_BY_URN =
      "DELETE FROM " + DATASET_OWNER_TABLE + " WHERE dataset_urn=?";


  public static List<Map<String, Object>> getDatasetDeploymentByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_DEPLOYMENT_BY_DATASET_ID, params);
  }

  public static List<Map<String, Object>> getDatasetDeploymentByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_DEPLOYMENT_BY_URN, params);
  }

  public static void updateDatasetDeployment(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode deployment = root.path("deployment_info");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || deployment.isMissingNode() || !deployment.isArray()) {
      throw new IllegalArgumentException(
          "Dataset deployment info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    for (final JsonNode deploymentInfo : deployment) {
      DatasetDeploymentRecord record = om.convertValue(deploymentInfo, DatasetDeploymentRecord.class);
      record.setDataset(datasetId, urn);
      record.setModifiedTime(eventTime);
      DEPLOYMENT_WRITER.append(record);
    }

    // remove old info then insert new info
    DEPLOYMENT_WRITER.execute(DELETE_DATASET_DEPLOYMENT_BY_URN, new Object[]{urn});
    DEPLOYMENT_WRITER.insert();
  }

  public static List<Map<String, Object>> getDatasetCapacityByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_CAPACITY_BY_DATASET_ID, params);
  }

  public static List<Map<String, Object>> getDatasetCapacityByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_CAPACITY_BY_URN, params);
  }

  public static void updateDatasetCapacity(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode capacity = root.path("capacity");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || capacity.isMissingNode() || !capacity.isArray()) {
      throw new IllegalArgumentException(
          "Dataset capacity info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    for (final JsonNode capacityInfo : capacity) {
      DatasetCapacityRecord record = om.convertValue(capacityInfo, DatasetCapacityRecord.class);
      record.setDataset(datasetId, urn);
      record.setModifiedTime(eventTime);
      CAPACITY_WRITER.append(record);
    }

    // remove old info then insert new info
    CAPACITY_WRITER.execute(DELETE_DATASET_CAPACITY_BY_URN, new Object[]{urn});
    CAPACITY_WRITER.insert();
  }

  public static List<Map<String, Object>> getDatasetTagByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_TAG_BY_DATASET_ID, params);
  }

  public static List<Map<String, Object>> getDatasetTagByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_TAG_BY_URN, params);
  }

  public static void updateDatasetTags(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode tags = root.path("tags");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || tags.isMissingNode() || !tags.isArray()) {
      throw new IllegalArgumentException(
          "Dataset tag info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    for (final JsonNode tag : tags) {
      DatasetTagRecord record = new DatasetTagRecord();
      record.setTag(tag.asText());
      record.setDataset(datasetId, urn);
      record.setModifiedTime(eventTime);
      TAG_WRITER.append(record);
    }

    // remove old info then insert new info
    TAG_WRITER.execute(DELETE_DATASET_TAG_BY_URN, new Object[]{urn});
    TAG_WRITER.insert();
  }

  public static Map<String, Object> getDatasetCaseSensitivityByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_CASE_SENSITIVE_BY_DATASET_ID, params);
  }

  public static Map<String, Object> getDatasetCaseSensitivityByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_CASE_SENSITIVE_BY_URN, params);
  }

  public static void updateDatasetCaseSensitivity(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode caseSensitivity = root.path("case_sensitivity");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || caseSensitivity.isMissingNode()) {
      throw new IllegalArgumentException(
          "Dataset case_sensitivity info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    DatasetCaseSensitiveRecord record = om.convertValue(caseSensitivity, DatasetCaseSensitiveRecord.class);
    record.setDataset(datasetId, urn);
    record.setModifiedTime(eventTime);
    try {
      Map<String, Object> result = getDatasetCaseSensitivityByDatasetUrn(urn);
      String[] columns = {"dataset_name", "field_name", "data_content", "modified_time"};
      Object[] columnValues =
          new Object[]{record.getDatasetName(), record.getFieldName(), record.getDataContent(), eventTime};
      String[] conditions = {"dataset_urn"};
      Object[] conditionValues = new Object[]{urn};
      CASE_SENSITIVE_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      CASE_SENSITIVE_WRITER.append(record);
      CASE_SENSITIVE_WRITER.insert();
    }
  }

  public static List<Map<String, Object>> getDatasetReferenceByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_REFERENCE_BY_DATASET_ID, params);
  }

  public static List<Map<String, Object>> getDatasetReferenceByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_REFERENCE_BY_URN, params);
  }

  public static void updateDatasetReference(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode references = root.path("references");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || references.isMissingNode() || !references.isArray()) {
      throw new IllegalArgumentException(
          "Dataset reference info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    for (final JsonNode reference : references) {
      DatasetReferenceRecord record = om.convertValue(reference, DatasetReferenceRecord.class);
      record.setDataset(datasetId, urn);
      record.setModifiedTime(eventTime);
      REFERENCE_WRITER.append(record);
    }

    // remove old info then insert new info
    REFERENCE_WRITER.execute(DELETE_DATASET_REFERENCE_BY_URN, new Object[]{urn});
    REFERENCE_WRITER.insert();
  }

  public static Map<String, Object> getDatasetPartitionByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_PARTITION_BY_DATASET_ID, params);
  }

  public static Map<String, Object> getDatasetPartitionByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_PARTITION_BY_URN, params);
  }

  public static void updateDatasetPartition(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode partition = root.path("partition_spec");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || partition.isMissingNode()) {
      throw new IllegalArgumentException(
          "Dataset reference info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    DatasetPartitionRecord record = om.convertValue(partition, DatasetPartitionRecord.class);
    record.setDataset(datasetId, urn);
    record.setModifiedTime(eventTime);
    try {
      Map<String, Object> result = getDatasetPartitionByDatasetUrn(urn);
      String[] columns =
          {"total_partition_level", "partition_spec_text", "has_time_partition", "has_hash_partition",
              "partition_keys", "time_partition_expression", "modified_time"};
      Object[] columnValues =
          new Object[]{record.getTotalPartitionLevel(), record.getPartitionSpecText(), record.getHasTimePartition(),
              record.getHasHashPartition(), StringUtil.toStr(record.getPartitionKeys()),
              record.getTimePartitionExpression(), eventTime};
      String[] conditions = {"dataset_urn"};
      Object[] conditionValues = new Object[]{urn};
      PARTITION_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      PARTITION_WRITER.append(record);
      PARTITION_WRITER.insert();
    }
  }

  public static Map<String, Object> getDatasetSecurityByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_SECURITY_BY_DATASET_ID, params);
  }

  public static Map<String, Object> getDatasetSecurityByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_SECURITY_BY_URN, params);
  }

  public static void updateDatasetSecurity(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode security = root.path("security_spec");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || security.isMissingNode()) {
      throw new IllegalArgumentException(
          "Dataset security info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    DatasetSecurityRecord record = om.convertValue(security, DatasetSecurityRecord.class);
    record.setDataset(datasetId, urn);
    record.setModifiedTime(eventTime);
    try {
      Map<String, Object> result = getDatasetSecurityByDatasetUrn(urn);
      String[] columns =
          {"classification", "record_owner_type", "record_owner", "compliance_type",
              "retention_policy", "geographic_affinity", "modified_time"};
      Object[] columnValues =
          new Object[]{StringUtil.toStr(record.getClassification().toString()), record.getRecordOwnerType(),
              StringUtil.toStr(record.getRecordOwner()), record.getComplianceType(),
              StringUtil.toStr(record.getRetentionPolicy()), StringUtil.toStr(record.getGeographicAffinity()), eventTime};
      String[] conditions = {"dataset_urn"};
      Object[] conditionValues = new Object[]{urn};
      SECURITY_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      SECURITY_WRITER.append(record);
      SECURITY_WRITER.insert();
    }
  }

  public static List<Map<String, Object>> getDatasetOwnerByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_OWNER_BY_DATASET_ID, params);
  }

  public static List<Map<String, Object>> getDatasetOwnerByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_OWNER_BY_URN, params);
  }

  public static void updateDatasetOwner(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode owners = root.path("owners");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || owners.isMissingNode() || !owners.isArray()) {
      throw new IllegalArgumentException(
          "Dataset owner info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    for (final JsonNode owner : owners) {
      DatasetOwnerRecord record = om.convertValue(owner, DatasetOwnerRecord.class);
      record.setDataset(datasetId, urn);
      record.setModifiedTime(eventTime);
      OWNER_WRITER.append(record);
    }

    // remove old info then insert new info
    OWNER_WRITER.execute(DELETE_DATASET_OWNER_BY_URN, new Object[]{urn});
    OWNER_WRITER.insert();
  }
}
