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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.utils.Urn;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import play.Logger;
import utils.JdbcUtil;
import wherehows.common.enums.OwnerType;
import wherehows.common.schemas.DatasetCapacityRecord;
import wherehows.common.schemas.DatasetCaseSensitiveRecord;
import wherehows.common.schemas.DatasetConstraintRecord;
import wherehows.common.schemas.DatasetDeploymentRecord;
import wherehows.common.schemas.DatasetFieldIndexRecord;
import wherehows.common.schemas.DatasetFieldSchemaRecord;
import wherehows.common.schemas.DatasetIndexRecord;
import wherehows.common.schemas.DatasetOwnerRecord;
import wherehows.common.schemas.DatasetPartitionKeyRecord;
import wherehows.common.schemas.DatasetPartitionRecord;
import wherehows.common.schemas.DatasetRecord;
import wherehows.common.schemas.DatasetReferenceRecord;
import wherehows.common.schemas.DatasetSchemaInfoRecord;
import wherehows.common.schemas.DatasetSecurityRecord;
import wherehows.common.schemas.DatasetTagRecord;
import wherehows.common.utils.PreparedStatementUtil;
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
  private static final String DATASET_OWNER_TABLE = "dataset_owner";
  private static final String DATASET_OWNER_UNMATCHED_TABLE = "stg_dataset_owner_unmatched";
  private static final String DATASET_CONSTRAINT_TABLE = "dataset_constraint";
  private static final String DATASET_INDEX_TABLE = "dataset_index";
  private static final String DATASET_SCHEMA_TABLE = "dataset_schema_info";
  private static final String DATASET_FIELD_DETAIL_TABLE = "dict_field_detail";
  private static final String EXTERNAL_USER_TABLE = "dir_external_user_info";
  private static final String EXTERNAL_GROUP_TABLE = "dir_external_group_user_map";

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
  private static final DatabaseWriter OWNER_UNMATCHED_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_OWNER_UNMATCHED_TABLE);
  private static final DatabaseWriter CONSTRAINT_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_CONSTRAINT_TABLE);
  private static final DatabaseWriter INDEX_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_INDEX_TABLE);
  private static final DatabaseWriter SCHEMA_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_SCHEMA_TABLE);
  private static final DatabaseWriter FIELD_DETAIL_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_FIELD_DETAIL_TABLE);

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
      "SELECT * FROM " + DATASET_OWNER_TABLE + " WHERE dataset_id = :dataset_id ORDER BY sort_id";

  public static final String GET_DATASET_OWNER_BY_URN =
      "SELECT * FROM " + DATASET_OWNER_TABLE + " WHERE dataset_urn = :dataset_urn ORDER BY sort_id";

  public static final String DELETE_DATASET_OWNER_BY_URN =
      "DELETE FROM " + DATASET_OWNER_TABLE + " WHERE dataset_urn=?";

  public static final String GET_USER_BY_USER_ID = "SELECT * FROM " + EXTERNAL_USER_TABLE + " WHERE user_id = :user_id";

  public static final String GET_APP_ID_BY_GROUP_ID =
      "SELECT app_id FROM " + EXTERNAL_GROUP_TABLE + " WHERE group_id = :group_id GROUP BY group_id";

  public static final String GET_DATASET_CONSTRAINT_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_CONSTRAINT_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_CONSTRAINT_BY_URN =
      "SELECT * FROM " + DATASET_CONSTRAINT_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_CONSTRAINT_BY_URN =
      "DELETE FROM " + DATASET_CONSTRAINT_TABLE + " WHERE dataset_urn=?";

  public static final String GET_DATASET_INDEX_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_INDEX_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_INDEX_BY_URN =
      "SELECT * FROM " + DATASET_INDEX_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_INDEX_BY_URN =
      "DELETE FROM " + DATASET_INDEX_TABLE + " WHERE dataset_urn=?";

  public static final String GET_DATASET_SCHEMA_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_SCHEMA_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_SCHEMA_BY_URN =
      "SELECT * FROM " + DATASET_SCHEMA_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String GET_DATASET_FIELDS_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_FIELD_DETAIL_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String DELETE_DATASET_FIELDS_BY_DATASET_ID =
      "DELETE FROM " + DATASET_FIELD_DETAIL_TABLE + " WHERE dataset_id=?";

  public static final String UPDATE_DATASET_FIELD_INDEXED_BY_FIELDNAME =
      "UPDATE " + DATASET_FIELD_DETAIL_TABLE + " SET is_indexed=? WHERE dataset_id=? AND field_name=?";

  public static final String UPDATE_DATASET_FIELD_PARTITIONED_BY_FIELDNAME =
      "UPDATE " + DATASET_FIELD_DETAIL_TABLE + " SET is_partitioned=? WHERE dataset_id=? AND field_name=?";

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
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

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
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

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
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

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
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    DatasetCaseSensitiveRecord record = om.convertValue(caseSensitivity, DatasetCaseSensitiveRecord.class);
    record.setDataset(datasetId, urn);
    record.setModifiedTime(eventTime);
    try {
      Map<String, Object> result = getDatasetCaseSensitivityByDatasetUrn(urn);
      String[] columns = record.getDbColumnNames();
      Object[] columnValues = record.getAllValuesToString();
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
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

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
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    DatasetPartitionRecord record = om.convertValue(partition, DatasetPartitionRecord.class);
    record.setDataset(datasetId, urn);
    record.setModifiedTime(eventTime);
    try {
      Map<String, Object> result = getDatasetPartitionByDatasetUrn(urn);
      String[] columns = record.getDbColumnNames();
      Object[] columnValues = record.getAllValuesToString();
      String[] conditions = {"dataset_urn"};
      Object[] conditionValues = new Object[]{urn};
      PARTITION_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      PARTITION_WRITER.append(record);
      PARTITION_WRITER.insert();
    }

    // update dataset field partitioned
    for (DatasetPartitionKeyRecord partitionKey : record.getPartitionKeys()) {
      List<String> fieldNames = partitionKey.getFieldNames();
      for (String fieldName : fieldNames) {
        FIELD_DETAIL_WRITER.execute(UPDATE_DATASET_FIELD_PARTITIONED_BY_FIELDNAME,
            new Object[]{"Y", datasetId, fieldName});
      }
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
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    DatasetSecurityRecord record = om.convertValue(security, DatasetSecurityRecord.class);
    record.setDataset(datasetId, urn);
    record.setModifiedTime(eventTime);
    try {
      Map<String, Object> result = getDatasetSecurityByDatasetUrn(urn);
      String[] columns = record.getDbColumnNames();
      Object[] columnValues = record.getAllValuesToString();
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
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

    Integer datasetId = 0;
    try {
      datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());
    } catch (Exception ex) {
      Logger.debug("Can't find dataset id for " + urn);
    }

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    List<DatasetOwnerRecord> ownerList = new ArrayList<>();
    int sortId = 0;
    for (final JsonNode owner : owners) {
      DatasetOwnerRecord record = om.convertValue(owner, DatasetOwnerRecord.class);
      record.setDatasetId(datasetId);
      record.setDatasetUrn(urn);
      record.setSourceTime(eventTime);
      record.setCreatedTime(eventTime);
      record.setModifiedTime(System.currentTimeMillis() / 1000);

      final String ownerString = record.getOwner();
      int lastIndex = ownerString.lastIndexOf(':');
      if (lastIndex >= 0) {
        record.setOwner(ownerString.substring(lastIndex + 1));
        record.setNamespace(ownerString.substring(0, lastIndex));
      } else {
        record.setNamespace("");
      }
      Map<String, Object> ownerInfo = getOwnerByOwnerId(record.getOwner());
      Integer appId = 0;
      String isActive = "N";
      if (ownerInfo.containsKey("app_id")) {
        appId = StringUtil.toInt(ownerInfo.get("app_id"));
        isActive = appId == 301 ? "Y" : appId == 300 ? (String) ownerInfo.get("is_active") : "N";
      }
      record.setAppId(appId);
      record.setIsActive(isActive);
      String ownerTypeString = record.getOwnerType();
      record.setIsGroup(ownerTypeString != null && ownerTypeString.equalsIgnoreCase("group") ? "Y" : "N");
      sortId++;
      record.setSortId(sortId);

      if (datasetId == 0 || appId == 0) {
        String sql = PreparedStatementUtil.prepareInsertTemplateWithColumn(DATASET_OWNER_UNMATCHED_TABLE,
            record.getDbColumnForUnmatchedOwner());
        OWNER_UNMATCHED_WRITER.execute(sql, record.getValuesForUnmatchedOwner());
      } else {
        ownerList.add(record);
      }
    }

    List<Map<String, Object>> oldOwnerList = getDatasetOwnerByDatasetUrn(urn);
    // merge old owner info into updated owner list
    for (DatasetOwnerRecord rec : ownerList) {
      for (Map<String, Object> old : oldOwnerList) {
        if (rec.getDatasetId().equals(StringUtil.toInt(old.get("dataset_id"))) && rec.getOwner()
            .equals(old.get("owner_id")) && rec.getAppId().equals(StringUtil.toInt(old.get("app_id")))) {
          rec.setDbIds((String) old.get("db_ids"));
          rec.setCreatedTime(StringUtil.toLong(old.get("created_time")));

          // take the higher priority owner category
          rec.setOwnerCategory(
              OwnerType.chooseOwnerType(rec.getOwnerCategory(), (String) old.get("owner_type")));

          // merge owner source as comma separated list
          rec.setOwnerSource(mergeOwnerSource(rec.getOwnerSource(), (String) old.get("owner_source")));

          // remove from owner source?
        }
      }
    }

    // remove old info then insert new info
    OWNER_WRITER.execute(DELETE_DATASET_OWNER_BY_URN, new Object[]{urn});
    for (DatasetOwnerRecord record : ownerList) {
      OWNER_WRITER.append(record);
    }
    OWNER_WRITER.insert();
  }

  public static String mergeOwnerSource(String source, String oldSourceList) {
    if (source == null) {
      return oldSourceList;
    }
    if (oldSourceList == null) {
      return source;
    }
    if (oldSourceList.contains(source)) {
      return oldSourceList;
    }
    return source + "," + oldSourceList;
  }

  public static Map<String, Object> getUserByUserId(String userId)
      throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put("user_id", userId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_USER_BY_USER_ID, params);
  }

  public static Map<String, Object> getGroupMapByGroupId(String groupId)
      throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put("group_id", groupId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_APP_ID_BY_GROUP_ID, params);
  }

  public static Map<String, Object> getOwnerByOwnerId(String ownerId)
      throws SQLException {
    try {
      return getUserByUserId(ownerId);
    } catch (EmptyResultDataAccessException ex) {
    }

    try {
      return getGroupMapByGroupId(ownerId);
    } catch (EmptyResultDataAccessException ex) {
    }
    Logger.debug("Can't find owner (user or group): " + ownerId);
    return new HashMap<>();
  }

  public static List<Map<String, Object>> getDatasetConstraintByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_CONSTRAINT_BY_DATASET_ID, params);
  }

  public static List<Map<String, Object>> getDatasetConstraintByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_CONSTRAINT_BY_URN, params);
  }

  public static void updateDatasetConstraint(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode constraints = root.path("constraints");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || constraints.isMissingNode()
        || !constraints.isArray()) {
      throw new IllegalArgumentException(
          "Dataset constraints info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    for (final JsonNode constraint : constraints) {
      DatasetConstraintRecord record = om.convertValue(constraint, DatasetConstraintRecord.class);
      record.setDataset(datasetId, urn);
      record.setModifiedTime(eventTime);
      CONSTRAINT_WRITER.append(record);
    }

    // remove old info then insert new info
    CONSTRAINT_WRITER.execute(DELETE_DATASET_CONSTRAINT_BY_URN, new Object[]{urn});
    CONSTRAINT_WRITER.insert();
  }

  public static List<Map<String, Object>> getDatasetIndexByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_INDEX_BY_DATASET_ID, params);
  }

  public static List<Map<String, Object>> getDatasetIndexByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_INDEX_BY_URN, params);
  }

  public static void updateDatasetIndex(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode indices = root.path("indices");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || indices.isMissingNode() || !indices.isArray()) {
      throw new IllegalArgumentException(
          "Dataset indices info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    for (final JsonNode index : indices) {
      DatasetIndexRecord record = om.convertValue(index, DatasetIndexRecord.class);
      record.setDataset(datasetId, urn);
      record.setModifiedTime(eventTime);
      INDEX_WRITER.append(record);

      // update dataset field indexed
      for (DatasetFieldIndexRecord rec : record.getIndexedFields()) {
        String fieldPath = rec.getFieldPath();
        int lastIndex = fieldPath.lastIndexOf('.'); // if not found, index = -1
        String fieldName = fieldPath.substring(lastIndex + 1);
        FIELD_DETAIL_WRITER.execute(UPDATE_DATASET_FIELD_INDEXED_BY_FIELDNAME, new Object[]{"Y", datasetId, fieldName});
      }
    }

    // remove old info then insert new info
    INDEX_WRITER.execute(DELETE_DATASET_INDEX_BY_URN, new Object[]{urn});
    INDEX_WRITER.insert();
  }

  public static Map<String, Object> getDatasetSchemaByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_SCHEMA_BY_DATASET_ID, params);
  }

  public static Map<String, Object> getDatasetSchemaByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_SCHEMA_BY_URN, params);
  }

  public static void updateDatasetSchema(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("audit_header");
    final JsonNode urnNode = root.path("urn");
    final JsonNode schemas = root.path("schemas");

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || schemas.isMissingNode()) {
      throw new IllegalArgumentException(
          "Dataset schemas update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong() / 1000; // millisecond to second

    Integer datasetId = 0;
    try {
      datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());
    } catch (Exception ex) {
    }

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    DatasetSchemaInfoRecord rec = om.convertValue(schemas, DatasetSchemaInfoRecord.class);
    rec.setDataset(datasetId, urn);
    rec.setModifiedTime(eventTime);

    // insert dataset and get ID if necessary
    if (datasetId == 0) {
      DatasetRecord record = new DatasetRecord();
      record.setUrn(urn);
      record.setSourceCreatedTime(rec.getCreateTime().toString());
      record.setSchema(rec.getOriginalSchema());
      record.setSchemaType(rec.getFormat());
      record.setFields(rec.getFieldSchema().toString());
      record.setSource("API");

      Urn urnType = new Urn(urn);
      record.setDatasetType(urnType.datasetType);
      String[] urnPaths = urnType.abstractObjectName.split("/");
      record.setName(urnPaths[urnPaths.length - 1]);

      DatabaseWriter dw = new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, "dict_dataset");
      dw.append(record);
      dw.close();

      datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());
    }

    // get old dataset fields info
    List<Map<String, Object>> oldInfo;
    try {
      oldInfo = getDatasetFieldsByDatasetId(datasetId);
    } catch (DataAccessException ex) {
      oldInfo = new ArrayList<>();
    }

    for (DatasetFieldSchemaRecord field : rec.getFieldSchema()) {
      field.setDatasetId(datasetId);
      String fieldPath = field.getFieldPath();
      int lastIndex = fieldPath.lastIndexOf('.'); // if not found, index = -1
      field.setFieldName(fieldPath.substring(lastIndex + 1));
      field.setParentPath(lastIndex > 0 ? fieldPath.substring(0, lastIndex) : "");

      // merge old info into updated list
      for (Map<String, Object> old : oldInfo) {
        if (datasetId.equals(StringUtil.toInt(old.get("dataset_id"))) && field.getFieldName()
            .equals(old.get("field_name")) && field.getParentPath().equals(old.get("parent_path"))) {
          field.setNamespace((String) old.get("namespace"));
          field.setCommentIds((String) old.get("comment_ids"));
          field.setDefaultCommentId(StringUtil.toInt(old.get("default_comment_id")));
          field.setPartitioned("Y".equals(old.get("is_partitioned")));
          field.setIndexed("Y".equals(old.get("is_indexed")));
        }
      }
    }
    // remove old info then insert new info
    FIELD_DETAIL_WRITER.execute(DELETE_DATASET_FIELDS_BY_DATASET_ID, new Object[]{datasetId});
    String sql = PreparedStatementUtil.prepareInsertTemplateWithColumn(DATASET_FIELD_DETAIL_TABLE,
        rec.getFieldSchema().get(0).getFieldDetailColumns());
    for (DatasetFieldSchemaRecord field : rec.getFieldSchema()) {
      FIELD_DETAIL_WRITER.execute(sql, field.getFieldDetailValues());
    }

    try {
      Map<String, Object> result = getDatasetSchemaByDatasetUrn(urn);
      String[] columns = rec.getDbColumnNames();
      Object[] columnValues = rec.getAllValuesToString();
      String[] conditions = {"dataset_urn"};
      Object[] conditionValues = new Object[]{urn};
      SCHEMA_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      SCHEMA_WRITER.append(rec);
      SCHEMA_WRITER.insert();
    }
  }

  public static List<Map<String, Object>> getDatasetFieldsByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_FIELDS_BY_DATASET_ID, params);
  }
}
