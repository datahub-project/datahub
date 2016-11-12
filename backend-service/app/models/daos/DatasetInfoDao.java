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
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import utils.Urn;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import play.Logger;
import utils.JdbcUtil;
import wherehows.common.enums.OwnerType;
import wherehows.common.schemas.DatasetCapacityRecord;
import wherehows.common.schemas.DatasetCaseSensitiveRecord;
import wherehows.common.schemas.DatasetComplianceRecord;
import wherehows.common.schemas.DatasetConstraintRecord;
import wherehows.common.schemas.DeploymentRecord;
import wherehows.common.schemas.DatasetFieldIndexRecord;
import wherehows.common.schemas.DatasetFieldSchemaRecord;
import wherehows.common.schemas.DatasetIndexRecord;
import wherehows.common.schemas.DatasetInventoryItemRecord;
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
  private static final String DATASET_COMPLIANCE_TABLE = "dataset_privacy_compliance";
  private static final String DATASET_SECURITY_TABLE = "dataset_security";
  private static final String DATASET_OWNER_TABLE = "dataset_owner";
  private static final String DATASET_OWNER_UNMATCHED_TABLE = "stg_dataset_owner_unmatched";
  private static final String DATASET_CONSTRAINT_TABLE = "dataset_constraint";
  private static final String DATASET_INDEX_TABLE = "dataset_index";
  private static final String DATASET_SCHEMA_TABLE = "dataset_schema_info";
  private static final String DATASET_FIELD_DETAIL_TABLE = "dict_field_detail";
  private static final String DATASET_INVENTORY_TABLE = "dataset_inventory";
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
  private static final DatabaseWriter COMPLIANCE_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_COMPLIANCE_TABLE);
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
  private static final DatabaseWriter DICT_DATASET_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, "dict_dataset");
  private static final DatabaseWriter SCHEMA_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_SCHEMA_TABLE);
  private static final DatabaseWriter FIELD_DETAIL_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_FIELD_DETAIL_TABLE);
  private static final DatabaseWriter INVENTORY_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_INVENTORY_TABLE);

  public static final String GET_DATASET_DEPLOYMENT_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_DEPLOYMENT_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_DEPLOYMENT_BY_URN =
      "SELECT * FROM " + DATASET_DEPLOYMENT_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_DEPLOYMENT_BY_DATASET_ID =
      "DELETE FROM " + DATASET_DEPLOYMENT_TABLE + " WHERE dataset_id=?";

  public static final String GET_DATASET_CAPACITY_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_CAPACITY_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_CAPACITY_BY_URN =
      "SELECT * FROM " + DATASET_CAPACITY_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_CAPACITY_BY_DATASET_ID =
      "DELETE FROM " + DATASET_CAPACITY_TABLE + " WHERE dataset_id=?";

  public static final String GET_DATASET_TAG_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_TAG_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_TAG_BY_URN =
      "SELECT * FROM " + DATASET_TAG_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_TAG_BY_DATASET_ID = "DELETE FROM " + DATASET_TAG_TABLE + " WHERE dataset_id=?";

  public static final String GET_DATASET_CASE_SENSITIVE_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_CASE_SENSITIVE_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_CASE_SENSITIVE_BY_URN =
      "SELECT * FROM " + DATASET_CASE_SENSITIVE_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String GET_DATASET_REFERENCE_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_REFERENCE_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_REFERENCE_BY_URN =
      "SELECT * FROM " + DATASET_REFERENCE_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_REFERENCE_BY_DATASET_ID =
      "DELETE FROM " + DATASET_REFERENCE_TABLE + " WHERE dataset_id=?";

  public static final String GET_DATASET_PARTITION_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_PARTITION_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_PARTITION_BY_URN =
      "SELECT * FROM " + DATASET_PARTITION_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String GET_DATASET_COMPLIANCE_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_COMPLIANCE_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_COMPLIANCE_BY_URN =
      "SELECT * FROM " + DATASET_COMPLIANCE_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String GET_DATASET_SECURITY_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_SECURITY_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_SECURITY_BY_URN =
      "SELECT * FROM " + DATASET_SECURITY_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String GET_DATASET_OWNER_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_OWNER_TABLE + " WHERE dataset_id = :dataset_id ORDER BY sort_id";

  public static final String GET_DATASET_OWNER_BY_URN =
      "SELECT * FROM " + DATASET_OWNER_TABLE + " WHERE dataset_urn = :dataset_urn ORDER BY sort_id";

  public static final String DELETE_DATASET_OWNER_BY_DATASET_ID =
      "DELETE FROM " + DATASET_OWNER_TABLE + " WHERE dataset_id=?";

  public static final String GET_USER_BY_USER_ID = "SELECT * FROM " + EXTERNAL_USER_TABLE + " WHERE user_id = :user_id";

  public static final String GET_APP_ID_BY_GROUP_ID =
      "SELECT app_id FROM " + EXTERNAL_GROUP_TABLE + " WHERE group_id = :group_id GROUP BY group_id";

  public static final String GET_DATASET_CONSTRAINT_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_CONSTRAINT_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_CONSTRAINT_BY_URN =
      "SELECT * FROM " + DATASET_CONSTRAINT_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_CONSTRAINT_BY_DATASET_ID =
      "DELETE FROM " + DATASET_CONSTRAINT_TABLE + " WHERE dataset_id=?";

  public static final String GET_DATASET_INDEX_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_INDEX_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_INDEX_BY_URN =
      "SELECT * FROM " + DATASET_INDEX_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String DELETE_DATASET_INDEX_BY_DATASET_ID =
      "DELETE FROM " + DATASET_INDEX_TABLE + " WHERE dataset_id=?";

  public static final String GET_DATASET_SCHEMA_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_SCHEMA_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String GET_DATASET_SCHEMA_BY_URN =
      "SELECT * FROM " + DATASET_SCHEMA_TABLE + " WHERE dataset_urn = :dataset_urn";

  public static final String UPDATE_DICT_DATASET_WITH_SCHEMA_CHANGE =
      "UPDATE dict_dataset SET `schema`=?, `schema_type`=?, `fields`=?, `source`=?, `modified_time`=? WHERE `id`=?";

  public static final String GET_DATASET_FIELDS_BY_DATASET_ID =
      "SELECT * FROM " + DATASET_FIELD_DETAIL_TABLE + " WHERE dataset_id = :dataset_id";

  public static final String DELETE_DATASET_FIELDS_BY_DATASET_ID =
      "DELETE FROM " + DATASET_FIELD_DETAIL_TABLE + " WHERE dataset_id=?";

  public static final String UPDATE_DATASET_FIELD_INDEXED_BY_FIELDNAME =
      "UPDATE " + DATASET_FIELD_DETAIL_TABLE + " SET is_indexed=? WHERE dataset_id=? AND field_name=?";

  public static final String UPDATE_DATASET_FIELD_PARTITIONED_BY_FIELDNAME =
      "UPDATE " + DATASET_FIELD_DETAIL_TABLE + " SET is_partitioned=? WHERE dataset_id=? AND field_name=?";

  public static final String GET_DATASET_INVENTORY_ITEMS =
      "SELECT * FROM " + DATASET_INVENTORY_TABLE + " WHERE data_platform = :data_platform AND native_name = :native_name "
          + "AND data_origin = :data_origin ORDER BY event_date DESC LIMIT :limit";

  public static final String INSERT_DATASET_INVENTORY_ITEM =
      PreparedStatementUtil.prepareInsertTemplateWithColumn("REPLACE", DATASET_INVENTORY_TABLE,
          DatasetInventoryItemRecord.getInventoryItemColumns());


  private static Object[] findIdAndUrn(Integer datasetId)
      throws SQLException {
    final String urn = datasetId != null ? DatasetDao.getDatasetById(datasetId).get("urn").toString() : null;
    return new Object[]{datasetId, urn};
  }

  private static Object[] findIdAndUrn(String urn)
      throws SQLException {
    final Integer datasetId = urn != null ? Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString()) : null;
    return new Object[]{datasetId, urn};
  }

  private static Object[] findDataset(JsonNode root) {
    // use dataset id to find dataset first
    final JsonNode idNode = root.path("datasetId");
    if (!idNode.isMissingNode() && !idNode.isNull()) {
      try {
        final Object[] idUrn = findIdAndUrn(idNode.asInt());
        if (idUrn[0] != null && idUrn[1] != null) {
          return idUrn;
        }
      } catch (Exception ex) {
      }
    }

    // use dataset uri to find dataset
    final JsonNode properties = root.path("datasetProperties");
    if (!properties.isMissingNode() && !properties.isNull()) {
      final JsonNode uri = properties.path("uri");
      if (!uri.isMissingNode() && !uri.isNull()) {
        try {
          final Object[] idUrn = findIdAndUrn(uri.asText());
          if (idUrn[0] != null && idUrn[1] != null) {
            return idUrn;
          }
        } catch (Exception ex) {
        }
      }
    }

    // use dataset urn to find dataset
    final JsonNode urnNode = root.path("urn");
    if (!urnNode.isMissingNode() && !urnNode.isNull()) {
      try {
        final Object[] idUrn = findIdAndUrn(urnNode.asText());
        if (idUrn[0] != null && idUrn[1] != null) {
          return idUrn;
        }
      } catch (Exception ex) {
      }
    }

    return new Object[]{null, null};
  }

  public static List<DeploymentRecord> getDatasetDeploymentByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_DEPLOYMENT_BY_DATASET_ID, params);

    List<DeploymentRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DeploymentRecord record = new DeploymentRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static List<DeploymentRecord> getDatasetDeploymentByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_DEPLOYMENT_BY_URN, params);

    List<DeploymentRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DeploymentRecord record = new DeploymentRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static void updateDatasetDeployment(JsonNode root)
      throws Exception {
    final JsonNode deployment = root.path("deploymentInfo");
    if (deployment.isMissingNode() || !deployment.isArray()) {
      throw new IllegalArgumentException(
          "Dataset deployment info update error, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    ObjectMapper om = new ObjectMapper();
    // om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    for (final JsonNode deploymentInfo : deployment) {
      DeploymentRecord record = om.convertValue(deploymentInfo, DeploymentRecord.class);
      record.setDatasetId(datasetId);
      record.setDatasetUrn(urn);
      record.setModifiedTime(System.currentTimeMillis() / 1000);
      DEPLOYMENT_WRITER.append(record);
    }

    // remove old info then insert new info
    DEPLOYMENT_WRITER.execute(DELETE_DATASET_DEPLOYMENT_BY_DATASET_ID, new Object[]{datasetId});
    DEPLOYMENT_WRITER.insert();
  }

  public static List<DatasetCapacityRecord> getDatasetCapacityByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_CAPACITY_BY_DATASET_ID, params);

    List<DatasetCapacityRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetCapacityRecord record = new DatasetCapacityRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static List<DatasetCapacityRecord> getDatasetCapacityByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_CAPACITY_BY_URN, params);

    List<DatasetCapacityRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetCapacityRecord record = new DatasetCapacityRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static void updateDatasetCapacity(JsonNode root)
      throws Exception {
    final JsonNode capacity = root.path("capacity");
    if (capacity.isMissingNode() || !capacity.isArray()) {
      throw new IllegalArgumentException(
          "Dataset capacity info update error, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    ObjectMapper om = new ObjectMapper();

    for (final JsonNode capacityInfo : capacity) {
      DatasetCapacityRecord record = om.convertValue(capacityInfo, DatasetCapacityRecord.class);
      record.setDatasetId(datasetId);
      record.setDatasetUrn(urn);
      record.setModifiedTime(System.currentTimeMillis() / 1000);
      CAPACITY_WRITER.append(record);
    }

    // remove old info then insert new info
    CAPACITY_WRITER.execute(DELETE_DATASET_CAPACITY_BY_DATASET_ID, new Object[]{datasetId});
    CAPACITY_WRITER.insert();
  }

  public static List<DatasetTagRecord> getDatasetTagByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_TAG_BY_DATASET_ID, params);

    List<DatasetTagRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetTagRecord record = new DatasetTagRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static List<DatasetTagRecord> getDatasetTagByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_TAG_BY_URN, params);

    List<DatasetTagRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetTagRecord record = new DatasetTagRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static void updateDatasetTags(JsonNode root)
      throws Exception {
    final JsonNode tags = root.path("tags");
    if (tags.isMissingNode() || !tags.isArray()) {
      throw new IllegalArgumentException(
          "Dataset tag info update error, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    for (final JsonNode tag : tags) {
      DatasetTagRecord record = new DatasetTagRecord();
      record.setTag(tag.asText());
      record.setDatasetId(datasetId);
      record.setDatasetUrn(urn);
      record.setModifiedTime(System.currentTimeMillis() / 1000);
      TAG_WRITER.append(record);
    }

    // remove old info then insert new info
    TAG_WRITER.execute(DELETE_DATASET_TAG_BY_DATASET_ID, new Object[]{datasetId});
    TAG_WRITER.insert();
  }

  public static DatasetCaseSensitiveRecord getDatasetCaseSensitivityByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_CASE_SENSITIVE_BY_DATASET_ID, params);

    DatasetCaseSensitiveRecord record = new DatasetCaseSensitiveRecord();
    record.convertToRecord(result);
    return record;
  }

  public static DatasetCaseSensitiveRecord getDatasetCaseSensitivityByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_CASE_SENSITIVE_BY_URN, params);

    DatasetCaseSensitiveRecord record = new DatasetCaseSensitiveRecord();
    record.convertToRecord(result);
    return record;
  }

  public static void updateDatasetCaseSensitivity(JsonNode root)
      throws Exception {
    final JsonNode properties = root.path("datasetProperties");
    if (properties.isMissingNode() || properties.isNull()) {
      throw new IllegalArgumentException(
          "Dataset properties update fail, missing necessary fields: " + root.toString());
    }
    final JsonNode caseSensitivity = properties.path("caseSensitivity");
    if (caseSensitivity.isMissingNode() || caseSensitivity.isNull()) {
      throw new IllegalArgumentException(
          "Dataset properties update fail, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    ObjectMapper om = new ObjectMapper();

    DatasetCaseSensitiveRecord record = om.convertValue(caseSensitivity, DatasetCaseSensitiveRecord.class);
    record.setDatasetId(datasetId);
    record.setDatasetUrn(urn);
    record.setModifiedTime(System.currentTimeMillis() / 1000);
    try {
      DatasetCaseSensitiveRecord result = getDatasetCaseSensitivityByDatasetId(datasetId);
      String[] columns = record.getDbColumnNames();
      Object[] columnValues = record.getAllValuesToString();
      String[] conditions = {"dataset_id"};
      Object[] conditionValues = new Object[]{datasetId};
      CASE_SENSITIVE_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      CASE_SENSITIVE_WRITER.append(record);
      CASE_SENSITIVE_WRITER.insert();
    }
  }

  public static List<DatasetReferenceRecord> getDatasetReferenceByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_REFERENCE_BY_DATASET_ID, params);

    List<DatasetReferenceRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetReferenceRecord record = new DatasetReferenceRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static List<DatasetReferenceRecord> getDatasetReferenceByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_REFERENCE_BY_URN, params);

    List<DatasetReferenceRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetReferenceRecord record = new DatasetReferenceRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static void updateDatasetReference(JsonNode root)
      throws Exception {
    final JsonNode references = root.path("references");
    if (references.isMissingNode() || !references.isArray()) {
      throw new IllegalArgumentException(
          "Dataset reference info update fail, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    ObjectMapper om = new ObjectMapper();

    for (final JsonNode reference : references) {
      DatasetReferenceRecord record = om.convertValue(reference, DatasetReferenceRecord.class);
      record.setDatasetId(datasetId);
      record.setDatasetUrn(urn);
      record.setModifiedTime(System.currentTimeMillis() / 1000);
      REFERENCE_WRITER.append(record);
    }

    // remove old info then insert new info
    REFERENCE_WRITER.execute(DELETE_DATASET_REFERENCE_BY_DATASET_ID, new Object[]{datasetId});
    REFERENCE_WRITER.insert();
  }

  public static DatasetPartitionRecord getDatasetPartitionByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_PARTITION_BY_DATASET_ID, params);

    DatasetPartitionRecord record = new DatasetPartitionRecord();
    record.convertToRecord(result);
    return record;
  }

  public static DatasetPartitionRecord getDatasetPartitionByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_PARTITION_BY_URN, params);

    DatasetPartitionRecord record = new DatasetPartitionRecord();
    record.convertToRecord(result);
    return record;
  }

  public static void updateDatasetPartition(JsonNode root)
      throws Exception {
    final JsonNode partition = root.path("partitionSpec");
    if (partition.isMissingNode() || partition.isNull()) {
      throw new IllegalArgumentException(
          "Dataset reference info update fail, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    ObjectMapper om = new ObjectMapper();

    DatasetPartitionRecord record = om.convertValue(partition, DatasetPartitionRecord.class);
    record.setDatasetId(datasetId);
    record.setDatasetUrn(urn);
    record.setModifiedTime(System.currentTimeMillis() / 1000);
    try {
      DatasetPartitionRecord result = getDatasetPartitionByDatasetId(datasetId);
      String[] columns = record.getDbColumnNames();
      Object[] columnValues = record.getAllValuesToString();
      String[] conditions = {"dataset_id"};
      Object[] conditionValues = new Object[]{datasetId};
      PARTITION_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      PARTITION_WRITER.append(record);
      PARTITION_WRITER.insert();
    }

    // update dataset field partitioned
    if (record.getPartitionKeys() != null) {
      for (DatasetPartitionKeyRecord partitionKey : record.getPartitionKeys()) {
        List<String> fieldNames = partitionKey.getFieldNames();
        for (String fieldName : fieldNames) {
          FIELD_DETAIL_WRITER.execute(UPDATE_DATASET_FIELD_PARTITIONED_BY_FIELDNAME,
              new Object[]{"Y", datasetId, fieldName});
        }
      }
    }
  }

  public static DatasetComplianceRecord getDatasetComplianceByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_COMPLIANCE_BY_DATASET_ID, params);

    DatasetComplianceRecord record = new DatasetComplianceRecord();
    record.convertToRecord(result);
    return record;
  }

  public static DatasetComplianceRecord getDatasetComplianceByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_COMPLIANCE_BY_URN, params);

    DatasetComplianceRecord record = new DatasetComplianceRecord();
    record.convertToRecord(result);
    return record;
  }

  public static void updateDatasetCompliance(JsonNode root)
      throws Exception {
    final JsonNode security = root.path("privacyCompliancePolicy");
    if (security.isMissingNode() || security.isNull()) {
      throw new IllegalArgumentException(
          "Dataset security info update fail, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    ObjectMapper om = new ObjectMapper();

    DatasetComplianceRecord record = om.convertValue(security, DatasetComplianceRecord.class);
    record.setDatasetId(datasetId);
    record.setDatasetUrn(urn);
    record.setModifiedTime(System.currentTimeMillis() / 1000);
    try {
      DatasetComplianceRecord result = getDatasetComplianceByDatasetId(datasetId);
      String[] columns = record.getDbColumnNames();
      Object[] columnValues = record.getAllValuesToString();
      String[] conditions = {"dataset_id"};
      Object[] conditionValues = new Object[]{datasetId};
      COMPLIANCE_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      COMPLIANCE_WRITER.append(record);
      COMPLIANCE_WRITER.insert();
    }
  }

  public static DatasetSecurityRecord getDatasetSecurityByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_SECURITY_BY_DATASET_ID, params);

    DatasetSecurityRecord record = new DatasetSecurityRecord();
    record.convertToRecord(result);
    return record;
  }

  public static DatasetSecurityRecord getDatasetSecurityByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_SECURITY_BY_URN, params);

    DatasetSecurityRecord record = new DatasetSecurityRecord();
    record.convertToRecord(result);
    return record;
  }

  public static void updateDatasetSecurity(JsonNode root)
      throws Exception {
    final JsonNode security = root.path("securitySpecification");
    if (security.isMissingNode() || security.isNull()) {
      throw new IllegalArgumentException(
          "Dataset security info update fail, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    ObjectMapper om = new ObjectMapper();

    DatasetSecurityRecord record = om.convertValue(security, DatasetSecurityRecord.class);
    record.setDatasetId(datasetId);
    record.setDatasetUrn(urn);
    record.setModifiedTime(System.currentTimeMillis() / 1000);
    try {
      DatasetSecurityRecord result = getDatasetSecurityByDatasetId(datasetId);
      String[] columns = record.getDbColumnNames();
      Object[] columnValues = record.getAllValuesToString();
      String[] conditions = {"dataset_id"};
      Object[] conditionValues = new Object[]{datasetId};
      SECURITY_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      SECURITY_WRITER.append(record);
      SECURITY_WRITER.insert();
    }
  }

  public static List<DatasetOwnerRecord> getDatasetOwnerByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_OWNER_BY_DATASET_ID, params);

    List<DatasetOwnerRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetOwnerRecord record = new DatasetOwnerRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static List<DatasetOwnerRecord> getDatasetOwnerByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_OWNER_BY_URN, params);

    List<DatasetOwnerRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetOwnerRecord record = new DatasetOwnerRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static void updateDatasetOwner(JsonNode root)
      throws Exception {
    final JsonNode owners = root.path("owners");
    if (owners.isMissingNode() || !owners.isArray()) {
      throw new IllegalArgumentException(
          "Dataset owner info update fail, missing necessary fields: " + root.toString());
    }

    final JsonNode ownerSourceNode = root.path("source");
    String ownerSource = null;
    if (!ownerSourceNode.isNull() && !ownerSourceNode.isMissingNode()) {
        ownerSource = ownerSourceNode.asText();
    }

    final Integer datasetId;
    final String urn;
    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      datasetId = 0;
      urn = root.path("datasetProperties").path("uri").asText();
    } else {
      datasetId = (Integer) idUrn[0];
      urn = (String) idUrn[1];
    }

    final JsonNode auditHeader = root.path("auditHeader");
    final Long eventTime = auditHeader != null ? auditHeader.path("time").asLong() / 1000 : null;

    ObjectMapper om = new ObjectMapper();

    List<DatasetOwnerRecord> ownerList = new ArrayList<>();
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

      if (datasetId == 0 || appId == 0) {
        String sql = PreparedStatementUtil.prepareInsertTemplateWithColumn(DATASET_OWNER_UNMATCHED_TABLE,
            record.getDbColumnForUnmatchedOwner());
        OWNER_UNMATCHED_WRITER.execute(sql, record.getValuesForUnmatchedOwner());
      } else {
        ownerList.add(record);
      }
    }

    mergeDatasetOwners(ownerList, datasetId, urn, ownerSource);
  }

  public static void updateKafkaDatasetOwner(String datasetUrn, String owners, String ownerSource, Long sourceUnixTime)
      throws Exception {
    if (datasetUrn == null) {
      return;
    }

    Integer datasetId = 0;
    try {
      datasetId = Integer.parseInt(DatasetDao.getDatasetByUrn(datasetUrn).get("id").toString());
    } catch (Exception e) {
      Logger.error("Exception in updateKafkaDatasetOwner: " + e.getMessage());
    }
    if (datasetId == 0) {
      return;
    }

    List<DatasetOwnerRecord> ownerList = new ArrayList<>();
    if (owners != null) {
      String[] ownerArray = owners.split(",");
      for (String owner : ownerArray) {
        String ownerName = null;
        String namespace = null;
        String ownerIdType = null;
        String isGroup = "N";
        if (owner != null) {
          int lastIndex = owner.lastIndexOf(':');
          if (lastIndex != -1) {
            ownerName = owner.substring(lastIndex + 1);
            namespace = owner.substring(0, lastIndex);
            if (namespace != null && namespace.equalsIgnoreCase("urn:li:griduser")) {
              isGroup = "Y";
              ownerIdType = "GROUP";
            } else {
              ownerIdType = "PERSON";
            }
          }
          DatasetOwnerRecord record = new DatasetOwnerRecord();
          record.setDatasetId(datasetId);
          record.setDatasetUrn(datasetUrn);
          record.setOwnerType("Producer");
          record.setOwner(ownerName);
          record.setOwnerType(ownerIdType);
          record.setIsGroup(isGroup);
          record.setIsActive("Y");
          record.setNamespace(namespace);
          record.setOwnerSource(ownerSource);
          record.setSourceTime(sourceUnixTime);
          record.setCreatedTime(sourceUnixTime);
          record.setModifiedTime(System.currentTimeMillis() / 1000);
          ownerList.add(record);
        }
      }
    }

    mergeDatasetOwners(ownerList, datasetId, datasetUrn, ownerSource);
  }

  private static void mergeDatasetOwners(List<DatasetOwnerRecord> newOwnerList, Integer datasetId, String datasetUrn,
      String source)
      throws Exception {
    List<DatasetOwnerRecord> oldOwnerList = new ArrayList<>();
    try {
      oldOwnerList.addAll(getDatasetOwnerByDatasetUrn(datasetUrn));
    } catch (Exception ex) {
    }

    Integer sortId = 0;
    Map<String, DatasetOwnerRecord> uniqueRecords = new HashMap<>();
    List<DatasetOwnerRecord> combinedList = new ArrayList<>();
    if (newOwnerList != null) {
      for (DatasetOwnerRecord owner : newOwnerList) {
        owner.setSortId(sortId++);
        uniqueRecords.put(owner.getOwner(), owner);
        combinedList.add(owner);
      }
    }

    for (DatasetOwnerRecord owner : oldOwnerList) {
      DatasetOwnerRecord exist = uniqueRecords.get(owner.getOwner());
      if (exist != null) {
        exist.setDbIds(owner.getDbIds());
        exist.setCreatedTime(StringUtil.toLong(owner.getCreatedTime()));

        // take the higher priority owner category
        exist.setOwnerCategory(OwnerType.chooseOwnerType(exist.getOwnerCategory(), owner.getOwnerCategory()));

        // merge owner source as comma separated list
        exist.setOwnerSource(mergeOwnerSource(exist.getOwnerSource(), owner.getOwnerSource()));
        exist.setConfirmedBy(owner.getConfirmedBy());
        exist.setConfirmedOn(owner.getConfirmedOn());
      } else {
        if (!(source != null && source.equalsIgnoreCase(owner.getOwnerSource()))) {
          owner.setSortId(sortId++);
          uniqueRecords.put(owner.getOwner(), owner);
          combinedList.add(owner);
        }
      }
    }

    // remove old info then insert new info
    OWNER_WRITER.execute(DELETE_DATASET_OWNER_BY_DATASET_ID, new Object[]{datasetId});
    for (DatasetOwnerRecord record : combinedList) {
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

  public static List<DatasetConstraintRecord> getDatasetConstraintByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_CONSTRAINT_BY_DATASET_ID, params);

    List<DatasetConstraintRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetConstraintRecord record = new DatasetConstraintRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static List<DatasetConstraintRecord> getDatasetConstraintByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_CONSTRAINT_BY_URN, params);

    List<DatasetConstraintRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetConstraintRecord record = new DatasetConstraintRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static void updateDatasetConstraint(JsonNode root)
      throws Exception {
    final JsonNode constraints = root.path("constraints");
    if (constraints.isMissingNode() || !constraints.isArray()) {
      throw new IllegalArgumentException(
          "Dataset constraints info update fail, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    ObjectMapper om = new ObjectMapper();

    for (final JsonNode constraint : constraints) {
      DatasetConstraintRecord record = om.convertValue(constraint, DatasetConstraintRecord.class);
      record.setDatasetId(datasetId);
      record.setDatasetUrn(urn);
      record.setModifiedTime(System.currentTimeMillis() / 1000);
      CONSTRAINT_WRITER.append(record);
    }

    // remove old info then insert new info
    CONSTRAINT_WRITER.execute(DELETE_DATASET_CONSTRAINT_BY_DATASET_ID, new Object[]{datasetId});
    CONSTRAINT_WRITER.insert();
  }

  public static List<DatasetIndexRecord> getDatasetIndexByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_INDEX_BY_DATASET_ID, params);

    List<DatasetIndexRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetIndexRecord record = new DatasetIndexRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static List<DatasetIndexRecord> getDatasetIndexByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    List<Map<String, Object>> results =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_INDEX_BY_URN, params);

    List<DatasetIndexRecord> records = new ArrayList<>();
    for (Map<String, Object> result : results) {
      DatasetIndexRecord record = new DatasetIndexRecord();
      record.convertToRecord(result);
      records.add(record);
    }
    return records;
  }

  public static void updateDatasetIndex(JsonNode root)
      throws Exception {
    final JsonNode indices = root.path("indices");
    if (indices.isMissingNode() || !indices.isArray()) {
      throw new IllegalArgumentException(
          "Dataset indices info update fail, missing necessary fields: " + root.toString());
    }

    final Object[] idUrn = findDataset(root);
    if (idUrn[0] == null || idUrn[1] == null) {
      throw new IllegalArgumentException("Cannot identify dataset from id/uri/urn: " + root.toString());
    }
    final Integer datasetId = (Integer) idUrn[0];
    final String urn = (String) idUrn[1];

    ObjectMapper om = new ObjectMapper();

    for (final JsonNode index : indices) {
      DatasetIndexRecord record = om.convertValue(index, DatasetIndexRecord.class);
      record.setDatasetId(datasetId);
      record.setDatasetUrn(urn);
      record.setModifiedTime(System.currentTimeMillis() / 1000);
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
    INDEX_WRITER.execute(DELETE_DATASET_INDEX_BY_DATASET_ID, new Object[]{datasetId});
    INDEX_WRITER.insert();
  }

  public static DatasetSchemaInfoRecord getDatasetSchemaByDatasetId(int datasetId)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_id", datasetId);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_SCHEMA_BY_DATASET_ID, params);

    DatasetSchemaInfoRecord record = new DatasetSchemaInfoRecord();
    record.convertToRecord(result);
    return record;
  }

  public static DatasetSchemaInfoRecord getDatasetSchemaByDatasetUrn(String datasetUrn)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_urn", datasetUrn);
    Map<String, Object> result =
        JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_SCHEMA_BY_URN, params);

    DatasetSchemaInfoRecord record = new DatasetSchemaInfoRecord();
    record.convertToRecord(result);
    return record;
  }

  public static void updateDatasetSchema(JsonNode root)
      throws Exception {
    final JsonNode schema = root.path("schema");
    if (schema.isMissingNode() || schema.isNull()) {
      throw new IllegalArgumentException(
          "Dataset schema update fail, missing necessary fields: " + root.toString());
    }

    Integer datasetId = 0;
    String urn = null;
    final Object[] idUrn = findDataset(root);
    if (idUrn[0] != null && idUrn[1] != null) {
      datasetId = (Integer) idUrn[0];
      urn = (String) idUrn[1];
    } else {
      urn = root.path("datasetProperties").path("uri").asText();
    }

    ObjectMapper om = new ObjectMapper();

    DatasetSchemaInfoRecord rec = om.convertValue(schema, DatasetSchemaInfoRecord.class);
    rec.setDatasetId(datasetId);
    rec.setDatasetUrn(urn);
    rec.setModifiedTime(System.currentTimeMillis() / 1000);

    // insert dataset and get ID if necessary
    if (datasetId == 0) {
      DatasetRecord record = new DatasetRecord();
      record.setUrn(urn);
      record.setSourceCreatedTime("" + rec.getCreateTime() / 1000);
      record.setSchema(rec.getOriginalSchema().getText());
      record.setSchemaType(rec.getOriginalSchema().getFormat());
      record.setFields((String) StringUtil.objectToJsonString(rec.getFieldSchema()));
      record.setSource("API");

      Urn urnType = new Urn(urn);
      record.setDatasetType(urnType.datasetType);
      String[] urnPaths = urnType.abstractObjectName.split("/");
      record.setName(urnPaths[urnPaths.length - 1]);

      DICT_DATASET_WRITER.append(record);
      DICT_DATASET_WRITER.close();

      datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());
      rec.setDatasetId(datasetId);
    }
    // if dataset already exist in dict_dataset, update info
    else {
      DICT_DATASET_WRITER.execute(UPDATE_DICT_DATASET_WITH_SCHEMA_CHANGE,
          new Object[]{rec.getOriginalSchema().getText(), rec.getOriginalSchema().getFormat(),
              StringUtil.objectToJsonString(rec.getFieldSchema()), "API", System.currentTimeMillis() / 1000, datasetId});
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
    if (rec.getFieldSchema().size() > 0) {
      String sql = PreparedStatementUtil.prepareInsertTemplateWithColumn(DATASET_FIELD_DETAIL_TABLE,
          rec.getFieldSchema().get(0).getFieldDetailColumns());
      for (DatasetFieldSchemaRecord field : rec.getFieldSchema()) {
        FIELD_DETAIL_WRITER.execute(sql, field.getFieldDetailValues());
      }
    }

    // insert/update new schema info
    try {
      DatasetSchemaInfoRecord result = getDatasetSchemaByDatasetId(datasetId);
      String[] columns = rec.getDbColumnNames();
      Object[] columnValues = rec.getAllValuesToString();
      String[] conditions = {"dataset_id"};
      Object[] conditionValues = new Object[]{datasetId};
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

  public static List<Map<String, Object>> getDatasetInventoryItems(String dataPlatform,
      String nativeName, String dataOrigin, int limit)
      throws DataAccessException {
    Map<String, Object> params = new HashMap<>();
    params.put("data_platform", dataPlatform);
    params.put("native_name", nativeName);
    params.put("data_origin", dataOrigin);
    params.put("limit", limit);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_INVENTORY_ITEMS, params);
  }

  public static void updateDatasetInventory(JsonNode root)
      throws Exception {
    final JsonNode auditHeader = root.path("auditHeader");
    final JsonNode dataPlatform = root.path("dataPlatformUrn");
    final JsonNode datasetList = root.path("datasetList");

    if (auditHeader.isMissingNode() || dataPlatform.isMissingNode() || datasetList.isMissingNode()) {
      throw new IllegalArgumentException(
          "Dataset inventory info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final Long eventTime = auditHeader.get("time").asLong();
    final String eventDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date(eventTime));
    final String dataPlatformUrn = dataPlatform.asText();

    final ObjectMapper om = new ObjectMapper();

    for (JsonNode datasetItem : datasetList) {
      try {
        DatasetInventoryItemRecord item = om.convertValue(datasetItem, DatasetInventoryItemRecord.class);
        item.setDataPlatformUrn(dataPlatformUrn);
        item.setEventDate(eventDate);
        INVENTORY_WRITER.execute(INSERT_DATASET_INVENTORY_ITEM, item.getInventoryItemValues());
      } catch (Exception ex) {
        Logger.debug("Dataset inventory item insertion error. ", ex);
      }
    }
  }
}
