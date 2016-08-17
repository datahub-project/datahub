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
import wherehows.common.schemas.DatasetConstraintRecord;
import wherehows.common.schemas.DatasetFieldSchemaRecord;
import wherehows.common.schemas.DatasetIndexRecord;
import wherehows.common.schemas.DatasetSchemaInfoRecord;
import wherehows.common.utils.StringUtil;
import wherehows.common.writers.DatabaseWriter;


public class DatasetSchemaDao {

  private static final String DATASET_CONSTRAINT_TABLE = "dataset_constraint";
  private static final String DATASET_INDEX_TABLE = "dataset_index";
  private static final String DATASET_SCHEMA_TABLE = "dataset_schema_info";
  private static final String DATASET_FIELD_SCHEMA_TABLE = "dataset_field_schema";

  private static final DatabaseWriter CONSTRAINT_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_CONSTRAINT_TABLE);
  private static final DatabaseWriter INDEX_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_INDEX_TABLE);
  private static final DatabaseWriter SCHEMA_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_SCHEMA_TABLE);
  private static final DatabaseWriter FIELD_SCHEMA_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, DATASET_FIELD_SCHEMA_TABLE);

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

  public static final String DELETE_DATASET_FIELD_SCHEMA_BY_DATASET_ID =
      "DELETE FROM " + DATASET_FIELD_SCHEMA_TABLE + " WHERE dataset_id=?";

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

    if (auditHeader.isMissingNode() || urnNode.isMissingNode() || constraints.isMissingNode() || !constraints.isArray()) {
      throw new IllegalArgumentException(
          "Dataset constraints info update fail, " + "Json missing necessary fields: " + root.toString());
    }

    final String urn = urnNode.asText();
    final Long eventTime = auditHeader.path("time").asLong();

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
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    for (final JsonNode index : indices) {
      DatasetIndexRecord record = om.convertValue(index, DatasetIndexRecord.class);
      record.setDataset(datasetId, urn);
      record.setModifiedTime(eventTime);
      INDEX_WRITER.append(record);
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
    final Long eventTime = auditHeader.path("time").asLong();

    final Integer datasetId = Integer.valueOf(DatasetDao.getDatasetByUrn(urn).get("id").toString());

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    DatasetSchemaInfoRecord rec = om.convertValue(schemas, DatasetSchemaInfoRecord.class);
    rec.setDataset(datasetId, urn);
    rec.setModifiedTime(eventTime);
    for (DatasetFieldSchemaRecord field : rec.getFieldSchema()) {
      field.setDatasetId(datasetId);
      FIELD_SCHEMA_WRITER.append(field);
    }
    // remove old info then insert new info
    FIELD_SCHEMA_WRITER.execute(DELETE_DATASET_FIELD_SCHEMA_BY_DATASET_ID, new Object[]{datasetId});
    FIELD_SCHEMA_WRITER.insert();

    try {
      Map<String, Object> result = getDatasetSchemaByDatasetUrn(urn);
      String[] columns = {"is_latest_revision", "create_time", "revision", "version", "name", "description", "format",
          "original_schema", "original_schema_checksum", "key_schema_type", "key_schema_format", "key_schema",
          "is_field_name_case_sensitive", "field_schema", "change_data_capture_fields", "audit_fields", "modified_time"};
      Object[] columnValues =
          new Object[]{rec.getIsLatestRevision(), rec.getCreateTime(), rec.getRevision(), rec.getVersion(),
              rec.getName(), rec.getDescription(), rec.getFormat(), rec.getOriginalSchema(),
              StringUtil.toStr(rec.getOriginalSchemaChecksum()), rec.getKeySchemaType(), rec.getKeySchemaFormat(),
              rec.getKeySchema(), rec.getIsFieldNameCaseSensitive(), StringUtil.toStr(rec.getFieldSchema()),
              StringUtil.toStr(rec.getChangeDataCaptureFields()), StringUtil.toStr(rec.getAuditFields()), eventTime};
      String[] conditions = {"dataset_urn"};
      Object[] conditionValues = new Object[]{urn};
      SCHEMA_WRITER.update(columns, columnValues, conditions, conditionValues);
    } catch (EmptyResultDataAccessException ex) {
      SCHEMA_WRITER.append(rec);
      SCHEMA_WRITER.insert();
    }
  }
}
