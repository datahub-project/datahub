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
package wherehows.dao.table;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import wherehows.models.table.DatasetCompliance;
import wherehows.models.table.DatasetFieldEntity;
import wherehows.models.view.DatasetOwner;

import static wherehows.util.JsonUtil.*;


public class DatasetsDao {

  private static final Logger logger = LoggerFactory.getLogger(DatasetsDao.class);

  private static final String GET_DATASET_URN_BY_ID = "SELECT urn FROM dict_dataset WHERE id=?";

  private static final String GET_DATASET_ID_BY_URN = "SELECT id FROM dict_dataset WHERE urn=?";

  private final static String UPDATE_DATASET_CONFIRMED_OWNERS =
      "INSERT INTO dataset_owner (dataset_id, owner_id, app_id, namespace, owner_type, is_group, is_active, "
          + "is_deleted, sort_id, created_time, modified_time, wh_etl_exec_id, dataset_urn, owner_sub_type, "
          + "owner_id_type, owner_source, confirmed_by, confirmed_on) "
          + "VALUES(?, ?, ?, ?, ?, ?, ?, 'N', ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), 0, ?, ?, ?, ?, ?, ?) "
          + "ON DUPLICATE KEY UPDATE owner_type = ?, is_group = ?, is_deleted = 'N', "
          + "sort_id = ?, modified_time= UNIX_TIMESTAMP(), owner_sub_type=?, confirmed_by=?, confirmed_on=?";

  private final static String MARK_DATASET_OWNERS_AS_DELETED =
      "UPDATE dataset_owner SET is_deleted = 'Y' WHERE dataset_id = ?";

  private static final String GET_DATASAT_COMPLIANCE_BY_DATASET_ID =
      "SELECT dataset_id, dataset_urn, compliance_purge_type, compliance_entities, confidentiality, "
          + "dataset_classification, field_classification, record_owner_type, retention_policy, "
          + "geographic_affinity, modified_by, modified_time "
          + "FROM dataset_compliance WHERE dataset_id = ?";

  private static final String GET_DATASET_COMPLIANCE_BY_URN =
      "SELECT dataset_id, dataset_urn, compliance_purge_type, compliance_entities, confidentiality, "
          + "dataset_classification, field_classification, record_owner_type, retention_policy, "
          + "geographic_affinity, modified_by, modified_time "
          + "FROM dataset_compliance WHERE dataset_urn = ?";

  private static final String INSERT_DATASET_COMPLIANCE =
      "INSERT INTO dataset_compliance (dataset_id, dataset_urn, compliance_purge_type, compliance_entities, "
          + "confidentiality, dataset_classification, field_classification, record_owner_type, retention_policy, "
          + "geographic_affinity, modified_by, modified_time) "
          + "VALUES (:id, :urn, :compliance_type, :compliance_entities, :confidentiality, :dataset_classification, "
          + ":field_classification, :ownerType, :policy, :geo, :modified_by, :modified_time) "
          + "ON DUPLICATE KEY UPDATE "
          + "compliance_purge_type = :compliance_type, compliance_entities = :compliance_entities, "
          + "confidentiality = :confidentiality, dataset_classification = :dataset_classification, "
          + "field_classification = :field_classification, record_owner_type = :ownerType, retention_policy = :policy, "
          + "geographic_affinity = :geo, modified_by = :modified_by, modified_time = :modified_time";

  /**
   * get WhereHows dataset URN by dataset ID
   * @param jdbcTemplate JdbcTemplate
   * @param datasetId int
   * @return URN String, if not found, return null
   */
  public String getDatasetUrnById(JdbcTemplate jdbcTemplate, int datasetId) {
    try {
      return jdbcTemplate.queryForObject(GET_DATASET_URN_BY_ID, String.class, datasetId);
    } catch (EmptyResultDataAccessException e) {
      logger.error("Can not find URN for dataset id: " + datasetId + " : " + e.getMessage());
    }
    return null;
  }

  /**
   * get dataset URN by dataset ID and do simple validation
   * @param jdbcTemplate JdbcTemplate
   * @param datasetId int
   * @return valid Wherehows URN
   * @throws IllegalArgumentException when dataset URN not found or invalid
   */
  public String validateUrn(JdbcTemplate jdbcTemplate, int datasetId) {
    String urn = getDatasetUrnById(jdbcTemplate, datasetId);
    if (urn == null || urn.length() < 6 || urn.split(":///").length != 2) {
      throw new IllegalArgumentException("Dataset id not found: " + datasetId);
    }
    return urn;
  }

  /**
   * get WhereHows dataset id by dataset URN
   * @param jdbcTemplate JdbcTemplate
   * @param urn String
   * @return dataset ID, if not found, return -1
   */
  public int getDatasetIdByUrn(JdbcTemplate jdbcTemplate, String urn) {
    try {
      return jdbcTemplate.queryForObject(GET_DATASET_ID_BY_URN, Integer.class, urn);
    } catch (EmptyResultDataAccessException e) {
      logger.debug("Can not find dataset id for urn: " + urn + " : " + e.toString());
    }
    return -1;
  }

  public void updateDatasetOwners(JdbcTemplate jdbcTemplate, String user, int datasetId, String datasetUrn,
      List<DatasetOwner> owners) throws Exception {
    // first mark existing owners as deleted, new owners will be updated later
    jdbcTemplate.update(MARK_DATASET_OWNERS_AS_DELETED, datasetId);

    if (owners.size() > 0) {
      updateDatasetOwnerDatabase(jdbcTemplate, datasetId, datasetUrn, owners);
    }
  }

  private void updateDatasetOwnerDatabase(JdbcTemplate jdbcTemplate, int datasetId, String datasetUrn,
      List<DatasetOwner> owners) {
    jdbcTemplate.batchUpdate(UPDATE_DATASET_CONFIRMED_OWNERS, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        DatasetOwner owner = owners.get(i);
        ps.setInt(1, datasetId);
        ps.setString(2, owner.getUserName());
        ps.setInt(3, owner.getIsGroup() ? 301 : 300);
        ps.setString(4, owner.getNamespace());
        ps.setString(5, owner.getType());
        ps.setString(6, owner.getIsGroup() ? "Y" : "N");
        ps.setString(7, owner.getIsActive() != null && owner.getIsActive() ? "Y" : "N");
        ps.setInt(8, owner.getSortId());
        ps.setString(9, datasetUrn);
        ps.setString(10, owner.getSubType());
        ps.setString(11, owner.getIdType());
        ps.setString(12, owner.getSource());
        ps.setString(13, owner.getConfirmedBy());
        ps.setLong(14, StringUtils.isBlank(owner.getConfirmedBy()) ? 0L : Instant.now().getEpochSecond());
        ps.setString(15, owner.getType());
        ps.setString(16, owner.getIsGroup() ? "Y" : "N");
        ps.setInt(17, owner.getSortId());
        ps.setString(18, owner.getSubType());
        ps.setString(19, owner.getConfirmedBy());
        ps.setLong(20, StringUtils.isBlank(owner.getConfirmedBy()) ? 0L : Instant.now().getEpochSecond());
      }

      @Override
      public int getBatchSize() {
        return owners.size();
      }
    });
  }

  public DatasetCompliance getDatasetComplianceInfoByDatasetId(JdbcTemplate jdbcTemplate, int datasetId)
      throws Exception {
    Map<String, Object> result = jdbcTemplate.queryForMap(GET_DATASAT_COMPLIANCE_BY_DATASET_ID, datasetId);

    return mapToDataseCompliance(result);
  }

  public DatasetCompliance getDatasetComplianceInfoByDatasetUrn(JdbcTemplate jdbcTemplate, String datasetUrn)
      throws Exception {
    Map<String, Object> result = jdbcTemplate.queryForMap(GET_DATASET_COMPLIANCE_BY_URN, datasetUrn);

    return mapToDataseCompliance(result);
  }

  private DatasetCompliance mapToDataseCompliance(Map<String, Object> result) throws IOException {
    DatasetCompliance record = new DatasetCompliance();
    record.setDatasetId(((Long) result.get("dataset_id")).intValue());
    record.setDatasetUrn((String) result.get("dataset_urn"));
    record.setComplianceType((String) result.get("compliance_purge_type"));
    record.setComplianceEntities(
        jsonToTypedObject((String) result.get("compliance_entities"), new TypeReference<List<DatasetFieldEntity>>() {
        }));
    record.setConfidentiality((String) result.get("confidentiality"));
    record.setDatasetClassification(
        jsonToTypedObject((String) result.get("dataset_classification"), new TypeReference<Map<String, Object>>() {
        }));
    record.setFieldClassification(
        jsonToTypedObject((String) result.get("field_classification"), new TypeReference<Map<String, String>>() {
        }));
    record.setRecordOwnerType((String) result.get("record_owner_type"));
    record.setRetentionPolicy(
        jsonToTypedObject((String) result.get("retention_policy"), new TypeReference<Map<String, Object>>() {
        }));
    record.setGeographicAffinity(
        jsonToTypedObject((String) result.get("geographic_affinity"), new TypeReference<Map<String, Object>>() {
        }));
    record.setModifiedBy((String) result.get("modified_by"));
    record.setModifiedTime((long) result.get("modified_time") * 1000);
    return record;
  }

  public void updateDatasetComplianceInfo(NamedParameterJdbcTemplate namedParameterJdbcTemplate,
      DatasetCompliance record, String user) throws Exception {
    ObjectMapper om = new ObjectMapper();

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("id", record.getDatasetId());
    parameters.put("urn", record.getDatasetUrn());
    parameters.put("compliance_type", record.getComplianceType());
    parameters.put("compliance_entities", om.writeValueAsString(record.getComplianceEntities()));
    parameters.put("confidentiality", record.getConfidentiality());
    parameters.put("dataset_classification", om.writeValueAsString(record.getDatasetClassification()));
    parameters.put("field_classification", om.writeValueAsString(record.getFieldClassification()));
    parameters.put("ownerType", record.getRecordOwnerType());
    parameters.put("policy", om.writeValueAsString(record.getRetentionPolicy()));
    parameters.put("geo", om.writeValueAsString(record.getGeographicAffinity()));
    parameters.put("modified_by", user);
    parameters.put("modified_time", System.currentTimeMillis() / 1000);
    namedParameterJdbcTemplate.update(INSERT_DATASET_COMPLIANCE, parameters);
  }
}
