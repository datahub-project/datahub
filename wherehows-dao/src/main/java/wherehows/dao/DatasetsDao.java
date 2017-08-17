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
package wherehows.dao;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
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
import wherehows.mapper.DatasetColumnRowMapper;
import wherehows.mapper.DatasetOwnerRowMapper;
import wherehows.models.DatasetColumn;
import wherehows.models.DatasetCompliance;
import wherehows.models.DatasetFieldEntity;
import wherehows.models.DatasetOwner;

import static wherehows.util.JsonUtil.*;


public class DatasetsDao {

  private static final Logger logger = LoggerFactory.getLogger(DatasetsDao.class);

  private static final String GET_DATASET_URN_BY_ID = "SELECT urn FROM dict_dataset WHERE id=?";

  private static final String GET_DATASET_COLUMNS_BY_DATASET_ID =
      "select dfd.field_id, dfd.sort_id, dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, "
          + "dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, "
          + "dfd.is_distributed as distributed, c.comment, " + "( SELECT count(*) FROM dict_dataset_field_comment ddfc "
          + "WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count "
          + "FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON "
          + "(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN field_comments c ON "
          + "c.id = ddfc.comment_id WHERE dfd.dataset_id = ? ORDER BY dfd.sort_id";

  private static final String GET_DATASET_COLUMNS_BY_DATASETID_AND_COLUMNID =
      "SELECT dfd.field_id, dfd.sort_id, dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, "
          + "dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, "
          + "dfd.is_distributed as distributed, c.text as comment, "
          + "( SELECT count(*) FROM dict_dataset_field_comment ddfc "
          + "WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count "
          + "FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON "
          + "(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN comments c ON "
          + "c.id = ddfc.comment_id WHERE dfd.dataset_id = ? AND dfd.field_id = ? ORDER BY dfd.sort_id";

  private final static String GET_DATASET_OWNERS_BY_ID =
      "SELECT o.owner_id, u.display_name, o.sort_id, o.owner_type, o.namespace, o.owner_id_type, o.owner_source, "
          + "o.owner_sub_type, o.confirmed_by, u.email, u.is_active, is_group, o.modified_time "
          + "FROM dataset_owner o "
          + "LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id and u.app_id = 300) "
          + "WHERE o.dataset_id = ? and (o.is_deleted is null OR o.is_deleted != 'Y') ORDER BY o.sort_id";

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


  public String getDatasetUrnById(JdbcTemplate jdbcTemplate, int dataset_id) {
    try {
      return jdbcTemplate.queryForObject(GET_DATASET_URN_BY_ID, String.class, dataset_id);
    } catch (EmptyResultDataAccessException e) {
      logger.error("Can not find URN for dataset id: " + dataset_id + " : " + e.getMessage());
    }
    return null;
  }

  public List<DatasetColumn> getDatasetColumnsByID(JdbcTemplate jdbcTemplate, int datasetId) {
    return jdbcTemplate.query(GET_DATASET_COLUMNS_BY_DATASET_ID, new DatasetColumnRowMapper(), datasetId);
  }

  public List<DatasetColumn> getDatasetColumnByID(JdbcTemplate jdbcTemplate, int datasetId, int columnId) {
    return jdbcTemplate.query(GET_DATASET_COLUMNS_BY_DATASETID_AND_COLUMNID, new DatasetColumnRowMapper(), datasetId,
        columnId);
  }

  public List<DatasetOwner> getDatasetOwnersByID(JdbcTemplate jdbcTemplate, NamedParameterJdbcTemplate namedJdbcTemplate, int datasetId) {
    return jdbcTemplate.query(GET_DATASET_OWNERS_BY_ID, new DatasetOwnerRowMapper(), datasetId);
  }

  public void updateDatasetOwners(JdbcTemplate jdbcTemplate, String user, int datasetId, List<DatasetOwner> owners)
      throws Exception {
    // first mark existing owners as deleted, new owners will be updated later
    jdbcTemplate.update(MARK_DATASET_OWNERS_AS_DELETED, datasetId);

    if (owners.size() > 0) {
      String urn = getDatasetUrnById(jdbcTemplate, datasetId);
      updateDatasetOwnerDatabase(jdbcTemplate, datasetId, urn, owners);
    }
  }

  private void updateDatasetOwnerDatabase(JdbcTemplate jdbcTemplate, int datasetId, String datasetUrn,
      List<DatasetOwner> owners) {
    jdbcTemplate.batchUpdate(UPDATE_DATASET_CONFIRMED_OWNERS, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        DatasetOwner owner = owners.get(i);
        ps.setInt(1, datasetId);
        ps.setString(2, owner.userName);
        ps.setInt(3, owner.isGroup ? 301 : 300);
        ps.setString(4, owner.namespace);
        ps.setString(5, owner.type);
        ps.setString(6, owner.isGroup ? "Y" : "N");
        ps.setString(7, owner.isActive != null && owner.isActive ? "Y" : "N");
        ps.setInt(8, owner.sortId);
        ps.setString(9, datasetUrn);
        ps.setString(10, owner.subType);
        ps.setString(11, owner.idType);
        ps.setString(12, owner.source);
        ps.setString(13, owner.confirmedBy);
        if (StringUtils.isBlank(owner.confirmedBy)) {
          ps.setLong(14, 0L);
        } else {
          ps.setLong(14, Instant.now().getEpochSecond());
        }
        ps.setString(15, owner.type);
        ps.setString(16, owner.isGroup ? "Y" : "N");
        ps.setInt(17, owner.sortId);
        ps.setString(18, owner.subType);
        ps.setString(19, owner.confirmedBy);
        if (StringUtils.isBlank(owner.confirmedBy)) {
          ps.setLong(20, 0L);
        } else {
          ps.setLong(20, Instant.now().getEpochSecond());
        }
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
    record.setModifiedTime((long) result.get("modified_time"));
    return record;
  }

  public DatasetCompliance updateDatasetComplianceInfo(JdbcTemplate jdbcTemplate,
      NamedParameterJdbcTemplate namedParameterJdbcTemplate, int datasetId, JsonNode node, String user)
      throws Exception {

    ObjectMapper om = new ObjectMapper();
    DatasetCompliance record = om.convertValue(node, DatasetCompliance.class);
    if (record.getDatasetId() != null && datasetId != record.getDatasetId()) {
      throw new IllegalArgumentException("Dataset id doesn't match.");
    }
    record.setDatasetId(datasetId);

    String urn = record.getDatasetUrn() != null ? record.getDatasetUrn() : getDatasetUrnById(jdbcTemplate, datasetId);
    record.setDatasetUrn(urn);

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("id", datasetId);
    parameters.put("urn", urn);
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

    return record;
  }
}
