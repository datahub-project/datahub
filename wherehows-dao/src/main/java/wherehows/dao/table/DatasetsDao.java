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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import wherehows.models.view.DatasetOwner;


@Slf4j
public class DatasetsDao {

  private static final String GET_DATASET_URN_BY_ID = "SELECT urn FROM dict_dataset WHERE id=?";

  private static final String GET_DATASET_ID_BY_URN = "SELECT id FROM dict_dataset WHERE urn=?";

  private final static String GET_DATASET_OWNER_TYPES =
      "SELECT DISTINCT owner_type FROM dataset_owner WHERE owner_type is not null";

  private final static String UPDATE_DATASET_CONFIRMED_OWNERS =
      "INSERT INTO dataset_owner (dataset_id, owner_id, app_id, namespace, owner_type, is_group, is_active, "
          + "is_deleted, sort_id, created_time, modified_time, wh_etl_exec_id, dataset_urn, owner_sub_type, "
          + "owner_id_type, owner_source, confirmed_by, confirmed_on) "
          + "VALUES(?, ?, ?, ?, ?, ?, ?, 'N', ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), 0, ?, ?, ?, ?, ?, ?) "
          + "ON DUPLICATE KEY UPDATE owner_type = ?, is_group = ?, is_deleted = 'N', "
          + "sort_id = ?, modified_time= UNIX_TIMESTAMP(), owner_sub_type=?, confirmed_by=?, confirmed_on=?";

  private final static String MARK_DATASET_OWNERS_AS_DELETED =
      "UPDATE dataset_owner SET is_deleted = 'Y' WHERE dataset_id = ?";

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
      log.error("Can not find URN for dataset id: " + datasetId + " : " + e.getMessage());
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
      log.debug("Can not find dataset id for urn: " + urn + " : " + e.toString());
    }
    return -1;
  }

  /**
   * @return list of dataset owner types
   */
  public List<String> getDatasetOwnerTypes(JdbcTemplate jdbcTemplate) {
    return jdbcTemplate.queryForList(GET_DATASET_OWNER_TYPES, String.class);
  }

  /**
   * Update dataset owners, set removed owners as deleted, and update existing owner information.
   * @param jdbcTemplate JdbcTemplate
   * @param user String
   * @param datasetId int
   * @param datasetUrn String
   * @param owners List<DatasetOwner>
   * @throws Exception
   */
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
}
