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
package dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;
import wherehows.models.table.Dataset;


public class DatasetWithUserRowMapper implements RowMapper<Dataset> {

  @Override
  public Dataset mapRow(ResultSet rs, int rowNum) throws SQLException {

    Dataset dataset = new Dataset();
    dataset.id = rs.getInt(DatasetRowMapper.DATASET_ID_COLUMN);
    dataset.name = rs.getString(DatasetRowMapper.DATASET_NAME_COLUMN);
    dataset.urn = rs.getString(DatasetRowMapper.DATASET_URN_COLUMN);
    dataset.schema = rs.getString(DatasetRowMapper.DATASET_SCHEMA_COLUMN);
    dataset.source = rs.getString(DatasetRowMapper.DATASET_SOURCE_COLUMN);

    if (StringUtils.isNotBlank(dataset.urn) && dataset.urn.substring(0, 4)
        .equalsIgnoreCase(DatasetRowMapper.HDFS_PREFIX)) {
      dataset.hdfsBrowserLink =
          DatasetRowMapper.HDFS_BROWSER_URL + dataset.urn.substring(DatasetRowMapper.HDFS_URN_PREFIX_LEN);
    }

    String strOwner = rs.getString(DatasetRowMapper.DATASET_OWNER_ID_COLUMN);
    String strOwnerName = rs.getString(DatasetRowMapper.DATASET_OWNER_NAME_COLUMN);
    String strOwnerEmail = rs.getString(DatasetRowMapper.DATASET_OWNER_EMAIL_COLUMN);
    String[] owners = StringUtils.isNotBlank(strOwner) ? strOwner.split(",") : null;
    String[] ownerNames = StringUtils.isNotBlank(strOwnerName) ? strOwnerName.split(",") : null;
    String[] ownerEmail = StringUtils.isNotBlank(strOwnerEmail) ? strOwnerEmail.split(",") : null;

    dataset.owners = utils.Dataset.fillDatasetOwnerList(owners, ownerNames, ownerEmail);

    Time created = rs.getTime(DatasetRowMapper.DATASET_CREATED_TIME_COLUMN);
    Time modified = rs.getTime(DatasetRowMapper.DATASET_MODIFIED_TIME_COLUMN);
    Long sourceModifiedTime = rs.getLong(DatasetRowMapper.DATASET_SOURCE_MODIFIED_TIME_COLUMN);
    Integer schemaHistoryId = rs.getInt(DatasetRowMapper.SCHEMA_HISTORY_ID_COLUMN);

    if (modified != null && sourceModifiedTime != null && sourceModifiedTime > 0) {
      dataset.modified = new java.util.Date(modified.getTime());
      dataset.formatedModified = dataset.modified.toString();
    }
    if (created != null) {
      dataset.created = new java.util.Date(created.getTime());
    } else if (modified != null) {
      dataset.created = new java.util.Date(modified.getTime());
    }

    dataset.hasSchemaHistory = schemaHistoryId != null && schemaHistoryId > 0;

    Long watchId = rs.getLong(DatasetRowMapper.DATASET_WATCH_ID_COLUMN);
    Integer favoriteId = rs.getInt(DatasetRowMapper.FAVORITE_DATASET_ID_COLUMN);

    dataset.isFavorite = favoriteId != null && favoriteId > 0;

    if (watchId != null && watchId > 0) {
      dataset.watchId = watchId;
      dataset.isWatched = true;
    } else {
      dataset.watchId = 0L;
      dataset.isWatched = false;
    }

    return dataset;
  }
}
