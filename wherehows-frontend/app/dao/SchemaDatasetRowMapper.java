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

import wherehows.models.table.SchemaDataset;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;


public class SchemaDatasetRowMapper implements RowMapper<SchemaDataset> {
  public static String DATASET_ID_COLUMN = "dataset_id";
  public static String URN_COLUMN = "urn";
  public static String MODIFIED_DATE_COLUMN = "modified_date";
  public static String DATASET_LINK_PREFIX = "/#/datasets/";

  @Override
  public SchemaDataset mapRow(ResultSet rs, int rowNum) throws SQLException {
    String urn = rs.getString(URN_COLUMN);
    String name = "";
    if (StringUtils.isNotBlank(urn)) {
      int index = urn.lastIndexOf('/');
      if (index != -1) {
        name = urn.substring(index + 1);
      }
    }

    SchemaDataset schemaDataset = new SchemaDataset();
    schemaDataset.setDataset_id(rs.getInt(DATASET_ID_COLUMN));
    schemaDataset.setUrn(urn);
    schemaDataset.setLastModified(rs.getString(MODIFIED_DATE_COLUMN));
    schemaDataset.setName(name);
    schemaDataset.setDatasetLink(DATASET_LINK_PREFIX + schemaDataset.getDataset_id());
    return schemaDataset;
  }
}