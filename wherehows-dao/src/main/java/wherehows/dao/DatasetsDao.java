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

import java.util.List;
import wherehows.models.DatasetColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;


public class DatasetsDao {

  private static final Logger logger = LoggerFactory.getLogger(DatasetsDao.class);

  private final static String GET_DATASET_COLUMNS_BY_DATASET_ID =
      "select dfd.field_id, dfd.sort_id, " + "dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, "
          + "dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, "
          + "dfd.is_distributed as distributed, c.comment, " + "( SELECT count(*) FROM dict_dataset_field_comment ddfc "
          + "WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count "
          + "FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON "
          + "(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN field_comments c ON "
          + "c.id = ddfc.comment_id WHERE dfd.dataset_id = ? ORDER BY dfd.sort_id";

  private final static String GET_DATASET_COLUMNS_BY_DATASETID_AND_COLUMNID = "SELECT dfd.field_id, " +
      "dfd.sort_id, dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, " +
      "dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, " +
      "dfd.is_distributed as distributed, c.text as comment, " +
      "( SELECT count(*) FROM dict_dataset_field_comment ddfc " +
      "WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count " +
      "FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON " +
      "(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN comments c ON " +
      "c.id = ddfc.comment_id WHERE dfd.dataset_id = ? AND dfd.field_id = ? ORDER BY dfd.sort_id";

  public List<DatasetColumn> getDatasetColumnsByID(JdbcTemplate jdbcTemplate, int datasetId) {
    return jdbcTemplate.query(GET_DATASET_COLUMNS_BY_DATASET_ID, new DatasetColumnRowMapper(), datasetId);
  }

  public List<DatasetColumn> getDatasetColumnByID(JdbcTemplate jdbcTemplate, int datasetId, int columnId)
  {
    return jdbcTemplate.query(GET_DATASET_COLUMNS_BY_DATASETID_AND_COLUMNID,
        new DatasetColumnRowMapper(), datasetId, columnId);
  }
}
