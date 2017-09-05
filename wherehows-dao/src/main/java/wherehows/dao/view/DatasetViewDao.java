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
package wherehows.dao.view;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.models.view.DatasetColumn;

import static org.apache.commons.lang3.StringUtils.isNotBlank;


public class DatasetViewDao extends BaseViewDao {

  private static final Logger log = LoggerFactory.getLogger(DatasetViewDao.class);

  public DatasetViewDao(EntityManagerFactory factory) {
    super(factory);
  }

  private static final String GET_DATASET_COLUMNS_BY_DATASET_ID =
      "select dfd.field_id, dfd.sort_id, dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, "
          + "dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, "
          + "dfd.is_distributed as distributed, c.comment, "
          + "( SELECT count(*) FROM dict_dataset_field_comment ddfc "
          + "WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count "
          + "FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON "
          + "(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN field_comments c ON "
          + "c.id = ddfc.comment_id WHERE dfd.dataset_id = :datasetId ORDER BY dfd.sort_id";

  private static final String GET_DATASET_COLUMN_BY_DATASETID_AND_COLUMNID =
      "SELECT dfd.field_id, dfd.sort_id, dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, "
          + "dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, "
          + "dfd.is_distributed as distributed, c.text as comment, "
          + "( SELECT count(*) FROM dict_dataset_field_comment ddfc "
          + "WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count "
          + "FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON "
          + "(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN comments c ON "
          + "c.id = ddfc.comment_id WHERE dfd.dataset_id = :datasetId AND dfd.field_id = :columnId ORDER BY dfd.sort_id";

  /**
   * Get dataset columns by dataset id
   * @param datasetId int
   * @param datasetUrn String
   * @return List of DatasetColumn
   */
  public List<DatasetColumn> getDatasetColumnsByID(int datasetId, String datasetUrn) {
    Map<String, Object> params = new HashMap<>();
    params.put("datasetId", datasetId);

    List<DatasetColumn> columns = getEntityListBy(GET_DATASET_COLUMNS_BY_DATASET_ID, DatasetColumn.class, params);
    fillInColumnEntity(columns);
    return columns;
  }

  /**
   * Get dataset column by dataset id and column id
   * @param datasetId int
   * @param columnId int
   * @return List of DatasetColumn
   */
  public List<DatasetColumn> getDatasetColumnByID(int datasetId, int columnId) {
    Map<String, Object> params = new HashMap<>();
    params.put("datasetId", datasetId);
    params.put("columnId", columnId);

    List<DatasetColumn> columns = getEntityListBy(GET_DATASET_COLUMN_BY_DATASETID_AND_COLUMNID, DatasetColumn.class, params);
    fillInColumnEntity(columns);
    return columns;
  }

  private void fillInColumnEntity(List<DatasetColumn> columns) {
    for (DatasetColumn column : columns) {
      column.setFullFieldPath(isNotBlank(column.getParentPath()) ? column.getParentPath() + "." + column.getFieldName()
          : column.getFieldName());
      column.setPartitioned("Y".equalsIgnoreCase(column.getPartitionedStr()));
      column.setIndexed("Y".equalsIgnoreCase(column.getIndexedStr()));
      column.setNullable("Y".equalsIgnoreCase(column.getNullableStr()));
      column.setDistributed("Y".equalsIgnoreCase(column.getDistributedStr()));

      String treeGrid = "treegrid-" + column.getSortID();
      if (column.getParentSortID() != 0) {
        treeGrid += " treegrid-parent-" + column.getParentSortID();
      }
      column.setTreeGridClass(treeGrid);
    }
  }
}
