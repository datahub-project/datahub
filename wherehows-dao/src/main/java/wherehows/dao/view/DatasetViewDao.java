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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import wherehows.dao.table.DictDatasetDao;
import wherehows.models.PagedCollection;
import wherehows.models.table.DictDataset;
import wherehows.models.view.DatasetColumn;
import wherehows.models.view.DatasetSchema;
import wherehows.models.view.DatasetView;

import static org.apache.commons.lang3.StringUtils.*;
import static wherehows.util.UrnUtil.*;


@Slf4j
public class DatasetViewDao extends BaseViewDao {

  private final DictDatasetDao _dictDatasetDao;

  public DatasetViewDao(@Nonnull EntityManagerFactory factory) {
    super(factory);
    _dictDatasetDao = new DictDatasetDao(factory);
  }

  private static final String GET_DATASET_COLUMNS_BY_DATASET_ID =
      "SELECT dfd.field_id, dfd.sort_id, dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, "
          + "dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, "
          + "dfd.is_distributed as distributed, c.comment, "
          + "( SELECT count(*) FROM dict_dataset_field_comment ddfc "
          + "WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count "
          + "FROM dict_field_detail dfd "
          + "LEFT JOIN dict_dataset_field_comment ddfc ON ddfc.field_id = dfd.field_id "
          + "  AND ddfc.comment_id = (select max(comment_id) from dict_dataset_field_comment "
          + "  where field_id = dfd.field_id and is_default = true) "
          + "LEFT JOIN field_comments c ON c.id = ddfc.comment_id "
          + "WHERE dfd.dataset_id = :datasetId ORDER BY dfd.sort_id";

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
   * Get dataset view from dict dataset.
   * @param datasetId int
   * @param datasetUrn String
   * @return DatasetView
   */
  public DatasetView getDatasetView(int datasetId, @Nonnull String datasetUrn) {
    return fillDatasetViewFromDictDataset(_dictDatasetDao.findById(datasetId));
  }

  @Nonnull
  public DatasetView getDatasetView(@Nonnull String datasetUrn) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Convert DictDataset to DatasetView
   * @param ds DictDataset
   * @return DatasetView
   */
  public DatasetView fillDatasetViewFromDictDataset(DictDataset ds) {
    String[] urnParts = splitWhUrn(ds.getUrn());

    DatasetView view = new DatasetView();
    view.setPlatform(urnParts[0]);
    view.setNativeName(urnParts[1]);
    view.setUri(ds.getUrn());
    view.setNativeType(ds.getStorageType());
    view.setProperties(ds.getProperties());
    view.setRemoved(ds.getIsActive() != null && !ds.getIsActive());
    view.setDeprecated(ds.getIsDeprecated());
    view.setCreatedTime(1000L * ds.getCreatedTime());
    view.setModifiedTime(1000L * ds.getModifiedTime());

    return view;
  }

  @Nonnull
  public PagedCollection<DatasetView> listDatasets(@Nullable String platform, @Nullable String origin,
      @Nonnull String prefix, int start, int count) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  public List<String> listSegments(@Nonnull String platform, @Nullable String origin, @Nonnull String prefix) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  public List<String> listFullNames(@Nonnull String platform, @Nullable String origin, @Nonnull String prefix) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Get dataset columns by dataset id
   * @param datasetId int
   * @param datasetUrn String
   * @return List of DatasetColumn
   */
  public DatasetSchema getDatasetColumnsByID(int datasetId, @Nonnull String datasetUrn) {
    Map<String, Object> params = new HashMap<>();
    params.put("datasetId", datasetId);

    List<DatasetColumn> columns = getEntityListBy(GET_DATASET_COLUMNS_BY_DATASET_ID, DatasetColumn.class, params);
    fillInColumnEntity(columns);

    DatasetSchema schema = new DatasetSchema();
    schema.setSchemaless(false);
    schema.setColumns(columns);

    return schema;
  }

  /**
   * Get dataset schema by dataset urn
   */
  @Nonnull
  public DatasetSchema getDatasetSchema(@Nonnull String datasetUrn) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
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

    List<DatasetColumn> columns =
        getEntityListBy(GET_DATASET_COLUMN_BY_DATASETID_AND_COLUMNID, DatasetColumn.class, params);
    fillInColumnEntity(columns);
    return columns;
  }

  private void fillInColumnEntity(@Nonnull List<DatasetColumn> columns) {
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
