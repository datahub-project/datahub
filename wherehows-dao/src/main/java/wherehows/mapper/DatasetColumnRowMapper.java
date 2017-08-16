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
package wherehows.mapper;

import wherehows.models.DatasetColumn;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.commons.lang3.StringUtils.isNotBlank;


public class DatasetColumnRowMapper implements RowMapper<DatasetColumn> {
    private static final String FIELD_ID_COLUMN = "field_id";
    private static final String SORT_ID_COLUMN = "sort_id";
    private static final String PARENT_SORT_ID_COLUMN = "parent_sort_id";
    private static final String DATA_TYPE_COLUMN = "data_type";
    private static final String FIELD_NAME_COLUMN = "field_name";
    private static final String PARENT_PATH_COLUMN = "parent_path";
    private static final String COMMENT_COLUMN = "comment";
    private static final String PARTITIONED_COLUMN = "partitioned";
    private static final String COMMENT_COUNT_COLUMN = "comment_count";
    private static final String INDEXED_COLUMN = "indexed";
    private static final String NULLABLE_COLUMN = "nullable";
    private static final String DISTRIBUTED_COLUMN = "distributed";

    @Override
    public DatasetColumn mapRow(ResultSet rs, int rowNum) throws SQLException {
        Long id = rs.getLong(FIELD_ID_COLUMN);
        int sortID = rs.getInt(SORT_ID_COLUMN);
        int parentSortID = rs.getInt(PARENT_SORT_ID_COLUMN);
        String dataType = rs.getString(DATA_TYPE_COLUMN);
        String fieldName = rs.getString(FIELD_NAME_COLUMN);
        String parentPath = rs.getString(PARENT_PATH_COLUMN);
        String fullPath = isNotBlank(parentPath) ? parentPath + "." + fieldName : fieldName;
        String comment = rs.getString(COMMENT_COLUMN);
        String strPartitioned = rs.getString(PARTITIONED_COLUMN);
        Long commentCount = rs.getLong(COMMENT_COUNT_COLUMN);
        boolean partitioned = isNotBlank(strPartitioned) && strPartitioned.equalsIgnoreCase("y");
        String strIndexed = rs.getString(INDEXED_COLUMN);
        boolean indexed = isNotBlank(strIndexed) && strIndexed.equalsIgnoreCase("y");
        String strNullable = rs.getString(NULLABLE_COLUMN);
        boolean nullable = isNotBlank(strNullable) && strNullable.equalsIgnoreCase("y");
        String strDistributed = rs.getString(DISTRIBUTED_COLUMN);
        boolean distributed = isNotBlank(strDistributed) && strDistributed.equalsIgnoreCase("y");
        DatasetColumn datasetColumn = new DatasetColumn();
        datasetColumn.id = id;
        datasetColumn.sortID = sortID;
        datasetColumn.parentSortID = parentSortID;
        datasetColumn.fieldName = fieldName;
        datasetColumn.fullFieldPath = fullPath;
        datasetColumn.dataType = dataType;
        datasetColumn.distributed = distributed;
        datasetColumn.partitioned = partitioned;
        datasetColumn.indexed = indexed;
        datasetColumn.nullable = nullable;
        datasetColumn.comment = comment;
        datasetColumn.commentCount = commentCount;
        datasetColumn.treeGridClass = "treegrid-" + Integer.toString(datasetColumn.sortID);
        if (datasetColumn.parentSortID != 0) {
            datasetColumn.treeGridClass += " treegrid-parent-" + Integer.toString(datasetColumn.parentSortID);
        }

        return datasetColumn;
    }
}
