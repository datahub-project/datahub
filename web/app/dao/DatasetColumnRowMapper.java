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

import models.DatasetColumn;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DatasetColumnRowMapper implements RowMapper<DatasetColumn>
{
    public static String FIELD_ID_COLUMN = "field_id";
    public static String SORT_ID_COLUMN = "sort_id";
    public static String PARENT_SORT_ID_COLUMN = "parent_sort_id";
    public static String DATA_TYPE_COLUMN = "data_type";
    public static String FIELD_NAME_COLUMN = "field_name";
    public static String COMMENT_COLUMN = "comment";
    public static String PARTITIONED_COLUMN = "partitioned";
    public static String COMMENT_COUNT_COLUMN = "comment_count";
    public static String INDEXED_COLUMN = "indexed";
    public static String NULLABLE_COLUMN = "nullable";
    public static String DISTRIBUTED_COLUMN = "distributed";

    @Override
    public DatasetColumn mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        Long id = rs.getLong(FIELD_ID_COLUMN);
        int sortID = rs.getInt(SORT_ID_COLUMN);
        int parentSortID = rs.getInt(PARENT_SORT_ID_COLUMN);
        String dataType = rs.getString(DATA_TYPE_COLUMN);
        String fieldName = rs.getString(FIELD_NAME_COLUMN);
        String comment = rs.getString(COMMENT_COLUMN);
        String strPartitioned = rs.getString(PARTITIONED_COLUMN);
        Long commentCount = rs.getLong(COMMENT_COUNT_COLUMN);
        boolean partitioned = false;
        if (StringUtils.isNotBlank(strPartitioned) && strPartitioned.equalsIgnoreCase("y"))
        {
            partitioned = true;
        }
        String strIndexed = rs.getString(INDEXED_COLUMN);
        boolean indexed = false;
        if (StringUtils.isNotBlank(strIndexed) && strIndexed.equalsIgnoreCase("y"))
        {
            indexed = true;
        }
        String strNullable = rs.getString(NULLABLE_COLUMN);
        boolean nullable = false;
        if (StringUtils.isNotBlank(strNullable) && strNullable.equalsIgnoreCase("y"))
        {
            nullable = true;
        }
        String strDistributed = rs.getString(DISTRIBUTED_COLUMN);
        boolean distributed = false;
        if (StringUtils.isNotBlank(strDistributed) && strDistributed.equalsIgnoreCase("y"))
        {
            distributed = true;
        }
        DatasetColumn datasetColumn = new DatasetColumn();
        datasetColumn.id = id;
        datasetColumn.sortID = sortID;
        datasetColumn.parentSortID = parentSortID;
        datasetColumn.fieldName = fieldName;
        datasetColumn.dataType = dataType;
        datasetColumn.distributed = distributed;
        datasetColumn.partitioned = partitioned;
        datasetColumn.indexed = indexed;
        datasetColumn.nullable = nullable;
        datasetColumn.comment = comment;
        datasetColumn.commentCount = commentCount;
        datasetColumn.treeGridClass = "treegrid-" + Integer.toString(datasetColumn.sortID);
        if (datasetColumn.parentSortID != 0)
        {
            datasetColumn.treeGridClass += " treegrid-parent-" + Integer.toString(datasetColumn.parentSortID);
        }

        return datasetColumn;
    }
}
