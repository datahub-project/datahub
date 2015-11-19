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
import models.DatasetColumnComment;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DatasetColumnCommentRowMapper implements RowMapper<DatasetColumnComment>
{
    public static String ID_COLUMN = "id";
    public static String AUTHOR_COLUMN = "author";
    public static String TEXT_COLUMN = "text";
    public static String CREATED_TIME_COLUMN = "created";
    public static String MODIFIED_TIME_COLUMN = "modified";
    public static String FIELD_ID_COLUMN = "field_id";
    public static String IS_DEFAULT_COLUMN = "is_default";

    @Override
    public DatasetColumnComment mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        Long id = rs.getLong(ID_COLUMN);
        String author = rs.getString(AUTHOR_COLUMN);
        String text = rs.getString(TEXT_COLUMN);
        String created = rs.getString(CREATED_TIME_COLUMN);
        String modified = rs.getString(MODIFIED_TIME_COLUMN);
        Long columnId = rs.getLong(FIELD_ID_COLUMN);
        String strIsDefault = rs.getString(IS_DEFAULT_COLUMN);
        boolean isDefault = false;
        if (StringUtils.isNotBlank(strIsDefault) && strIsDefault == "Y")
        {
            isDefault = true;
        }
        DatasetColumnComment datasetColumnComment = new DatasetColumnComment();

        datasetColumnComment.id = id;
        datasetColumnComment.author = author;
        datasetColumnComment.text = text;
        datasetColumnComment.created = created;
        datasetColumnComment.modified = modified;
        datasetColumnComment.columnId = columnId;
        datasetColumnComment.isDefault = isDefault;

        return datasetColumnComment;
    }
}
