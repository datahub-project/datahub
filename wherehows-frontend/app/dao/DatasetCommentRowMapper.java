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

import models.DatasetComment;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DatasetCommentRowMapper implements RowMapper<DatasetComment>
{
    public static String ID_COLUMN = "id";
    public static String DATASET_ID_COLUMN = "dataset_id";
    public static String TEXT_COLUMN = "text";
    public static String CREATED_TIME_COLUMN = "created";
    public static String MODIFIED_TIME_COLUMN = "modified";
    public static String COMMENT_TYPE_COLUMN = "comment_type";
    public static String DEFAULT_COMMENT_TYPE = "Comment";
    public static String USER_FULL_NAME_COLUMN = "name";
    public static String USER_EMAIL_COLUMN = "email";
    public static String USER_NAME_COLUMN = "username";

    @Override
    public DatasetComment mapRow(ResultSet rs, int rowNum) throws SQLException    {

        int id = rs.getInt(ID_COLUMN);
        int datasetId = rs.getInt(DATASET_ID_COLUMN);
        String text = rs.getString(TEXT_COLUMN);
        String created = rs.getString(CREATED_TIME_COLUMN);
        String modified = rs.getString(MODIFIED_TIME_COLUMN);
        String type = rs.getString(COMMENT_TYPE_COLUMN);
        if (StringUtils.isBlank(type))
        {
            type = DEFAULT_COMMENT_TYPE;
        }
        String authorName = rs.getString(USER_FULL_NAME_COLUMN);
        String authorEmail = rs.getString(USER_EMAIL_COLUMN);
        String userName = rs.getString(USER_NAME_COLUMN);
        DatasetComment datasetComment = new DatasetComment();
        datasetComment.id = id;
        datasetComment.datasetId = datasetId;
        datasetComment.text = text;
        datasetComment.created = created;
        datasetComment.modified = modified;
        datasetComment.type = type;
        datasetComment.authorName = authorName;
        datasetComment.authorEmail = authorEmail;
        datasetComment.authorUserName = userName;

        return datasetComment;
    }
}