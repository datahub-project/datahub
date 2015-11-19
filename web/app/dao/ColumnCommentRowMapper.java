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

import models.ColumnComment;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnCommentRowMapper implements RowMapper<ColumnComment>
{
    public static String ID_COLUMN = "id";
    public static String COMMENT_COLUMN = "comment";
    public static String CREATED_TIME_COLUMN = "created";
    public static String MODIFIED_TIME_COLUMN = "modified";
    public static String USER_FULL_NAME_COLUMN = "name";
    public static String USER_EMAIL_COLUMN = "email";
    public static String USER_NAME_COLUMN = "username";

    @Override
    public ColumnComment mapRow(ResultSet rs, int rowNum) throws SQLException    {

        int id = rs.getInt(ID_COLUMN);
        String text = rs.getString(COMMENT_COLUMN);
        String created = rs.getString(CREATED_TIME_COLUMN);
        String modified = rs.getString(MODIFIED_TIME_COLUMN);
        String authorName = rs.getString(USER_FULL_NAME_COLUMN);
        String authorEmail = rs.getString(USER_EMAIL_COLUMN);
        String userName = rs.getString(USER_NAME_COLUMN);

        ColumnComment columnComment = new ColumnComment();
        columnComment.id = id;
        columnComment.text = text;
        columnComment.created = created;
        columnComment.modified = modified;
        columnComment.authorName = authorName;
        columnComment.authorEmail = authorEmail;
        columnComment.authorUserName = userName;

        return columnComment;
    }
}