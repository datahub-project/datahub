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

import models.User;
import models.UserSetting;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class UserRowMapper implements RowMapper<User>
{
    public static String USER_ID_COLUMN = "id";
    public static String USER_FULL_NAME_COLUMN = "name";
    public static String USER_EMAIL_COLUMN = "email";
    public static String USER_NAME_COLUMN = "username";
    public static String USER_DEPARTMENT_NUMBER_COLUMN = "department_number";
    public static String USER_SETTING_DETAIL_DEFAULT_VIEW_COLUMN = "detail_default_view";
    public static String USER_SETTING_DEFAULT_WATCH_COLUMN = "default_watch";
    public static String CATEGORY_COLUMN = "category";

    @Override
    public User mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        int id = rs.getInt(USER_ID_COLUMN);
        String name = rs.getString(USER_FULL_NAME_COLUMN);
        String email = rs.getString(USER_EMAIL_COLUMN);
        String username = rs.getString(USER_NAME_COLUMN);
        Integer departmentNum = rs.getInt(USER_DEPARTMENT_NUMBER_COLUMN);
        String defaultView = rs.getString(USER_SETTING_DETAIL_DEFAULT_VIEW_COLUMN);
        String defaultWatch = rs.getString(USER_SETTING_DEFAULT_WATCH_COLUMN);

        User user = new User();
        UserSetting userSetting = new UserSetting();
        userSetting.detailDefaultView = defaultView;
        userSetting.defaultWatch = defaultWatch;
        user.id = id;
        user.name = name;
        user.email = email;
        user.userName = username;
        user.departmentNum = departmentNum;
        user.userSetting = userSetting;

        return user;
    }
}