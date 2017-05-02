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

import models.Project;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.sql.Date;

public class ProjectRowMapper implements RowMapper<Project>
{
    public static String APP_ID_COLUMN = "app_id";
    public static String APP_CODE_COLUMN = "app_code";
    public static String PROJECT_NAME_COLUMN = "project_name";
    public static String FLOW_GROUP_COLUMN = "flow_group";

    @Override
    public Project mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        String name = rs.getString(PROJECT_NAME_COLUMN);
        String flowGroup = rs.getString(FLOW_GROUP_COLUMN);
        Integer appID = rs.getInt(APP_ID_COLUMN);
        String appCode = rs.getString(APP_CODE_COLUMN);
        Project project = new Project();
        project.appId = appID;
        project.appCode = appCode;
        project.name = name;
        project.flowGroup = flowGroup;
        return project;
    }
}
