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

import models.Flow;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class FlowRowMapper implements RowMapper<Flow>
{
    public static String FLOW_ID_COLUMN = "flow_id";
    public static String JOB_ID_COLUMN = "job_id";
    public static String JOB_NAME_COLUMN = "job_name";
    public static String JOB_PATH_COLUMN = "job_path";
    public static String JOB_TYPE_COLUMN = "job_type";
    public static String FLOW_NAME_COLUMN = "flow_name";
    public static String APP_CODE_COLUMN = "app_code";
    public static String APP_ID_COLUMN = "app_id";
    public static String FLOW_GROUP_COLUMN = "flow_group";
    public static String FLOW_PATH_COLUMN = "flow_path";
    public static String FLOW_LEVEL_COLUMN = "flow_level";

    @Override
    public Flow mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        Long id = rs.getLong(FLOW_ID_COLUMN);
        String name = rs.getString(FLOW_NAME_COLUMN);
        Integer appId = rs.getInt(APP_ID_COLUMN);
        String appCode = rs.getString(APP_CODE_COLUMN);
        String group = rs.getString(FLOW_GROUP_COLUMN);
        String path = rs.getString(FLOW_PATH_COLUMN);
        Integer level = rs.getInt(FLOW_LEVEL_COLUMN);

        Flow flow = new Flow();
        flow.id = id;
        flow.appId = appId;
        flow.name = name;
        flow.appCode = appCode;
        flow.group = group;
        flow.path = path;
        flow.level = level;

        return flow;
    }
}
