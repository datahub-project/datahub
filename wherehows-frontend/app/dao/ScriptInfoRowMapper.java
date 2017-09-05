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

import wherehows.models.table.ScriptInfo;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ScriptInfoRowMapper implements RowMapper<ScriptInfo>
{
    public static String APPLICATION_ID_COLUMN = "app_id";
    public static String JOB_ID_COLUMN = "job_id";
    public static String SCRIPT_URL_COLUMN = "script_url";
    public static String SCRIPT_PATH_COLUMN = "script_path";
    public static String SCRIPT_NAME_COLUMN = "script_name";
    public static String SCRIPT_TYPE_COLUMN = "script_type";
    public static String CHAIN_NAME_COLUMN = "chain_name";
    public static String JOB_NAME_COLUMN = "job_name";
    public static String COMMITTER_NAMES_COLUMN = "committer_names";
    public static String COMMITTER_EMAILS_COLUMN = "committer_emails";

    @Override
    public ScriptInfo mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        int applicationID = rs.getInt(APPLICATION_ID_COLUMN);
        int jobID = rs.getInt(JOB_ID_COLUMN);
        String scriptUrl = rs.getString(SCRIPT_URL_COLUMN);
        String scriptPath = rs.getString(SCRIPT_PATH_COLUMN);
        String scriptType = rs.getString(SCRIPT_TYPE_COLUMN);
        String chainName = rs.getString(CHAIN_NAME_COLUMN);
        String jobName = rs.getString(JOB_NAME_COLUMN);
        String scriptName = rs.getString(SCRIPT_NAME_COLUMN);
        String committerName = null;
        String committerEmail = null;

        ScriptInfo scriptInfo = new ScriptInfo();
        scriptInfo.applicationID = applicationID;
        scriptInfo.jobID = jobID;
        scriptInfo.scriptUrl = scriptUrl;
        scriptInfo.scriptPath = scriptPath;
        scriptInfo.scriptType = scriptType;
        scriptInfo.scriptName = scriptName;
        scriptInfo.chainName = chainName;
        scriptInfo.jobName = jobName;
        scriptInfo.committerName = committerName;
        scriptInfo.committerEmail = committerEmail;

        return scriptInfo;
    }
}