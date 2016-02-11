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

import controllers.api.v1.Jira;
import models.DatasetComment;
import models.JiraTicket;
import models.LdapInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class JiraTicketRowMapper implements RowMapper<JiraTicket>
{
    public static String USER_ID_COLUMN = "user_id";
    public static String FULL_NAME_COLUMN = "full_name";
    public static String DISPLAY_NAME_COLUMN = "display_name";
    public static String EMAIL_COLUMN = "email";
    public static String ORG_HIERARCHY_COLUMN = "org_hierarchy";
    public static String TITLE_COLUMN = "title";
    public static String MANAGER_USER_ID_COLUMN = "manager_user_id";
    public static String HDFS_NAME_COLUMN = "hdfs_name";
    public static String DIRECTORY_PATH_COLUMN = "directory_path";
    public static String TOTAL_SIZE_COLUMN = "total_size_mb";
    public static String NUM_OF_FILES_COLUMN = "num_of_files";
    public static String JIRA_KEY_COLUMN = "jira_key";
    public static String JIRA_STATUS_COLUMN = "jira_status";
    public static String JIRA_COMPONENT_COLUMN = "jira_component";

    @Override
    public JiraTicket mapRow(ResultSet rs, int rowNum) throws SQLException    {

        String userId = rs.getString(USER_ID_COLUMN);
        String fullName = rs.getString(FULL_NAME_COLUMN);
        String displayName = rs.getString(DISPLAY_NAME_COLUMN);
        String title = rs.getString(TITLE_COLUMN);
        String managerId = rs.getString(MANAGER_USER_ID_COLUMN);
        String email = rs.getString(EMAIL_COLUMN);
        String orgHierarchy = rs.getString(ORG_HIERARCHY_COLUMN);
        String hdfsName = rs.getString(HDFS_NAME_COLUMN);
        String directoryPath = rs.getString(DIRECTORY_PATH_COLUMN);
        Long totalSize = rs.getLong(TOTAL_SIZE_COLUMN);
        Long numOfFiles = rs.getLong(NUM_OF_FILES_COLUMN);
        String jiraKey = rs.getString(JIRA_KEY_COLUMN);
        String jiraStatus = rs.getString(JIRA_STATUS_COLUMN);
        String jiraComponent = rs.getString(JIRA_COMPONENT_COLUMN);
        JiraTicket ticket = new JiraTicket();
        ticket.currentAssignee = userId;
        ticket.assigneeDisplayName = displayName;
        ticket.assigneeFullName = fullName;
        ticket.assigneeTitle = title;
        ticket.assigneeEmail = email;
        ticket.currentAssigneeOrgHierarchy = orgHierarchy;
        ticket.assigneeManagerId = managerId;
        ticket.ticketHdfsName = hdfsName;
        ticket.ticketDirectoryPath = directoryPath;
        ticket.ticketTotalSize = totalSize;
        ticket.ticketNumOfFiles = numOfFiles;
        ticket.ticketKey = jiraKey;
        ticket.ticketStatus = jiraStatus;
        ticket.ticketComponent = jiraComponent;

        return ticket;
    }
}
