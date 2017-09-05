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

import wherehows.models.table.JiraTicket;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;


public class JiraTicketRowMapper implements RowMapper<JiraTicket> {
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
  public JiraTicket mapRow(ResultSet rs, int rowNum) throws SQLException {

    JiraTicket ticket = new JiraTicket();
    ticket.setCurrentAssignee(rs.getString(USER_ID_COLUMN));
    ticket.setAssigneeDisplayName(rs.getString(DISPLAY_NAME_COLUMN));
    ticket.setAssigneeFullName(rs.getString(FULL_NAME_COLUMN));
    ticket.setAssigneeTitle(rs.getString(TITLE_COLUMN));
    ticket.setAssigneeEmail(rs.getString(EMAIL_COLUMN));
    ticket.setCurrentAssigneeOrgHierarchy(rs.getString(ORG_HIERARCHY_COLUMN));
    ticket.setAssigneeManagerId(rs.getString(MANAGER_USER_ID_COLUMN));
    ticket.setTicketHdfsName(rs.getString(HDFS_NAME_COLUMN));
    ticket.setTicketDirectoryPath(rs.getString(DIRECTORY_PATH_COLUMN));
    ticket.setTicketTotalSize(rs.getLong(TOTAL_SIZE_COLUMN));
    ticket.setTicketNumOfFiles(rs.getLong(NUM_OF_FILES_COLUMN));
    ticket.setTicketKey(rs.getString(JIRA_KEY_COLUMN));
    ticket.setTicketStatus(rs.getString(JIRA_STATUS_COLUMN));
    ticket.setTicketComponent(rs.getString(JIRA_COMPONENT_COLUMN));

    return ticket;
  }
}
