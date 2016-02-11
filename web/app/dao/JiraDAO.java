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

import java.util.*;

import models.*;

public class JiraDAO extends AbstractMySQLOpenSourceDAO
{
	  private final static String GET_LDAP_INFO  = "SELECT full_name, display_name, email, " +
			  "department_id, department_name, manager_user_id, org_hierarchy, user_id " +
        "FROM dir_external_user_info";

    private final static String GET_CURRENT_USER_INFO  = "SELECT full_name, display_name, email, " +
        "department_id, department_name, manager_user_id, org_hierarchy, user_id " +
        "FROM dir_external_user_info WHERE user_id = ?";

    private final static String GET_FIRST_LEVEL_LDAP_INFO  = "SELECT full_name, display_name, email, " +
        "department_id, department_name, manager_user_id, org_hierarchy, user_id " +
        "FROM dir_external_user_info WHERE manager_user_id = ?";

    private final static String GET_TICKETS_BY_MANAGER_ID = "SELECT u.user_id, u.full_name, " +
        "u.display_name, u.title, u.manager_user_id, u.email, u.org_hierarchy, l.hdfs_name, l.directory_path, " +
        "l.total_size_mb, l.num_of_files, l.jira_key, l.jira_status, l.jira_component " +
        "FROM dir_external_user_info u JOIN log_jira__hdfs_directory_to_owner_map l " +
        "on u.user_id = l.current_assignee_id WHERE u.org_hierarchy like ?";

	  public static List<LdapInfo> getLdapInfo()
	  {
        return getJdbcTemplate().query(GET_LDAP_INFO, new LdapInfoRowMapper());
		}

    public static List<LdapInfo> getCurrentUserLdapInfo(String userId)
    {
        return getJdbcTemplate().query(GET_CURRENT_USER_INFO, new LdapInfoRowMapper(), userId);
    }

    public static List<LdapInfo> getFirstLevelLdapInfo(String managerId)
    {
        return getJdbcTemplate().query(GET_FIRST_LEVEL_LDAP_INFO, new LdapInfoRowMapper(), managerId);
    }

    public static List<LdapInfo> getFirstLevelLdapInfo()
    {
        return getJdbcTemplate().query(GET_LDAP_INFO, new LdapInfoRowMapper());
    }

    public static List<JiraTicket> getUserTicketsByManagerId(String managerId)
    {
      return getJdbcTemplate().query(GET_TICKETS_BY_MANAGER_ID, new JiraTicketRowMapper(), "/%" + managerId + "%");
    }
}
