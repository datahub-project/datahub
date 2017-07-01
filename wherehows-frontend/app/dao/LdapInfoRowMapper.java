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
import models.LdapInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LdapInfoRowMapper implements RowMapper<LdapInfo>
{
    public static String USER_ID_COLUMN = "user_id";
    public static String FULL_NAME_COLUMN = "full_name";
    public static String DISPLAY_NAME_COLUMN = "display_name";
    public static String EMAIL_COLUMN = "email";
    public static String DEPARTMENT_ID_COLUMN = "department_id";
    public static String DEPARTMENT_NAME_COLUMN = "department_name";
    public static String MANAGER_USER_ID_COLUMN = "manager_user_id";
    public static String ORG_HIERARCHY_COLUMN = "org_hierarchy";
    public static String ICON_URL_PREFIX = "https://cinco.corp.linkedin.com/api/profile/";
    public static String ICON_URL_SUBFIX = "/picture?access_token=2rzmbzEMGlHsszQktFY-B1TxUic";

    @Override
    public LdapInfo mapRow(ResultSet rs, int rowNum) throws SQLException    {

        String userId = rs.getString(USER_ID_COLUMN);
        String fullName = rs.getString(FULL_NAME_COLUMN);
        String displayName = rs.getString(DISPLAY_NAME_COLUMN);
        String email = rs.getString(EMAIL_COLUMN);
        Integer departmentId = rs.getInt(DEPARTMENT_ID_COLUMN);
        String departmentName = rs.getString(DEPARTMENT_NAME_COLUMN);
        String managerUserId = rs.getString(MANAGER_USER_ID_COLUMN);
        String orgHierarchy = rs.getString(ORG_HIERARCHY_COLUMN);
        LdapInfo ldapInfo = new LdapInfo();
        ldapInfo.userName = userId;
        ldapInfo.displayName = displayName;
        ldapInfo.fullName = fullName;
        ldapInfo.email = email;
        ldapInfo.departmentId = departmentId;
        ldapInfo.departmentName = departmentName;
        ldapInfo.managerUserId = managerUserId;
        ldapInfo.orgHierarchy = orgHierarchy;
        ldapInfo.iconUrl = ICON_URL_PREFIX + userId + ICON_URL_SUBFIX;

        return ldapInfo;
    }
}
