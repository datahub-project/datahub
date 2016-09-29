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

import com.fasterxml.jackson.databind.node.ObjectNode;
import models.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import play.Logger;
import play.libs.Json;
import java.text.DecimalFormat;

public class DashboardDAO extends AbstractMySQLOpenSourceDAO
{

    private static DecimalFormat df2 = new DecimalFormat("##0.##");

    private final static String GET_CONFIDENTIAL_DATASETS  = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in " +
            "( SELECT DISTINCT dataset_id from dict_pii_field )" +
            " and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_OWNERSHIP_CONFIRMED_DATASETS = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, " +
            "GROUP_CONCAT(CASE WHEN o.confirmed_by is not null and o.confirmed_by != '' THEN o.confirmed_by " +
            "ELSE null END " +
            "ORDER BY o.confirmed_by ASC SEPARATOR ',') as confirmed_owner_id, d.name " +
            "FROM dataset_owner o " +
            "JOIN dict_dataset d ON o.dataset_id = d.id " +
            "WHERE o.dataset_id in " +
            "(SELECT DISTINCT dataset_id FROM dataset_owner " +
            "WHERE confirmed_by is not null and confirmed_by != '' and ( is_deleted != 'Y' or is_deleted is null )) " +
            "and o.owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_OWNERSHIP_DATASETS = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, " +
            "GROUP_CONCAT(CASE WHEN o.confirmed_by is not null THEN o.confirmed_by ELSE null END " +
            "ORDER BY o.confirmed_by ASC SEPARATOR ',') " +
            "as confirmed_owner_id, d.name " +
            "FROM dataset_owner o " +
            "JOIN dict_dataset d ON o.dataset_id = d.id and ( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_OWNERSHIP_UNCONFIRMED_DATASETS = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, " +
            "GROUP_CONCAT(CASE WHEN o.confirmed_by is not null THEN o.confirmed_by ELSE null END " +
            "ORDER BY o.confirmed_by ASC SEPARATOR ',') as confirmed_owner_id, d.name " +
            "FROM dataset_owner o " +
            "JOIN dict_dataset d ON o.dataset_id = d.id and ( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id not in (SELECT DISTINCT dataset_id FROM dataset_owner " +
            "WHERE confirmed_by is not null and confirmed_by != '' and ( is_deleted != 'Y' or is_deleted is null ))" +
            " and o.owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_ALL_DATASETS_BY_ID  = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITH_DESCRIPTION_BY_ID  = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id FROM comments ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_NO_DESCRIPTION_BY_ID  = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id not in ( SELECT DISTINCT dataset_id FROM comments ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITH_IDPC_COMPLIANCE_BY_ID  = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id FROM dataset_security WHERE compliance_type = ? ) " +
            "and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITH_FULL_FIELD_DESCRIPTION_BY_ID  = "SELECT SQL_CALC_FOUND_ROWS " +
            "o.dataset_id, GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( " +
            "SELECT dd.dataset_id FROM " +
            "( SELECT count(field_id) as field_count, dataset_id FROM dict_field_detail GROUP BY dataset_id ) dd " +
            "JOIN " +
            "( SELECT count(distinct field_id) as comments_count, dataset_id FROM dict_dataset_field_comment " +
            "GROUP BY dataset_id) dc ON dd.dataset_id = dc.dataset_id and dd.field_count = dc.comments_count " +
            ") and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITH_ANY_FIELD_DESCRIPTION_BY_ID  = "SELECT SQL_CALC_FOUND_ROWS " +
            "o.dataset_id, GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id FROM dict_dataset_field_comment ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITH_NO_FIELD_DESCRIPTION_BY_ID  = "SELECT SQL_CALC_FOUND_ROWS " +
            "o.dataset_id, GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id not in ( SELECT DISTINCT dataset_id FROM dict_dataset_field_comment ) and " +
            "owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_CONFIDENTIAL_INFO  = "SELECT count(distinct o.dataset_id) as count " +
            "FROM dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id from dict_pii_field ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)";

    private final static String GET_COUNT_OF_DATASET  = "SELECT count(DISTINCT o.dataset_id) as count " +
            "FROM dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE owner_id in (" +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)";

    private final static String GET_COUNT_OF_DATASET_WITH_DESCRIPTION  = "SELECT count(DISTINCT o.dataset_id) " +
            "as count FROM dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id from comments ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)";

    private final static String GET_COUNT_OF_DATASET_WITH_IDPC_PURGE  = "SELECT count(DISTINCT o.dataset_id) " +
            "as count FROM dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id from dataset_security ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)";

    private final static String GET_COUNT_OF_DATASET_WITH_CONFIRMED_OWNER  = "SELECT count(DISTINCT o.dataset_id) " +
            "as count FROM dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id " +
            "WHERE o.dataset_id in (SELECT DISTINCT dataset_id FROM dataset_owner " +
            "WHERE confirmed_by is not null and confirmed_by != '' and ( is_deleted != 'Y' or is_deleted is null )) " +
            "and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)";

    private final static String GET_FIELDS_WITH_DESCRIPTION = "SELECT field_name FROM " +
            "dict_field_detail dd JOIN dict_dataset_field_comment fc ON " +
            "dd.field_id = fc.field_id and dd.dataset_id = fc.dataset_id WHERE dd.dataset_id = ?";

    private final static String GET_DATASET_LEVEL_COMMENTS = "SELECT text FROM comments WHERE dataset_id = ? LIMIT 1";

    private final static String GET_COMPLIANCE_TYPE_BY_DATASET_ID = "SELECT compliance_type FROM " +
            "dataset_security WHERE dataset_id = ?";

    private final static String GET_CONFIDENTIAL_FIELDS = "SELECT field_name " +
            "FROM dict_pii_field WHERE dataset_id = ?";

    private final static String GET_CONFIRMED_DATASET_BAR_CHART_DATA = "SELECT " +
            "SUM(CASE WHEN dc.confirmed_on > NOW() - INTERVAL 1 MONTH THEN 1 ELSE 0 END) as 1_month_ago, " +
            "SUM(CASE WHEN dc.confirmed_on > NOW() - INTERVAL 2 MONTH THEN 1 ELSE 0 END) as 2_month_ago, " +
            "SUM(CASE WHEN dc.confirmed_on > NOW() - INTERVAL 3 MONTH THEN 1 ELSE 0 END) as 3_month_ago, " +
            "SUM(CASE WHEN dc.confirmed_on > NOW() - INTERVAL 4 MONTH THEN 1 ELSE 0 END) as 4_month_ago, " +
            "SUM(CASE WHEN dc.confirmed_on > NOW() - INTERVAL 5 MONTH THEN 1 ELSE 0 END) as 5_month_ago, " +
            "SUM(CASE WHEN dc.confirmed_on > NOW() - INTERVAL 6 MONTH THEN 1 ELSE 0 END) as 6_month_ago " +
            "FROM ( " +
            "SELECT d.dataset_id, from_unixtime(d.confirmed_on) as confirmed_on from dataset_owner d " +
            "WHERE d.confirmed_by is not null and d.owner_id in ( SELECT user_id FROM dir_external_user_info " +
            "WHERE org_hierarchy = ? or org_hierarchy like ?)) dc";

    private final static String GET_COMMENTS_DESCRIPTION_BAR_CHART_DATA = "SELECT " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 1 MONTH THEN 1 ELSE 0 END) as 1_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 2 MONTH THEN 1 ELSE 0 END) as 2_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 3 MONTH THEN 1 ELSE 0 END) as 3_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 4 MONTH THEN 1 ELSE 0 END) as 4_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 5 MONTH THEN 1 ELSE 0 END) as 5_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 6 MONTH THEN 1 ELSE 0 END) as 6_month_ago " +
            "FROM ( SELECT dc.id, dc.comments_on FROM " +
            "(SELECT d.id, CASE WHEN c.modified is not null THEN c.modified ELSE c.created END as comments_on " +
            "FROM dict_dataset d JOIN comments c ON d.id = c.dataset_id ORDER BY d.id) dc JOIN " +
            "(SELECT dataset_id FROM dataset_owner WHERE owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)) o " +
            "ON dc.id = o.dataset_id GROUP BY dc.id) co";

    private final static String GET_FIELD_COMMENTS_DESCRIPTION_BAR_CHART_DATA = "SELECT " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 1 MONTH THEN 1 ELSE 0 END) as 1_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 2 MONTH THEN 1 ELSE 0 END) as 2_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 3 MONTH THEN 1 ELSE 0 END) as 3_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 4 MONTH THEN 1 ELSE 0 END) as 4_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 5 MONTH THEN 1 ELSE 0 END) as 5_month_ago, " +
            "SUM(CASE WHEN comments_on > NOW() - INTERVAL 6 MONTH THEN 1 ELSE 0 END) as 6_month_ago " +
            "FROM (SELECT df.dataset_id as id, " +
            "CASE WHEN fc.modified is not null THEN fc.modified ELSE fc.created END as comments_on " +
            "FROM dict_dataset_field_comment df JOIN field_comments fc ON df.comment_id = fc.id " +
            "JOIN dataset_owner o on df.dataset_id = o.dataset_id " +
            "WHERE o.owner_id in (SELECT user_id FROM dir_external_user_info " +
            "WHERE org_hierarchy = ? or org_hierarchy like ?)) co";

    private final static String IDPC_AUTO_PURGE = "AUTO_PURGE";
    private final static String IDPC_CUSTOMER_PURGE = "CUSTOMER_PURGE";
    private final static String IDPC_RETENTION = "RETENTION";
    private final static String IDPC_PURGE_NA = "Not purge applicable";
    private final static String IDPC_PURGE_UNKNOWN = "Unknown";
    private final static String ALL_DATASETS = "All Datasets";

    public static ObjectNode getOwnershipPercentageByManagerId(String managerId) {

        ObjectNode resultNode = Json.newObject();
        ConfidentialFieldsOwner currentUser = null;
        List<ConfidentialFieldsOwner> memberList = new ArrayList<ConfidentialFieldsOwner>();
        if (StringUtils.isNotBlank(managerId))
        {
            List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
            if (ldapInfoList != null && ldapInfoList.size() > 0)
            {
                LdapInfo ldapInfo = ldapInfoList.get(0);
                if (ldapInfo != null)
                {
                    currentUser = new ConfidentialFieldsOwner();
                    currentUser.displayName = ldapInfo.displayName;
                    currentUser.iconUrl = ldapInfo.iconUrl;
                    currentUser.managerUserId = ldapInfo.managerUserId;
                    currentUser.orgHierarchy = ldapInfo.orgHierarchy;
                    currentUser.userName = ldapInfo.userName;
                    if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                        currentUser.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET,
                                Long.class,
                                currentUser.orgHierarchy,
                                currentUser.orgHierarchy + "/%");
                    }
                    else if (managerId.equalsIgnoreCase("jweiner"))
                    {
                        currentUser.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET,
                                Long.class,
                                "/" + currentUser.userName,
                                "/" + currentUser.userName + "/%");
                    }
                    if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                        currentUser.confirmed = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET_WITH_CONFIRMED_OWNER,
                                Long.class,
                                currentUser.orgHierarchy,
                                currentUser.orgHierarchy + "/%");
                    }
                    else if (managerId.equalsIgnoreCase("jweiner"))
                    {
                        currentUser.confirmed = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET_WITH_CONFIRMED_OWNER,
                                Long.class,
                                "/" + currentUser.userName,
                                "/" + currentUser.userName + "/%");
                    }

                    if (currentUser.potentialDatasets != 0)
                    {
                        currentUser.completed =
                                df2.format(100.0 * currentUser.confirmed / currentUser.potentialDatasets);
                    }
                    else
                    {
                        currentUser.completed = df2.format(0.0);
                    }

                }
            }
            List<LdapInfo> members = JiraDAO.getFirstLevelLdapInfo(managerId);
            if (members != null)
            {
                for(LdapInfo member : members)
                {
                    if (member != null && StringUtils.isNotBlank(member.orgHierarchy))
                    {
                        ConfidentialFieldsOwner owner = new ConfidentialFieldsOwner();
                        owner.displayName = member.displayName;
                        owner.fullName = member.fullName;
                        owner.userName = member.userName;
                        owner.iconUrl = member.iconUrl;
                        owner.managerUserId = member.managerUserId;
                        owner.orgHierarchy = member.orgHierarchy;
                        owner.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET,
                                Long.class,
                                owner.orgHierarchy,
                                owner.orgHierarchy + "/%");

                        owner.confirmed = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET_WITH_CONFIRMED_OWNER,
                                Long.class,
                                owner.orgHierarchy,
                                owner.orgHierarchy + "/%");
                        if (owner.potentialDatasets != 0)
                        {
                            owner.completed = df2.format(100.0 * owner.confirmed / owner.potentialDatasets);
                        }
                        else
                        {
                            owner.completed = df2.format(0.0);
                        }
                        memberList.add(owner);
                    }
                }
            }
        }
        resultNode.put("status", "ok");
        resultNode.set("currentUser", Json.toJson(currentUser));
        resultNode.set("members", Json.toJson(memberList));
        return resultNode;
    }

    public static ObjectNode getDescriptionPercentageByManagerId(String managerId) {

        ObjectNode resultNode = Json.newObject();
        ConfidentialFieldsOwner currentUser = null;
        List<ConfidentialFieldsOwner> memberList = new ArrayList<ConfidentialFieldsOwner>();
        if (StringUtils.isNotBlank(managerId))
        {
            List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
            if (ldapInfoList != null && ldapInfoList.size() > 0)
            {
                LdapInfo ldapInfo = ldapInfoList.get(0);
                if (ldapInfo != null)
                {
                    currentUser = new ConfidentialFieldsOwner();
                    currentUser.displayName = ldapInfo.displayName;
                    currentUser.iconUrl = ldapInfo.iconUrl;
                    currentUser.managerUserId = ldapInfo.managerUserId;
                    currentUser.orgHierarchy = ldapInfo.orgHierarchy;
                    currentUser.userName = ldapInfo.userName;
                    if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                        currentUser.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET,
                                Long.class,
                                currentUser.orgHierarchy,
                                currentUser.orgHierarchy + "/%");
                    }
                    else if (managerId.equalsIgnoreCase("jweiner"))
                    {
                        currentUser.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET,
                                Long.class,
                                "/" + currentUser.userName,
                                "/" + currentUser.userName + "/%");
                    }
                    if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                        currentUser.confirmed = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET_WITH_DESCRIPTION,
                                Long.class,
                                currentUser.orgHierarchy,
                                currentUser.orgHierarchy + "/%");
                    }
                    else if (managerId.equalsIgnoreCase("jweiner"))
                    {
                        currentUser.confirmed = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET_WITH_DESCRIPTION,
                                Long.class,
                                "/" + currentUser.userName,
                                "/" + currentUser.userName + "/%");
                    }

                    if (currentUser.potentialDatasets != 0)
                    {
                        currentUser.completed =
                                df2.format(100.0 * currentUser.confirmed / currentUser.potentialDatasets);
                    }
                    else
                    {
                        currentUser.completed = df2.format(0.0);
                    }

                }
            }
            List<LdapInfo> members = JiraDAO.getFirstLevelLdapInfo(managerId);
            if (members != null)
            {
                for(LdapInfo member : members)
                {
                    if (member != null && StringUtils.isNotBlank(member.orgHierarchy))
                    {
                        ConfidentialFieldsOwner owner = new ConfidentialFieldsOwner();
                        owner.displayName = member.displayName;
                        owner.fullName = member.fullName;
                        owner.userName = member.userName;
                        owner.iconUrl = member.iconUrl;
                        owner.managerUserId = member.managerUserId;
                        owner.orgHierarchy = member.orgHierarchy;
                        owner.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET,
                                Long.class,
                                owner.orgHierarchy,
                                owner.orgHierarchy + "/%");

                        owner.confirmed = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET_WITH_DESCRIPTION,
                                Long.class,
                                owner.orgHierarchy,
                                owner.orgHierarchy + "/%");
                        if (owner.potentialDatasets != 0)
                        {
                            owner.completed = df2.format(100.0 * owner.confirmed / owner.potentialDatasets);
                        }
                        else
                        {
                            owner.completed = df2.format(0.0);
                        }
                        memberList.add(owner);
                    }
                }
            }
        }
        resultNode.put("status", "ok");
        resultNode.set("currentUser", Json.toJson(currentUser));
        resultNode.set("members", Json.toJson(memberList));
        return resultNode;
    }

    public static ObjectNode getIdpcCompliancePercentageByManagerId(String managerId) {

        ObjectNode resultNode = Json.newObject();
        ConfidentialFieldsOwner currentUser = null;
        List<ConfidentialFieldsOwner> memberList = new ArrayList<ConfidentialFieldsOwner>();
        if (StringUtils.isNotBlank(managerId))
        {
            List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
            if (ldapInfoList != null && ldapInfoList.size() > 0)
            {
                LdapInfo ldapInfo = ldapInfoList.get(0);
                if (ldapInfo != null)
                {
                    currentUser = new ConfidentialFieldsOwner();
                    currentUser.displayName = ldapInfo.displayName;
                    currentUser.iconUrl = ldapInfo.iconUrl;
                    currentUser.managerUserId = ldapInfo.managerUserId;
                    currentUser.orgHierarchy = ldapInfo.orgHierarchy;
                    currentUser.userName = ldapInfo.userName;
                    if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                        currentUser.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET,
                                Long.class,
                                currentUser.orgHierarchy,
                                currentUser.orgHierarchy + "/%");
                    }
                    else if (managerId.equalsIgnoreCase("jweiner"))
                    {
                        currentUser.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET,
                                Long.class,
                                "/" + currentUser.userName,
                                "/" + currentUser.userName + "/%");
                    }
                    if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                        currentUser.confirmed = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET_WITH_IDPC_PURGE,
                                Long.class,
                                currentUser.orgHierarchy,
                                currentUser.orgHierarchy + "/%");
                    }
                    else if (managerId.equalsIgnoreCase("jweiner"))
                    {
                        currentUser.confirmed = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET_WITH_IDPC_PURGE,
                                Long.class,
                                "/" + currentUser.userName,
                                "/" + currentUser.userName + "/%");
                    }

                    if (currentUser.potentialDatasets != 0)
                    {
                        currentUser.completed =
                                df2.format(100.0 * currentUser.confirmed / currentUser.potentialDatasets);
                    }
                    else
                    {
                        currentUser.completed = df2.format(0.0);
                    }

                }
            }
            List<LdapInfo> members = JiraDAO.getFirstLevelLdapInfo(managerId);
            if (members != null)
            {
                for(LdapInfo member : members)
                {
                    if (member != null && StringUtils.isNotBlank(member.orgHierarchy))
                    {
                        ConfidentialFieldsOwner owner = new ConfidentialFieldsOwner();
                        owner.displayName = member.displayName;
                        owner.fullName = member.fullName;
                        owner.userName = member.userName;
                        owner.iconUrl = member.iconUrl;
                        owner.managerUserId = member.managerUserId;
                        owner.orgHierarchy = member.orgHierarchy;
                        owner.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET,
                                Long.class,
                                owner.orgHierarchy,
                                owner.orgHierarchy + "/%");

                        owner.confirmed = getJdbcTemplate().queryForObject(
                                GET_COUNT_OF_DATASET_WITH_IDPC_PURGE,
                                Long.class,
                                owner.orgHierarchy,
                                owner.orgHierarchy + "/%");
                        if (owner.potentialDatasets != 0)
                        {
                            owner.completed = df2.format(100.0 * owner.confirmed / owner.potentialDatasets);
                        }
                        else
                        {
                            owner.completed = df2.format(0.0);
                        }
                        memberList.add(owner);
                    }
                }
            }
        }
        resultNode.put("status", "ok");
        resultNode.set("currentUser", Json.toJson(currentUser));
        resultNode.set("members", Json.toJson(memberList));
        return resultNode;
    }

    public static ObjectNode getConfidentialPercentageByManagerId(String managerId) {

        ObjectNode resultNode = Json.newObject();
        ConfidentialFieldsOwner currentUser = null;
        List<ConfidentialFieldsOwner> memberList = new ArrayList<ConfidentialFieldsOwner>();
        if (StringUtils.isNotBlank(managerId))
        {
            List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
            if (ldapInfoList != null && ldapInfoList.size() > 0)
            {
                LdapInfo ldapInfo = ldapInfoList.get(0);
                if (ldapInfo != null)
                {
                    currentUser = new ConfidentialFieldsOwner();
                    currentUser.displayName = ldapInfo.displayName;
                    currentUser.iconUrl = ldapInfo.iconUrl;
                    currentUser.managerUserId = ldapInfo.managerUserId;
                    currentUser.orgHierarchy = ldapInfo.orgHierarchy;
                    currentUser.userName = ldapInfo.userName;
                    if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                        currentUser.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_CONFIDENTIAL_INFO,
                                Long.class,
                                currentUser.orgHierarchy,
                                currentUser.orgHierarchy + "/%");
                    }
                    else if (managerId.equalsIgnoreCase("jweiner"))
                    {
                        currentUser.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_CONFIDENTIAL_INFO,
                                Long.class,
                                "/" + currentUser.userName,
                                "/" + currentUser.userName + "/%");
                    }
                    currentUser.confirmed = 0L;
                    if (currentUser.potentialDatasets != 0)
                    {
                        currentUser.completed =
                                df2.format(100.0 * (currentUser.confirmed / currentUser.potentialDatasets));
                    }
                    else
                    {
                        currentUser.completed = df2.format(0.0);
                    }

                }
            }
            List<LdapInfo> members = JiraDAO.getFirstLevelLdapInfo(managerId);
            if (members != null)
            {
                for(LdapInfo member : members)
                {
                    if (member != null && StringUtils.isNotBlank(member.orgHierarchy))
                    {
                        ConfidentialFieldsOwner owner = new ConfidentialFieldsOwner();
                        owner.displayName = member.displayName;
                        owner.fullName = member.fullName;
                        owner.userName = member.userName;
                        owner.iconUrl = member.iconUrl;
                        owner.managerUserId = member.managerUserId;
                        owner.orgHierarchy = member.orgHierarchy;
                        owner.potentialDatasets = getJdbcTemplate().queryForObject(
                                GET_CONFIDENTIAL_INFO,
                                Long.class,
                                owner.orgHierarchy,
                                owner.orgHierarchy + "/%");

                        owner.confirmed = 0L;
                        if (owner.potentialDatasets != 0)
                        {
                            owner.completed = df2.format(100.0 * (owner.confirmed / owner.potentialDatasets));
                        }
                        else
                        {
                            owner.completed = df2.format(0.0);
                        }
                        memberList.add(owner);
                    }
                }
            }
        }
        resultNode.put("status", "ok");
        resultNode.set("currentUser", Json.toJson(currentUser));
        resultNode.set("members", Json.toJson(memberList));
        return resultNode;
    }

    public static ObjectNode getPagedOwnershipDatasetsByManagerId(String managerId, Integer option, Integer page, Integer size) {

        ObjectNode result = Json.newObject();

        javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        TransactionTemplate txTemplate = new TransactionTemplate(tm);
        result = txTemplate.execute(new TransactionCallback<ObjectNode>()
        {
            public ObjectNode doInTransaction(TransactionStatus status)
            {
                ObjectNode resultNode = Json.newObject();
                Long count = 0L;
                List<DatasetConfidential> confidentialList = new ArrayList<DatasetConfidential>();

                if (StringUtils.isNotBlank(managerId))
                {
                    List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
                    if (ldapInfoList != null && ldapInfoList.size() > 0)
                    {
                        LdapInfo ldapInfo = ldapInfoList.get(0);
                        if (ldapInfo != null)
                        {
                            String datasetQuery = GET_OWNERSHIP_DATASETS;
                            switch (option)
                            {
                                case 1:
                                    datasetQuery = GET_OWNERSHIP_CONFIRMED_DATASETS;
                                    break;
                                case 2:
                                    datasetQuery = GET_OWNERSHIP_UNCONFIRMED_DATASETS;
                                    break;
                                case 3:
                                    datasetQuery = GET_OWNERSHIP_DATASETS;
                                    break;
                                default:
                                    datasetQuery = GET_OWNERSHIP_DATASETS;
                            }
                            List<Map<String, Object>> rows = null;
                            if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                                rows = getJdbcTemplate().queryForList(
                                        datasetQuery,
                                        ldapInfo.orgHierarchy,
                                        ldapInfo.orgHierarchy + "/%",
                                        (page - 1) * size,
                                        size);
                            }
                            else if (managerId.equalsIgnoreCase("jweiner"))
                            {
                                rows = getJdbcTemplate().queryForList(
                                        datasetQuery,
                                        "/" + ldapInfo.userName,
                                        "/" + ldapInfo.userName + "/%",
                                        (page - 1) * size,
                                        size);
                            }

                            try {
                                count = getJdbcTemplate().queryForObject(
                                        "SELECT FOUND_ROWS()",
                                        Long.class);
                            }
                            catch(EmptyResultDataAccessException e)
                            {
                                Logger.error("Exception = " + e.getMessage());
                            }

                            for (Map row : rows) {
                                DatasetConfidential datasetConfidential = new DatasetConfidential();
                                datasetConfidential.datasetId = (Long) row.get("dataset_id");
                                datasetConfidential.datasetName = (String) row.get("name");
                                datasetConfidential.ownerId = (String) row.get("owner_id");
                                String confirmedOwnerId = (String) row.get("confirmed_owner_id");
                                if (StringUtils.isBlank(confirmedOwnerId) && option == 1)
                                {
                                    confirmedOwnerId = "<other team>";
                                }
                                datasetConfidential.confirmedOwnerId = confirmedOwnerId;
                                confidentialList.add(datasetConfidential);
                            }
                        }
                    }
                }
                resultNode.put("status", "ok");
                resultNode.put("count", count);
                resultNode.put("page", page);
                resultNode.put("itemsPerPage", size);
                resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
                resultNode.set("datasets", Json.toJson(confidentialList));
                return resultNode;
            }
        });
        return result;
    }

    public static ObjectNode getPagedConfidentialDatasetsByManagerId(String managerId, Integer page, Integer size) {

        ObjectNode result = Json.newObject();

        javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        TransactionTemplate txTemplate = new TransactionTemplate(tm);
        result = txTemplate.execute(new TransactionCallback<ObjectNode>()
        {
            public ObjectNode doInTransaction(TransactionStatus status)
            {
                ObjectNode resultNode = Json.newObject();
                Long count = 0L;
                List<DatasetConfidential> confidentialList = new ArrayList<DatasetConfidential>();

                if (StringUtils.isNotBlank(managerId))
                {
                    List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
                    if (ldapInfoList != null && ldapInfoList.size() > 0)
                    {
                        LdapInfo ldapInfo = ldapInfoList.get(0);
                        if (ldapInfo != null)
                        {
                            List<Map<String, Object>> rows = null;
                            if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                                rows = getJdbcTemplate().queryForList(
                                        GET_CONFIDENTIAL_DATASETS,
                                        ldapInfo.orgHierarchy,
                                        ldapInfo.orgHierarchy + "/%",
                                        (page - 1) * size,
                                        size);
                            }
                            else if (managerId.equalsIgnoreCase("jweiner"))
                            {
                                rows = getJdbcTemplate().queryForList(
                                        GET_CONFIDENTIAL_DATASETS,
                                        "/" + ldapInfo.userName,
                                        "/" + ldapInfo.userName + "/%",
                                        (page - 1) * size,
                                        size);
                            }

                            try {
                                count = getJdbcTemplate().queryForObject(
                                        "SELECT FOUND_ROWS()",
                                        Long.class);
                            }
                            catch(EmptyResultDataAccessException e)
                            {
                                Logger.error("Exception = " + e.getMessage());
                            }

                            for (Map row : rows) {
                                DatasetConfidential datasetConfidential = new DatasetConfidential();
                                datasetConfidential.datasetId = (Long) row.get("dataset_id");
                                datasetConfidential.datasetName = (String) row.get("name");
                                datasetConfidential.ownerId = (String) row.get("owner_id");
                                if (datasetConfidential.datasetId != null && datasetConfidential.datasetId > 0) {
                                    datasetConfidential.confidentialFieldList =
                                            getJdbcTemplate().queryForList(
                                                    GET_CONFIDENTIAL_FIELDS,
                                                    String.class,
                                                    datasetConfidential.datasetId);
                                }
                                confidentialList.add(datasetConfidential);
                            }
                        }
                    }
                }
                resultNode.put("status", "ok");
                resultNode.put("count", count);
                resultNode.put("page", page);
                resultNode.put("itemsPerPage", size);
                resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
                resultNode.set("datasets", Json.toJson(confidentialList));
                return resultNode;
            }
        });
        return result;
    }

    public static ObjectNode getPagedDescriptionDatasetsByManagerId(String managerId, Integer option, Integer page, Integer size) {

        ObjectNode result = Json.newObject();

        javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        TransactionTemplate txTemplate = new TransactionTemplate(tm);
        result = txTemplate.execute(new TransactionCallback<ObjectNode>()
        {
            public ObjectNode doInTransaction(TransactionStatus status)
            {
                ObjectNode resultNode = Json.newObject();
                Long count = 0L;
                List<DatasetConfidential> confidentialList = new ArrayList<DatasetConfidential>();
                Boolean isDatasetLevel = true;
                String description = "";

                if (StringUtils.isNotBlank(managerId))
                {
                    List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
                    if (ldapInfoList != null && ldapInfoList.size() > 0)
                    {
                        LdapInfo ldapInfo = ldapInfoList.get(0);
                        if (ldapInfo != null)
                        {
                            String datasetQuery = GET_ALL_DATASETS_BY_ID;
                            switch (option)
                            {
                                case 1:
                                    datasetQuery = GET_DATASETS_WITH_DESCRIPTION_BY_ID;
                                    description = "has dataset level description";
                                    break;
                                case 2:
                                    datasetQuery = GET_DATASETS_NO_DESCRIPTION_BY_ID;
                                    description = "no dataset level description";
                                    break;
                                case 3:
                                    datasetQuery = GET_DATASETS_WITH_FULL_FIELD_DESCRIPTION_BY_ID;
                                    description = "all fields have description";
                                    isDatasetLevel = false;
                                    break;
                                case 4:
                                    datasetQuery = GET_DATASETS_WITH_ANY_FIELD_DESCRIPTION_BY_ID;
                                    description = "has field description";
                                    isDatasetLevel = false;
                                    break;
                                case 5:
                                    datasetQuery = GET_DATASETS_WITH_NO_FIELD_DESCRIPTION_BY_ID;
                                    description = "no field description";
                                    isDatasetLevel = false;
                                    break;
                                case 6:
                                    datasetQuery = GET_ALL_DATASETS_BY_ID;
                                    description = "";
                                    break;
                                default:
                                    datasetQuery = GET_ALL_DATASETS_BY_ID;
                                    description = "";
                            }
                            List<Map<String, Object>> rows = null;
                            if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                                rows = getJdbcTemplate().queryForList(
                                        datasetQuery,
                                        ldapInfo.orgHierarchy,
                                        ldapInfo.orgHierarchy + "/%",
                                        (page - 1) * size,
                                        size);
                            }
                            else if (managerId.equalsIgnoreCase("jweiner"))
                            {
                                rows = getJdbcTemplate().queryForList(
                                        datasetQuery,
                                        "/" + ldapInfo.userName,
                                        "/" + ldapInfo.userName + "/%",
                                        (page - 1) * size,
                                        size);
                            }

                            try {
                                count = getJdbcTemplate().queryForObject(
                                        "SELECT FOUND_ROWS()",
                                        Long.class);
                            }
                            catch(EmptyResultDataAccessException e)
                            {
                                Logger.error("Exception = " + e.getMessage());
                            }

                            for (Map row : rows) {
                                DatasetConfidential datasetConfidential = new DatasetConfidential();
                                datasetConfidential.datasetId = (Long) row.get("dataset_id");
                                datasetConfidential.datasetName = (String) row.get("name");
                                datasetConfidential.ownerId = (String) row.get("owner_id");
                                if (datasetConfidential.datasetId != null && datasetConfidential.datasetId > 0) {
                                    if (isDatasetLevel)
                                    {
                                        datasetConfidential.confidentialFieldList =
                                                getJdbcTemplate().queryForList(
                                                        GET_DATASET_LEVEL_COMMENTS,
                                                        String.class,
                                                        datasetConfidential.datasetId);
                                    }
                                    else {
                                        datasetConfidential.confidentialFieldList =
                                                getJdbcTemplate().queryForList(
                                                        GET_FIELDS_WITH_DESCRIPTION,
                                                        String.class,
                                                        datasetConfidential.datasetId);
                                    }
                                }
                                confidentialList.add(datasetConfidential);
                            }
                        }
                    }
                }
                resultNode.put("status", "ok");
                resultNode.put("count", count);
                resultNode.put("page", page);
                resultNode.put("description", description);
                resultNode.put("itemsPerPage", size);
                resultNode.put("isDatasetLevel", isDatasetLevel);
                resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
                resultNode.set("datasets", Json.toJson(confidentialList));
                return resultNode;
            }
        });
        return result;
    }

    public static ObjectNode getPagedComplianceDatasetsByManagerId(
            String managerId,
            String option,
            Integer page,
            Integer size) {

        ObjectNode result = Json.newObject();

        javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        TransactionTemplate txTemplate = new TransactionTemplate(tm);
        result = txTemplate.execute(new TransactionCallback<ObjectNode>()
        {
            public ObjectNode doInTransaction(TransactionStatus status)
            {
                ObjectNode resultNode = Json.newObject();
                Long count = 0L;
                List<DatasetConfidential> confidentialList = new ArrayList<DatasetConfidential>();

                if (StringUtils.isNotBlank(managerId))
                {
                    List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
                    if (ldapInfoList != null && ldapInfoList.size() > 0)
                    {
                        LdapInfo ldapInfo = ldapInfoList.get(0);
                        if (ldapInfo != null)
                        {
                            List<Map<String, Object>> rows = null;
                            if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {

                                if (StringUtils.isNotBlank(option) && !(option.equalsIgnoreCase(ALL_DATASETS)))
                                {
                                    rows = getJdbcTemplate().queryForList(
                                            GET_DATASETS_WITH_IDPC_COMPLIANCE_BY_ID,
                                            option,
                                            ldapInfo.orgHierarchy,
                                            ldapInfo.orgHierarchy + "/%",
                                            (page - 1) * size,
                                            size);
                                }
                                else
                                {
                                    rows = getJdbcTemplate().queryForList(
                                            GET_ALL_DATASETS_BY_ID,
                                            ldapInfo.orgHierarchy,
                                            ldapInfo.orgHierarchy + "/%",
                                            (page - 1) * size,
                                            size);
                                }

                            }
                            else if (managerId.equalsIgnoreCase("jweiner"))
                            {
                                if (StringUtils.isNotBlank(option) && !(option.equalsIgnoreCase(ALL_DATASETS)))
                                {
                                    rows = getJdbcTemplate().queryForList(
                                            GET_DATASETS_WITH_IDPC_COMPLIANCE_BY_ID,
                                            option,
                                            "/" + ldapInfo.userName,
                                            "/" + ldapInfo.userName + "/%",
                                            (page - 1) * size,
                                            size);
                                }
                                else
                                {
                                    rows = getJdbcTemplate().queryForList(
                                            GET_ALL_DATASETS_BY_ID,
                                            "/" + ldapInfo.userName,
                                            "/" + ldapInfo.userName + "/%",
                                            (page - 1) * size,
                                            size);
                                }
                            }

                            try {
                                count = getJdbcTemplate().queryForObject(
                                        "SELECT FOUND_ROWS()",
                                        Long.class);
                            }
                            catch(EmptyResultDataAccessException e)
                            {
                                Logger.error("Exception = " + e.getMessage());
                            }

                            for (Map row : rows) {
                                DatasetConfidential datasetConfidential = new DatasetConfidential();
                                datasetConfidential.datasetId = (Long) row.get("dataset_id");
                                datasetConfidential.datasetName = (String) row.get("name");
                                datasetConfidential.ownerId = (String) row.get("owner_id");
                                if (datasetConfidential.datasetId != null && datasetConfidential.datasetId > 0) {
                                    datasetConfidential.confidentialFieldList =
                                            getJdbcTemplate().queryForList(
                                                    GET_COMPLIANCE_TYPE_BY_DATASET_ID,
                                                    String.class,
                                                    datasetConfidential.datasetId);
                                }
                                confidentialList.add(datasetConfidential);
                            }
                        }
                    }
                }
                resultNode.put("status", "ok");
                resultNode.put("count", count);
                resultNode.put("page", page);
                resultNode.put("itemsPerPage", size);
                resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
                resultNode.set("datasets", Json.toJson(confidentialList));
                return resultNode;
            }
        });
        return result;
    }

    public static ObjectNode getOwnershipBarChartData(String managerId) {

        ObjectNode resultNode = Json.newObject();
        List<MetadataBarData> barDataList = new ArrayList<MetadataBarData>();

        if (StringUtils.isNotBlank(managerId))
        {
            List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
            if (ldapInfoList != null && ldapInfoList.size() > 0)
            {
                LdapInfo ldapInfo = ldapInfoList.get(0);
                if (ldapInfo != null)
                {
                    String query = GET_CONFIRMED_DATASET_BAR_CHART_DATA;
                    List<Map<String, Object>> rows = null;
                    if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                        rows = getJdbcTemplate().queryForList(
                                query,
                                ldapInfo.orgHierarchy,
                                ldapInfo.orgHierarchy + "/%");
                    }
                    else if (managerId.equalsIgnoreCase("jweiner"))
                    {
                        rows = getJdbcTemplate().queryForList(
                                query,
                                "/" + ldapInfo.userName,
                                "/" + ldapInfo.userName + "/%");
                    }

                    for (Map row : rows) {
                        for (int i = 6; i > 0; i--)
                        {
                            java.math.BigDecimal value = (java.math.BigDecimal) row.get(Integer.toString(i) + "_month_ago");
                            GregorianCalendar date = new GregorianCalendar();
                            Integer month = date.get(Calendar.MONTH);
                            Integer year = date.get(Calendar.YEAR);
                            month = month+1;
                            if (month > i)
                            {
                                month = month - i + 1;
                            }
                            else
                            {
                                month = month + 12 - i + 1;
                            }
                            String label = Integer.toString(month) + "/" + Integer.toString(year);
                            MetadataBarData metadataBarData = new MetadataBarData();
                            metadataBarData.label = label;
                            if (value != null)
                            {
                                metadataBarData.value = value.longValue();
                            }
                            else
                            {
                                metadataBarData.value = 0L;
                            }
                            barDataList.add(metadataBarData);
                        }
                    }
                }
            }
        }
        resultNode.put("status", "ok");
        resultNode.set("barData", Json.toJson(barDataList));
        return resultNode;
    }

    public static ObjectNode getDescriptionBarChartData(String managerId, Integer option) {

        ObjectNode resultNode = Json.newObject();
        List<MetadataBarData> barDataList = new ArrayList<MetadataBarData>();

        if (StringUtils.isNotBlank(managerId))
        {
            List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(managerId);
            if (ldapInfoList != null && ldapInfoList.size() > 0)
            {
                LdapInfo ldapInfo = ldapInfoList.get(0);
                if (ldapInfo != null)
                {
                    String query = GET_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
                    switch (option)
                    {
                        case 1:
                            query = GET_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
                            break;
                        case 2:
                            query = GET_FIELD_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
                            break;
                        case 3:
                            query = GET_FIELD_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
                            break;
                        case 4:
                            query = GET_FIELD_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
                            break;
                        case 5:
                            query = GET_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
                            break;
                        default:
                            query = GET_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
                    }
                    List<Map<String, Object>> rows = null;
                    if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
                        rows = getJdbcTemplate().queryForList(
                                query,
                                ldapInfo.orgHierarchy,
                                ldapInfo.orgHierarchy + "/%");
                    }
                    else if (managerId.equalsIgnoreCase("jweiner"))
                    {
                        rows = getJdbcTemplate().queryForList(
                                query,
                                "/" + ldapInfo.userName,
                                "/" + ldapInfo.userName + "/%");
                    }

                    for (Map row : rows) {
                        for (int i = 6; i > 0; i--)
                        {
                            java.math.BigDecimal value = (java.math.BigDecimal) row.get(Integer.toString(i) + "_month_ago");
                            GregorianCalendar date = new GregorianCalendar();
                            Integer month = date.get(Calendar.MONTH);
                            Integer year = date.get(Calendar.YEAR);
                            month = month+1;
                            if (month > i)
                            {
                                month = month - i + 1;
                            }
                            else
                            {
                                month = month + 12 - i + 1;
                            }
                            String label = Integer.toString(month) + "/" + Integer.toString(year);
                            MetadataBarData metadataBarData = new MetadataBarData();
                            metadataBarData.label = label;
                            if (value != null)
                            {
                                metadataBarData.value = value.longValue();
                            }
                            else
                            {
                                metadataBarData.value = 0L;
                            }
                            barDataList.add(metadataBarData);
                        }
                    }
                }
            }
        }
        resultNode.put("status", "ok");
        resultNode.set("barData", Json.toJson(barDataList));
        return resultNode;
    }
}
