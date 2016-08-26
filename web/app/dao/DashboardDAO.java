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

public class DashboardDAO extends AbstractMySQLOpenSourceDAO
{

    private final static String GET_CONFIDENTIAL_DATASETS  = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in " +
            "( SELECT DISTINCT dataset_id from dict_pii_field )" +
            " and owner_id in ( " +
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

    private final static String GET_DATASETS_WITH_FULL_FIELD_DESCRIPTION_BY_ID  = "SELECT SQL_CALC_FOUND_ROWS " +
            "o.dataset_id, GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( " +
            "SELECT dataset_id FROM ( " +
            "SELECT df.dataset_id as dataset_id, COUNT(df.dataset_id) as field_count, " +
            "SUM(CASE WHEN dc.dataset_id is null THEN 0 ELSE 1 end) as comment_count " +
            "FROM dict_field_detail df LEFT JOIN dict_dataset_field_comment dc " +
            "ON df.dataset_id = dc.dataset_id and df.field_id = dc.field_id GROUP BY " +
            "df.dataset_id HAVING field_count = comment_count ) t " +
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

    private final static String GET_FIELDS_WITH_DESCRIPTION = "SELECT field_name FROM " +
            "dict_field_detail dd JOIN dict_dataset_field_comment fc ON " +
            "dd.field_id = fc.field_id and dd.dataset_id = fc.dataset_id WHERE dd.dataset_id = ?";

    private final static String GET_CONFIDENTIAL_FIELDS = "SELECT field_name " +
            "FROM dict_pii_field WHERE dataset_id = ?";


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
                        currentUser.completed =  100.0 * currentUser.confirmed / currentUser.potentialDatasets;
                    }
                    else
                    {
                        currentUser.completed = 0.0;
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
                            owner.completed = 100.0 * owner.confirmed / owner.potentialDatasets;
                        }
                        else
                        {
                            owner.completed = 0.0;
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
                        currentUser.completed = 100.0 * (currentUser.confirmed / currentUser.potentialDatasets);
                    }
                    else
                    {
                        currentUser.completed = 0.0;
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
                            owner.completed = 100.0 * (owner.confirmed / owner.potentialDatasets);
                        }
                        else
                        {
                            owner.completed = 0.0;
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
                                    datasetQuery = GET_ALL_DATASETS_BY_ID;
                                    break;
                                case 2:
                                    datasetQuery = GET_DATASETS_WITH_DESCRIPTION_BY_ID;
                                    break;
                                case 3:
                                    datasetQuery = GET_DATASETS_WITH_FULL_FIELD_DESCRIPTION_BY_ID;
                                    break;
                                case 4:
                                    datasetQuery = GET_DATASETS_WITH_ANY_FIELD_DESCRIPTION_BY_ID;
                                    break;
                                case 5:
                                    datasetQuery = GET_DATASETS_WITH_NO_FIELD_DESCRIPTION_BY_ID;
                                    break;
                                default:
                                    datasetQuery = GET_ALL_DATASETS_BY_ID;
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
                                    datasetConfidential.confidentialFieldList =
                                            getJdbcTemplate().queryForList(
                                                    GET_FIELDS_WITH_DESCRIPTION,
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
}
