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
    private final static String GET_ORG_HIERARCHY  = "SELECT display_name, org_hierarchy FROM " +
            "dir_external_user_info WHERE user_id = ? limit 1";

    private final static String GET_CONFIDENTIAL_DATASETS  = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id WHERE o.dataset_id in " +
            "( SELECT DISTINCT dataset_id from dict_pii_field )" +
            " and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_TOP_LEVEL_CONFIDENTIAL_DATASETS  = "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, " +
            "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id WHERE o.dataset_id in " +
            "( SELECT DISTINCT dataset_id from dict_pii_field )" +
            " and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_CONFIDENTIAL_INFO  = "SELECT count(distinct o.dataset_id) as count " +
            "FROM dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id from dict_pii_field ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)";

    private final static String GET_TOP_LEVEL_CONFIDENTIAL_INFO  = "SELECT count(distinct o.dataset_id) as count " +
            "FROM dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id from dict_pii_field ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)";

    private final static String GET_CONFIDENTIAL_FIELDS = "SELECT field_name " +
            "FROM dict_pii_field WHERE dataset_id = ?";


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
}
