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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import wherehows.models.table.DashboardBarData;
import wherehows.models.table.DashboardDataset;
import wherehows.models.table.DashboardOwnerStat;
import wherehows.models.table.LdapInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import play.Logger;
import play.libs.Json;


public class DashboardDAO extends AbstractMySQLOpenSourceDAO {

    private static final DecimalFormat df2 = new DecimalFormat("##0.##");

    private final static String GET_CONFIDENTIAL_DATASETS_FILTER_PLATFORM =
        "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, d.name, "
            + "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "WHERE o.dataset_id in (SELECT DISTINCT dataset_id FROM dataset_security) "
            + "and owner_id in ( "
            + "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_OWNERSHIP_CONFIRMED_DATASETS_FILTER_PLATFORM =
        "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, d.name, "
            + "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, "
            + "GROUP_CONCAT(CASE WHEN o.confirmed_by > '' THEN o.owner_id ELSE null END "
            + "ORDER BY o.owner_id ASC SEPARATOR ',') as confirmed_owner_id "
            + "FROM dataset_owner o JOIN dict_dataset d ON d.urn like ? and o.dataset_id = d.id "
            + "WHERE o.dataset_id in "
            + "(SELECT DISTINCT dataset_id FROM dataset_owner "
            + "WHERE confirmed_by is not null and confirmed_by != '' and (is_deleted != 'Y' or is_deleted is null)) "
            + "and o.owner_id in "
            + "(SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_OWNERSHIP_DATASETS_FILTER_PLATFORM =
        "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, d.name, "
            + "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, "
            + "GROUP_CONCAT(CASE WHEN o.confirmed_by > '' THEN o.owner_id ELSE null END "
            + "ORDER BY o.owner_id ASC SEPARATOR ',') as confirmed_owner_id "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "WHERE o.owner_id in ( "
            + "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_OWNERSHIP_UNCONFIRMED_DATASETS_FILTER_PLATFORM =
        "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, d.name, "
            + "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, "
            + "GROUP_CONCAT(CASE WHEN o.confirmed_by > '' THEN o.owner_id ELSE null END "
            + "ORDER BY o.owner_id ASC SEPARATOR ',') as confirmed_owner_id "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "WHERE o.dataset_id not in (SELECT DISTINCT dataset_id FROM dataset_owner "
            + "WHERE confirmed_by is not null and confirmed_by != '' and (is_deleted != 'Y' or is_deleted is null)) "
            + "and o.owner_id in ( "
            + "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_ALL_DATASETS_BY_ID_FILTER_PLATFORM =
        "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, d.name, "
            + "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "WHERE owner_id in ( "
            + "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITH_DESCRIPTION_FILTER_PLATFORM =
        "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, d.name, "
            + "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "WHERE o.dataset_id in (SELECT DISTINCT dataset_id FROM comments) "
            + "and owner_id in ( "
            + "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITHOUT_DESCRIPTION_FILTER_PLATFORM =
        "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, d.name, "
            + "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "WHERE o.dataset_id not in (SELECT DISTINCT dataset_id FROM comments) "
            + "and owner_id in ( "
            + "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITH_COMPLIANCE_FILTER_PLATFORM =
        "SELECT SQL_CALC_FOUND_ROWS o.dataset_id, d.name, "
            + "GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "WHERE o.dataset_id in "
            + "(SELECT DISTINCT dataset_id FROM dataset_privacy_compliance WHERE compliance_purge_type = ?) "
            + "and owner_id in ( "
            + "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITH_FULL_FIELD_DESCRIPTION_FILTER_PLATFORM = "SELECT SQL_CALC_FOUND_ROWS " +
            "o.dataset_id, GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON d.urn like ? and o.dataset_id = d.id and " +
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

    private final static String GET_DATASETS_WITH_ANY_FIELD_DESCRIPTION_FILTER_PLATFORM  = "SELECT SQL_CALC_FOUND_ROWS " +
            "o.dataset_id, GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON d.urn like ? and  o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id FROM dict_dataset_field_comment ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    private final static String GET_DATASETS_WITH_NO_FIELD_DESCRIPTION_FILTER_PLATFORM  = "SELECT SQL_CALC_FOUND_ROWS " +
            "o.dataset_id, GROUP_CONCAT(o.owner_id ORDER BY o.owner_id ASC SEPARATOR ',') as owner_id, d.name FROM " +
            "dataset_owner o JOIN dict_dataset d ON d.urn like ? and  o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id not in ( SELECT DISTINCT dataset_id FROM dict_dataset_field_comment ) and " +
            "owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) " +
            "GROUP BY o.dataset_id, d.name ORDER BY d.name LIMIT ?, ?";

    // potential datasets that contains confidential field, stored in dict_pii_field
    private final static String GET_CONFIDENTIAL_INFO  = "SELECT count(distinct o.dataset_id) as count " +
            "FROM dataset_owner o JOIN dict_dataset d ON o.dataset_id = d.id and " +
            "( o.is_deleted != 'Y' or o.is_deleted is null ) " +
            "WHERE o.dataset_id in ( SELECT DISTINCT dataset_id from dict_pii_field ) and owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)";

    private final static String GET_COUNT_OF_DATASET_FILTER_PLATFORM =
        "SELECT count(DISTINCT o.dataset_id) as `count` "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "and owner_id in "
            + "(SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)";

    private final static String GET_COUNT_OF_DATASET_WITH_DESCRIPTION_FILTER_PLATFORM =
        "SELECT count(DISTINCT o.dataset_id) as `count` "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "and owner_id in "
            + "(SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "WHERE o.dataset_id in (SELECT DISTINCT dataset_id from comments)";

    private final static String GET_COUNT_OF_DATASET_WITH_CONFIDENTIAL_FILTER_PLATFORM =
        "SELECT count(DISTINCT o.dataset_id) as `count` "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "and owner_id in "
            + "(SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "WHERE o.dataset_id in (SELECT DISTINCT dataset_id from dataset_security)";

    private final static String GET_COUNT_OF_DATASET_WITH_CONFIRMED_OWNER_FILTER_PLATFORM =
        "SELECT count(DISTINCT o.dataset_id) as `count` "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and "
            + "owner_id in "
            + "(SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)"
            + "WHERE o.dataset_id in "
            + "(SELECT DISTINCT dataset_id FROM dataset_owner "
            + "WHERE confirmed_by is not null and confirmed_by != '' and (is_deleted != 'Y' or is_deleted is null))";

    private final static String GET_COUNT_OF_DATASET_WITH_COMPLIANCE_FILTER_PLATFORM =
        "SELECT count(DISTINCT o.dataset_id) as `count` "
            + "FROM dataset_owner o JOIN dict_dataset d "
            + "ON d.urn like ? and o.dataset_id = d.id and (o.is_deleted != 'Y' or o.is_deleted is null) "
            + "and owner_id in "
            + "(SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?) "
            + "WHERE o.dataset_id in (SELECT DISTINCT dataset_id from dataset_privacy_compliance)";

    private final static String GET_FIELDS_WITH_DESCRIPTION = "SELECT field_name FROM " +
            "dict_field_detail dd JOIN dict_dataset_field_comment fc ON " +
            "dd.field_id = fc.field_id and dd.dataset_id = fc.dataset_id WHERE dd.dataset_id = ?";

    private final static String GET_DATASET_LEVEL_COMMENTS = "SELECT text FROM comments WHERE dataset_id = ? LIMIT 1";

    private final static String GET_CONFIDENTIAL_FIELDS_BY_DATASET_ID =
        "SELECT classification FROM dataset_security WHERE dataset_id = ?";

    private final static String GET_COMPLIANCE_FIELDS_BY_DATASET_ID =
        "SELECT compliance_purge_entities FROM dataset_privacy_compliance where dataset_id = ?";

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
            "FROM ( " +
            "SELECT d.id, CASE WHEN c.modified is not null THEN c.modified ELSE c.created END as comments_on " +
            "FROM dict_dataset d JOIN comments c ON d.id = c.dataset_id " +
            "JOIN (SELECT dataset_id FROM dataset_owner WHERE owner_id in ( " +
            "SELECT user_id FROM dir_external_user_info WHERE org_hierarchy = ? or org_hierarchy like ?)) o " +
            "ON d.id = o.dataset_id) co";

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

    public enum Category {
        Ownership, Description, PrivacyCompliance, SecuritySpec
    }

    private static DashboardOwnerStat getUserStat(LdapInfo ldapInfo, String platform, String totalDatasetQuery,
        String qualifyingDatasetQuery) {
        final String platformPrefix = platformPrefix(platform);

        final DashboardOwnerStat ownerStat = new DashboardOwnerStat();
        ownerStat.userName = ldapInfo.userName;
        ownerStat.fullName = ldapInfo.fullName;
        ownerStat.displayName = ldapInfo.displayName;
        ownerStat.iconUrl = ldapInfo.iconUrl;
        ownerStat.managerUserId = ldapInfo.managerUserId;
        ownerStat.orgHierarchy = ldapInfo.orgHierarchy;
        ownerStat.potentialDatasets = 0L;
        ownerStat.qualifiedDatasets = 0L;

        if (StringUtils.isNotBlank(ldapInfo.orgHierarchy)) {
            // total dataset under the owner
            ownerStat.potentialDatasets =
                getJdbcTemplate().queryForObject(totalDatasetQuery, Long.class, platformPrefix,
                    ownerStat.orgHierarchy, ownerStat.orgHierarchy + "/%");

            // qualifying dataset under the owner
            ownerStat.qualifiedDatasets =
                getJdbcTemplate().queryForObject(qualifyingDatasetQuery, Long.class, platformPrefix,
                    ownerStat.orgHierarchy, ownerStat.orgHierarchy + "/%");
        }

        if (ownerStat.potentialDatasets != 0) {
            ownerStat.completeRate = df2.format(100.0 * ownerStat.qualifiedDatasets / ownerStat.potentialDatasets);
        } else {
            ownerStat.completeRate = df2.format(0.0);
        }

        return ownerStat;
    }

    private static String platformPrefix(String platform) {
        return StringUtils.isNotBlank(platform) ? platform + ":///%" : "%:///%";
    }

    public static ObjectNode getDashboardStatByUserId(String userId, String platform, Category category) {
        ObjectNode resultNode = Json.newObject();
        String totalDatasetQuery;
        String qualifyingDatasetQuery;

        switch (category) {
            case Ownership:
                totalDatasetQuery = GET_COUNT_OF_DATASET_FILTER_PLATFORM;
                qualifyingDatasetQuery = GET_COUNT_OF_DATASET_WITH_CONFIRMED_OWNER_FILTER_PLATFORM;
                break;

            case Description:
                totalDatasetQuery = GET_COUNT_OF_DATASET_FILTER_PLATFORM;
                qualifyingDatasetQuery = GET_COUNT_OF_DATASET_WITH_DESCRIPTION_FILTER_PLATFORM;
                break;

            case PrivacyCompliance:
                totalDatasetQuery = GET_COUNT_OF_DATASET_FILTER_PLATFORM;
                qualifyingDatasetQuery = GET_COUNT_OF_DATASET_WITH_COMPLIANCE_FILTER_PLATFORM;
                break;

            case SecuritySpec:
                totalDatasetQuery = GET_COUNT_OF_DATASET_FILTER_PLATFORM;
                qualifyingDatasetQuery = GET_COUNT_OF_DATASET_WITH_CONFIDENTIAL_FILTER_PLATFORM;
                break;

            default:
                resultNode.put("status", "query failure, unrecognized dashboard category.");
                return resultNode;
        }

        DashboardOwnerStat currentUser = null;
        List<DashboardOwnerStat> memberList = new ArrayList<>();

        if (StringUtils.isNotBlank(userId)) {
            List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(userId);
            if (ldapInfoList != null && ldapInfoList.size() > 0 && ldapInfoList.get(0) != null) {
                currentUser = getUserStat(ldapInfoList.get(0), platform, totalDatasetQuery, qualifyingDatasetQuery);
            }
            List<LdapInfo> members = JiraDAO.getFirstLevelLdapInfo(userId);
            if (members != null) {
                for (LdapInfo member : members) {
                    if (member != null && StringUtils.isNotBlank(member.orgHierarchy)) {
                        memberList.add(getUserStat(member, platform, totalDatasetQuery, qualifyingDatasetQuery));
                    }
                }
            }
        }
        resultNode.put("status", "ok");
        resultNode.set("currentUser", Json.toJson(currentUser));
        resultNode.set("members", Json.toJson(memberList));
        return resultNode;
    }

    private static long getPagedDashboardDatasets(String userId, String query, String platform, String option, int page,
        int size, List<Map<String, Object>> rows) {
        long count = 0;
        if (StringUtils.isNotBlank(userId)) {
            List<LdapInfo> ldapList = JiraDAO.getCurrentUserLdapInfo(userId);
            if (ldapList != null && ldapList.size() > 0 && ldapList.get(0) != null) {
                String orgHierarchy = ldapList.get(0).orgHierarchy;
                if (StringUtils.isNotBlank(orgHierarchy)) {
                    if (StringUtils.isNotBlank(option)) {
                        rows.addAll(
                            getJdbcTemplate().queryForList(query, platformPrefix(platform), option, orgHierarchy,
                                orgHierarchy + "/%", (page - 1) * size, size));
                    } else {
                        rows.addAll(getJdbcTemplate().queryForList(query, platformPrefix(platform), orgHierarchy,
                            orgHierarchy + "/%", (page - 1) * size, size));
                    }
                }

                try {
                    count = getJdbcTemplate().queryForObject("SELECT FOUND_ROWS()", Long.class);
                } catch (EmptyResultDataAccessException e) {
                    Logger.error("Exception = " + e.getMessage());
                }
            }
        }
        return count;
    }

    public static ObjectNode getPagedOwnershipDatasetsByManagerId(String managerId, String platform, int option,
        int page, int size) {

        final DataSourceTransactionManager tm = new DataSourceTransactionManager(getJdbcTemplate().getDataSource());
        final TransactionTemplate txTemplate = new TransactionTemplate(tm);

        return txTemplate.execute(new TransactionCallback<ObjectNode>() {

            public ObjectNode doInTransaction(TransactionStatus status) {
                final String datasetQuery;
                switch (option) {
                    case 1:
                        datasetQuery = GET_OWNERSHIP_CONFIRMED_DATASETS_FILTER_PLATFORM;
                        break;
                    case 2:
                        datasetQuery = GET_OWNERSHIP_UNCONFIRMED_DATASETS_FILTER_PLATFORM;
                        break;
                    case 3:
                        datasetQuery = GET_OWNERSHIP_DATASETS_FILTER_PLATFORM;
                        break;
                    default:
                        datasetQuery = GET_OWNERSHIP_DATASETS_FILTER_PLATFORM;
                }

                List<Map<String, Object>> rows = new ArrayList<>(size);
                long count = getPagedDashboardDatasets(managerId, datasetQuery, platform, null, page, size, rows);

                List<DashboardDataset> datasets = new ArrayList<>();
                for (Map row : rows) {
                    DashboardDataset dashboardDataset = new DashboardDataset();
                    dashboardDataset.datasetId = (Long) row.get("dataset_id");
                    dashboardDataset.datasetName = (String) row.get("name");
                    dashboardDataset.ownerId = (String) row.get("owner_id");
                    String confirmedOwnerId = (String) row.get("confirmed_owner_id");
                    if (StringUtils.isBlank(confirmedOwnerId) && option == 1) {
                        confirmedOwnerId = "<other team>";
                    }
                    dashboardDataset.confirmedOwnerId = confirmedOwnerId;
                    datasets.add(dashboardDataset);
                }

                ObjectNode resultNode = Json.newObject();
                resultNode.put("status", "ok");
                resultNode.put("count", count);
                resultNode.put("page", page);
                resultNode.put("itemsPerPage", size);
                resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
                resultNode.set("datasets", Json.toJson(datasets));
                return resultNode;
            }
        });
    }

    public static ObjectNode getPagedConfidentialDatasetsByManagerId(String managerId, String platform, int page,
        int size) {

        DataSourceTransactionManager tm = new DataSourceTransactionManager(getJdbcTemplate().getDataSource());
        TransactionTemplate txTemplate = new TransactionTemplate(tm);

        return txTemplate.execute(new TransactionCallback<ObjectNode>() {

            public ObjectNode doInTransaction(TransactionStatus status) {
                List<Map<String, Object>> rows = new ArrayList<>(size);
                long count =
                    getPagedDashboardDatasets(managerId, GET_CONFIDENTIAL_DATASETS_FILTER_PLATFORM, platform, null, page, size, rows);

                List<DashboardDataset> datasets = new ArrayList<>();
                for (Map row : rows) {
                    DashboardDataset dashboardDataset = new DashboardDataset();
                    dashboardDataset.datasetId = (Long) row.get("dataset_id");
                    dashboardDataset.datasetName = (String) row.get("name");
                    dashboardDataset.ownerId = (String) row.get("owner_id");
                    if (dashboardDataset.datasetId != null && dashboardDataset.datasetId > 0) {
                        dashboardDataset.fields =
                            getJdbcTemplate().queryForList(GET_CONFIDENTIAL_FIELDS_BY_DATASET_ID, String.class,
                                dashboardDataset.datasetId);
                    }
                    datasets.add(dashboardDataset);
                }

                ObjectNode resultNode = Json.newObject();
                resultNode.put("status", "ok");
                resultNode.put("count", count);
                resultNode.put("page", page);
                resultNode.put("itemsPerPage", size);
                resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
                resultNode.set("datasets", Json.toJson(datasets));
                return resultNode;
            }
        });
    }

    public static ObjectNode getPagedDescriptionDatasetsByManagerId(String managerId, String platform, int option,
        int page, int size) {

        DataSourceTransactionManager tm = new DataSourceTransactionManager(getJdbcTemplate().getDataSource());
        TransactionTemplate txTemplate = new TransactionTemplate(tm);

        return txTemplate.execute(new TransactionCallback<ObjectNode>() {

            public ObjectNode doInTransaction(TransactionStatus status) {
                Boolean isDatasetLevel = true;
                String description;

                final String datasetQuery;
                switch (option) {
                    case 1:
                        datasetQuery = GET_DATASETS_WITH_DESCRIPTION_FILTER_PLATFORM;
                        description = "has dataset level description";
                        break;
                    case 2:
                        datasetQuery = GET_DATASETS_WITHOUT_DESCRIPTION_FILTER_PLATFORM;
                        description = "no dataset level description";
                        break;
                    case 3:
                        datasetQuery = GET_DATASETS_WITH_FULL_FIELD_DESCRIPTION_FILTER_PLATFORM;
                        description = "all fields have description";
                        isDatasetLevel = false;
                        break;
                    case 4:
                        datasetQuery = GET_DATASETS_WITH_ANY_FIELD_DESCRIPTION_FILTER_PLATFORM;
                        description = "has field description";
                        isDatasetLevel = false;
                        break;
                    case 5:
                        datasetQuery = GET_DATASETS_WITH_NO_FIELD_DESCRIPTION_FILTER_PLATFORM;
                        description = "no field description";
                        isDatasetLevel = false;
                        break;
                    case 6:
                        datasetQuery = GET_ALL_DATASETS_BY_ID_FILTER_PLATFORM;
                        description = "";
                        break;
                    default:
                        datasetQuery = GET_ALL_DATASETS_BY_ID_FILTER_PLATFORM;
                        description = "";
                }

                List<Map<String, Object>> rows = new ArrayList<>(size);
                long count = getPagedDashboardDatasets(managerId, datasetQuery, platform, null, page, size, rows);

                List<DashboardDataset> datasets = new ArrayList<>();
                for (Map row : rows) {
                    DashboardDataset dashboardDataset = new DashboardDataset();
                    dashboardDataset.datasetId = (Long) row.get("dataset_id");
                    dashboardDataset.datasetName = (String) row.get("name");
                    dashboardDataset.ownerId = (String) row.get("owner_id");
                    if (dashboardDataset.datasetId != null && dashboardDataset.datasetId > 0) {
                        if (isDatasetLevel) {
                            dashboardDataset.fields =
                                getJdbcTemplate().queryForList(GET_DATASET_LEVEL_COMMENTS, String.class,
                                    dashboardDataset.datasetId);
                        } else {
                            dashboardDataset.fields =
                                getJdbcTemplate().queryForList(GET_FIELDS_WITH_DESCRIPTION, String.class,
                                    dashboardDataset.datasetId);
                        }
                    }
                    datasets.add(dashboardDataset);
                }

                ObjectNode resultNode = Json.newObject();
                resultNode.put("status", "ok");
                resultNode.put("count", count);
                resultNode.put("page", page);
                resultNode.put("description", description);
                resultNode.put("itemsPerPage", size);
                resultNode.put("isDatasetLevel", isDatasetLevel);
                resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
                resultNode.set("datasets", Json.toJson(datasets));
                return resultNode;
            }
        });
    }

    public static ObjectNode getPagedComplianceDatasetsByManagerId(String managerId, String platform, String option,
        int page, int size) {

        DataSourceTransactionManager tm = new DataSourceTransactionManager(getJdbcTemplate().getDataSource());
        TransactionTemplate txTemplate = new TransactionTemplate(tm);

        return txTemplate.execute(new TransactionCallback<ObjectNode>() {

            public ObjectNode doInTransaction(TransactionStatus status) {
                long count;
                List<Map<String, Object>> rows = new ArrayList<>(size);
                if (StringUtils.isNotBlank(option) && !(option.equalsIgnoreCase(ALL_DATASETS))) {
                    count = getPagedDashboardDatasets(managerId, GET_DATASETS_WITH_COMPLIANCE_FILTER_PLATFORM,
                        platform, option, page, size, rows);
                } else {
                    count =
                        getPagedDashboardDatasets(managerId, GET_ALL_DATASETS_BY_ID_FILTER_PLATFORM, platform, null, page, size, rows);
                }

                List<DashboardDataset> datasets = new ArrayList<>();
                for (Map row : rows) {
                    DashboardDataset dashboardDataset = new DashboardDataset();
                    dashboardDataset.datasetId = (Long) row.get("dataset_id");
                    dashboardDataset.datasetName = (String) row.get("name");
                    dashboardDataset.ownerId = (String) row.get("owner_id");
                    if (dashboardDataset.datasetId != null && dashboardDataset.datasetId > 0) {
                        dashboardDataset.fields =
                            getJdbcTemplate().queryForList(GET_COMPLIANCE_FIELDS_BY_DATASET_ID, String.class,
                                dashboardDataset.datasetId);
                    }
                    datasets.add(dashboardDataset);
                }

                ObjectNode resultNode = Json.newObject();
                resultNode.put("status", "ok");
                resultNode.put("count", count);
                resultNode.put("page", page);
                resultNode.put("itemsPerPage", size);
                resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
                resultNode.set("datasets", Json.toJson(datasets));
                return resultNode;
            }
        });
    }

    private static List<DashboardBarData> getDashboardBarData(String userId, String query) {
        Map<String, Object> result = null;
        if (StringUtils.isNotBlank(userId)) {
            List<LdapInfo> ldapInfoList = JiraDAO.getCurrentUserLdapInfo(userId);
            if (ldapInfoList != null && ldapInfoList.size() > 0 && ldapInfoList.get(0) != null) {
                String orgHierarchy = ldapInfoList.get(0).orgHierarchy;

                if (StringUtils.isNotBlank(orgHierarchy)) {
                    result = getJdbcTemplate().queryForMap(query, orgHierarchy, orgHierarchy + "/%");
                }
            }
        }

        List<DashboardBarData> barDataList = new ArrayList<>();
        if (result != null && result.size() >= 6) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(new Date());
            DateFormat df = new SimpleDateFormat("MM/yy");

            for (int i = 1; i <= 6; i++) {
                BigDecimal value = (BigDecimal) result.get(i + "_month_ago");

                DashboardBarData dashboardBarData = new DashboardBarData();
                dashboardBarData.label = df.format(cal.getTime());
                if (value != null) {
                    dashboardBarData.value = value.longValue();
                } else {
                    dashboardBarData.value = 0L;
                }
                barDataList.add(dashboardBarData);
                cal.add(Calendar.MONTH, -1);
            }
        }
        return barDataList;
    }

    public static ObjectNode getOwnershipBarChartData(String managerId) {

        List<DashboardBarData> barDataList = getDashboardBarData(managerId, GET_CONFIRMED_DATASET_BAR_CHART_DATA);

        ObjectNode resultNode = Json.newObject();
        resultNode.put("status", "ok");
        resultNode.set("barData", Json.toJson(barDataList));
        return resultNode;
    }

    public static ObjectNode getDescriptionBarChartData(String managerId, int option) {
        String query;
        switch (option) {
            case 1:
                query = GET_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
                break;
            case 2:
                query = GET_FIELD_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
                break;
            default:
                query = GET_COMMENTS_DESCRIPTION_BAR_CHART_DATA;
        }

        List<DashboardBarData> barDataList = getDashboardBarData(managerId, query);

        ObjectNode resultNode = Json.newObject();
        resultNode.put("status", "ok");
        resultNode.set("barData", Json.toJson(barDataList));
        return resultNode;
    }
}
