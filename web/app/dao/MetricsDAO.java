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

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.primitives.Ints;
import models.TreeNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import play.Logger;
import play.libs.Json;
import models.Metric;

public class MetricsDAO extends AbstractMySQLOpenSourceDAO
{
	private final static String SELECT_PAGED_METRICS  = "SELECT SQL_CALC_FOUND_ROWS m.metric_id, m.metric_name, " +
            "m.metric_description, m.dashboard_name, m.metric_group, m.metric_category, m.metric_sub_category, " +
            "m.metric_level, m.metric_source_type, m.metric_source, m.metric_source_dataset_id, " +
            "m.metric_ref_id_type, m.metric_ref_id, m.metric_type, m.metric_grain, m.metric_display_factor, " +
            "m.metric_display_factor_sym, m.metric_good_direction, m.metric_formula, m.dimensions, " +
            "m.owners, m.tags, m.urn, m.metric_url, m.wiki_url, m.scm_url, IFNULL(w.id,0) as watch_id " +
			"FROM dict_business_metric2 m " +
      		"LEFT JOIN watch w ON (m.metric_id = w.item_id AND w.item_type = 'metric' AND w.user_id = ?) " +
      		"ORDER BY metric_name LIMIT ?, ?";

	private final static String SELECT_PAGED_METRICS_BY_DASHBOARD_NAME =  "SELECT SQL_CALC_FOUND_ROWS " +
			"m.metric_id, m.metric_name, m.metric_description, m.dashboard_name, m.metric_group, " +
            "m.metric_category, m.metric_sub_category, m.metric_level, m.metric_source_type, m.metric_source, " +
            "m.metric_source_dataset_id, m.metric_ref_id_type, m.metric_ref_id, m.metric_type, m.metric_grain, " +
            "m.metric_display_factor, m.metric_display_factor_sym, m.metric_good_direction, " +
            "m.metric_formula, m.dimensions, m.owners, m.tags, m.urn, m.metric_url, m.wiki_url, m.scm_url, " +
            "IFNULL(w.id,0) as watch_id " +
			"FROM dict_business_metric2 m " +
      		"LEFT JOIN watch w ON (m.metric_id = w.item_id AND w.item_type = 'metric' AND w.user_id = ?) " +
      		"WHERE dashboard_name $value ORDER BY m.metric_name limit ?, ?";

	private final static String SELECT_PAGED_METRICS_BY_DASHBOARD_AND_GROUP = "SELECT SQL_CALC_FOUND_ROWS " +
            "m.metric_id, m.metric_name, m.metric_description, m.dashboard_name, m.metric_group, " +
            "m.metric_category, m.metric_sub_category, m.metric_level, m.metric_source_type, m.metric_source, " +
            "m.metric_source_dataset_id, m.metric_ref_id_type, m.metric_ref_id, m.metric_type, m.metric_grain, " +
            "m.metric_display_factor, m.metric_display_factor_sym, m.metric_good_direction, " +
            "m.metric_formula, m.dimensions, m.owners, m.tags, m.urn, m.metric_url, m.wiki_url, m.scm_url, " +
            "IFNULL(w.id,0) as watch_id  " +
			"FROM dict_business_metric2 m " +
      		"LEFT JOIN watch w ON (m.metric_id = w.item_id AND w.item_type = 'metric' AND w.user_id = ?) " +
      		"WHERE m.dashboard_name $dashboard and m.metric_group $group " +
			"ORDER BY metric_name limit ?, ?";

	private final static String GET_METRIC_BY_ID = "SELECT m.metric_id, m.metric_name, " +
			"m.metric_description, m.dashboard_name, m.metric_group, m.metric_category, m.metric_sub_category, " +
            "m.metric_level, m.metric_source_type, m.metric_source, m.metric_source_dataset_id, " +
            "m.metric_ref_id_type, m.metric_ref_id, m.metric_type, m.metric_grain, m.metric_display_factor, " +
            "m.metric_display_factor_sym, m.metric_good_direction, m.metric_formula, m.dimensions, " +
            "m.owners, m.tags, m.urn, m.metric_url, m.wiki_url, m.scm_url, " +
      		"IFNULL(w.id, 0) as watch_id " +
			"FROM dict_business_metric2 m " +
      		"LEFT JOIN watch w ON (m.metric_id = w.item_id AND w.item_type = 'metric' AND w.user_id = ?) " +
      		"WHERE m.metric_id = ?";

  	private final static String GET_WATCHED_METRIC_ID = "SELECT id FROM watch " +
      		"WHERE user_id = ? and item_id = ? and item_type = 'metric'";

  	private final static String WATCH_METRIC = "INSERT INTO watch " +
      		"(user_id, item_id, urn, item_type, notification_type, created) VALUES(?, ?, NULL, 'metric', ?, NOW())";

  	private final static String UNWATCH_METRIC = "DELETE FROM watch WHERE id = ?";

  	private final static String GET_USER_ID = "SELECT id FROM users WHERE username = ?";

	private final static String UPDATE_METRIC = "UPDATE dict_business_metric2 SET $SET_CLAUSE WHERE metric_id = ?";

    private final static String GET_METRIC_TREE_DASHBOARD_NODES = "SELECT DISTINCT " +
            "COALESCE(dashboard_name, '(Other)') FROM dict_business_metric2 order by 1";

    private final static String GET_METRIC_TREE_OTHER_GROUP_NODES = "SELECT DISTINCT " +
            "COALESCE(metric_group, '(Other)') FROM dict_business_metric2 WHERE dashboard_name is null order by 1";

    private final static String GET_METRIC_TREE_GROUP_NODES = "SELECT DISTINCT " +
            "COALESCE(metric_group, '(Other)') FROM dict_business_metric2 WHERE dashboard_name = ? order by 1";

    private final static String GET_METRIC_TREE_NODES = "SELECT DISTINCT metric_category, " +
            "COALESCE(metric_name, '(Other)') as metric_name, metric_id " +
            "FROM dict_business_metric2 WHERE dashboard_name = ? and metric_group = ? order by 1";

    private final static String GET_METRIC_TREE_NODES_NO_DASHBOARD = "SELECT DISTINCT metric_category, " +
            "COALESCE(metric_name, '(Other)') as metric_name, metric_id " +
            "FROM dict_business_metric2 WHERE dashboard_name is null and metric_group = ? order by 1";

    private final static String GET_METRIC_TREE_NODES_NO_GROUP = "SELECT DISTINCT metric_category, " +
            "COALESCE(metric_name, '(Other)') as metric_name, metric_id " +
            "FROM dict_business_metric2 WHERE dashboard_name = ? and metric_group is null order by 1";

    private final static String GET_METRIC_TREE_NODES_NO_DASHBOARD_AND_GROUP = "SELECT DISTINCT metric_category, " +
            "COALESCE(metric_name, '(Other)') as metric_name, metric_id " +
            "FROM dict_business_metric2 WHERE dashboard_name is null and metric_group is null order by 1";

    public final static String GET_METRIC_AUTO_COMPLETE_LIST = "SELECT DISTINCT metric_name " +
            "FROM dict_business_metric2 WHERE metric_name is not null and metric_name != '' ORDER by 1";

    public static JsonNode getMetricDashboardNodes()
    {
        List<String> dashboardList = getJdbcTemplate().queryForList(GET_METRIC_TREE_DASHBOARD_NODES, String.class);
        List<TreeNode> nodes = new ArrayList<TreeNode>();
        if (dashboardList != null && dashboardList.size() > 0)
        {
            for (String dashboard : dashboardList)
            {
                TreeNode node = new TreeNode();
                node.folder = true;
                node.lazy = true;
                node.title = dashboard;
                node.level = 1;
                nodes.add(node);
            }
        }
        return Json.toJson(nodes);
    }

    public static JsonNode getMetricGroupNodes(String dashboard)
    {
        List<TreeNode> nodes = new ArrayList<TreeNode>();
        if (StringUtils.isBlank(dashboard))
        {
            return Json.toJson(nodes);
        }
        List<String> groupList = null;
        if (dashboard.equalsIgnoreCase("(Other)"))
        {
            groupList = getJdbcTemplate().queryForList(GET_METRIC_TREE_OTHER_GROUP_NODES, String.class);
        }
        else
        {
            groupList = getJdbcTemplate().queryForList(GET_METRIC_TREE_GROUP_NODES, String.class, dashboard);
        }

        if (groupList != null && groupList.size() > 0)
        {
            for (String group : groupList)
            {
                TreeNode node = new TreeNode();
                node.folder = true;
                node.lazy = true;
                node.title = group;
                node.parent = dashboard;
                node.level = 2;
                nodes.add(node);
            }
        }
        return Json.toJson(nodes);
    }

    public static JsonNode getMetricNodes(String dashboard, String group)
    {
        List<TreeNode> treeNodes = new ArrayList<TreeNode>();
        if (StringUtils.isBlank(dashboard) || StringUtils.isBlank(group))
        {
            return Json.toJson(treeNodes);
        }
        List<Map<String, Object>> rows = null;
        if (dashboard.equalsIgnoreCase("(Other)"))
        {
            if (group.equalsIgnoreCase("(Other)"))
            {
                rows = getJdbcTemplate().queryForList(GET_METRIC_TREE_NODES_NO_DASHBOARD_AND_GROUP);
            }
            else
            {
                rows = getJdbcTemplate().queryForList(GET_METRIC_TREE_NODES_NO_DASHBOARD, group);
            }

        }
        else
        {
            if (group.equalsIgnoreCase("(Other)"))
            {
                rows = getJdbcTemplate().queryForList(GET_METRIC_TREE_NODES_NO_GROUP, dashboard);
            }
            else
            {
                rows = getJdbcTemplate().queryForList(GET_METRIC_TREE_NODES, dashboard, group);
            }
        }

        if (rows != null)
        {
            for (Map row : rows)
            {
                String category = (String)row.get(MetricRowMapper.METRIC_CATEGORY_COLUMN);
                String name = (String)row.get(MetricRowMapper.METRIC_NAME_COLUMN);
                String title = "";
                if (StringUtils.isBlank(category))
                {
                    title = name;
                }
                else
                {
                    title = "{" + category + "} " + name;
                }

                TreeNode treeNode = new TreeNode();
                treeNode.folder = false;
                treeNode.lazy = false;
                treeNode.title = title;
                treeNode.parent = group;
                treeNode.level = 3;
                treeNode.id = new Long((Integer)row.get(MetricRowMapper.METRIC_ID_COLUMN));
                treeNodes.add(treeNode);
            }
        }
        return Json.toJson(treeNodes);
    }

    public static ObjectNode getPagedMetrics(
			String dashboardName,
			String group,
			Integer page,
			Integer size,
			String user)
	{
    	Integer userId = UserDAO.getUserIDByUserName(user);

		final JdbcTemplate jdbcTemplate = getJdbcTemplate();
		javax.sql.DataSource ds = jdbcTemplate.getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);

		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		ObjectNode result;
    	final Integer id = userId;
		result = txTemplate.execute(new TransactionCallback<ObjectNode>()
		{
			public ObjectNode doInTransaction(TransactionStatus status)
			{
				String query = null;
				if (StringUtils.isBlank(dashboardName))
				{
					query = SELECT_PAGED_METRICS;
				}
				else if (StringUtils.isBlank(group))
				{
					query = SELECT_PAGED_METRICS_BY_DASHBOARD_NAME;
					if (dashboardName.equals("(Other)"))
					{
						query = query.replace("$value", "is null");
					}
					else
					{
						query = query.replace("$value", "= '" + dashboardName + "'");
					}
				}
				else
				{
					query = SELECT_PAGED_METRICS_BY_DASHBOARD_AND_GROUP;
					if (dashboardName.equals("(Other)"))
					{
						query = query.replace("$dashboard", "is null");
					}
					else
					{
						query = query.replace("$dashboard", "= '" + dashboardName + "'");
					}
					if (group.equals("(Other)"))
					{
						query = query.replace("$group", "is null");
					}
					else
					{
						query = query.replace("$group", "= '" + group + "'");
					}
				}
				List<Metric> pagedMetrics = jdbcTemplate.query(
                        query,
                        new MetricRowMapper(),
                        id,
                        (page - 1) * size, size);
				long count = 0;
				try {
					count = jdbcTemplate.queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				}
				catch(EmptyResultDataAccessException e)
				{
					Logger.error("Exception = " + e.getMessage());
				}

				ObjectNode resultNode = Json.newObject();
				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int)Math.ceil(count/((double)size)));
				resultNode.set("metrics", Json.toJson(pagedMetrics));

				return resultNode;
			}
		});

		return result;
	}

	public static Metric getMetricByID(int id, String user)
	{
    	Integer userId = UserDAO.getUserIDByUserName(user);
		Metric metric = null;
		try
		{
			metric = (Metric)getJdbcTemplate().queryForObject(
					GET_METRIC_BY_ID,
					new MetricRowMapper(),
          			userId,
					id);
		}
		catch(EmptyResultDataAccessException e)
		{
			Logger.error("Metric getMetricByID failed, id = " + id);
			Logger.error("Exception = " + e.getMessage());
		}

		return metric;
	}

  	public static String watchMetric(int metricId, Map<String, String[]> params, String user)
  	{
	  	String message = "Internal error";
	  	if (params == null || params.size() == 0)
	  	{
			return "Empty post body";
	  	}

	  	String type = "metric";

	  	String notificationType = "";
	  	if (params.containsKey("notification_type")) {
		  	String[] notificationTypeArray = params.get("notification_type");
		  	if (notificationTypeArray != null && notificationTypeArray.length > 0)
		  	{
			  	notificationType = notificationTypeArray[0];
		  	}
	  	}
	  	if (StringUtils.isBlank(notificationType))
	  	{
		  	return "notification_type is missing";
	  	}

	  	Integer userId = UserDAO.getUserIDByUserName(user);
	  	if (userId != null && userId !=0)
	  	{
		  	List<Map<String, Object>> rows = null;
		  	rows = getJdbcTemplate().queryForList(GET_WATCHED_METRIC_ID, userId, metricId);
		  	if (rows != null && rows.size() > 0)
		  	{
			  	message = "watch item is already exist";
		  	}
		  	else
		  	{
			  	int row = getJdbcTemplate().update(WATCH_METRIC, userId, metricId, notificationType);
			  	if (row > 0)
			  	{
				  	message = "";
			  	}
		  	}
	  	}
	  	else
	  	{
		  	message = "User not found";
	  	}

	  	return message;
  	}

  	public static boolean unwatch(int id)
  	{
    	boolean result = false;
    	int row = getJdbcTemplate().update(UNWATCH_METRIC, id);
    	if (row > 0)
    	{
      		result = true;
    	}
    	return result;
  	}

    public static String updateMetricValues(int id, Map<String, String[]> params)
    {
        String message = "Internal error";
        if (params == null || params.size() == 0)
        {
            return "Empty post body";
        }

        boolean needAnd = false;
        String setClause = "";
        String description = "";
        String[] descArray = null;
        List<Object> args = new ArrayList<Object>();
        List<Integer> argTypes = new ArrayList<Integer>();

        if (params.containsKey(MetricRowMapper.METRIC_DESCRIPTION_COLUMN)) {
            descArray = params.get(MetricRowMapper.METRIC_DESCRIPTION_COLUMN);
        }
        else if (params.containsKey(MetricRowMapper.METRIC_MODULE_DESCRIPTION)) {
            descArray = params.get(MetricRowMapper.METRIC_MODULE_DESCRIPTION);
        }

        if (descArray != null && descArray.length > 0)
        {
            description = descArray[0];
            setClause += MetricRowMapper.METRIC_DESCRIPTION_COLUMN + " = ? ";
            needAnd = true;
            args.add(description);
            argTypes.add(Types.VARCHAR);
        }

        String dashboard = "";
        String[] dashboardArray = null;
        if (params.containsKey(MetricRowMapper.METRIC_DASHBOARD_NAME_COLUMN))
        {
            dashboardArray = params.get(MetricRowMapper.METRIC_DASHBOARD_NAME_COLUMN);
        }
        else if (params.containsKey(MetricRowMapper.METRIC_MODULE_DASHBOARD_NAME))
        {
            dashboardArray = params.get(MetricRowMapper.METRIC_MODULE_DASHBOARD_NAME);
        }

        if (dashboardArray != null && dashboardArray.length > 0)
        {
            dashboard = dashboardArray[0];
            if (needAnd)
            {
                setClause += ", ";
            }
            setClause += MetricRowMapper.METRIC_DASHBOARD_NAME_COLUMN + " = ? ";
            needAnd = true;
            args.add(dashboard);
            argTypes.add(Types.VARCHAR);
        }

        String type = "";
        String[] typeArray = null;
        if (params.containsKey(MetricRowMapper.METRIC_SOURCE_TYPE_COLUMN))
        {
            typeArray = params.get(MetricRowMapper.METRIC_SOURCE_TYPE_COLUMN);
        }
        else if (params.containsKey(MetricRowMapper.METRIC_MODULE_SOURCE_TYPE))
        {
            typeArray = params.get(MetricRowMapper.METRIC_MODULE_SOURCE_TYPE);
        }

        if (typeArray != null && typeArray.length > 0)
        {
            type = typeArray[0];
            if (needAnd)
            {
                setClause += ", ";
            }
            setClause += MetricRowMapper.METRIC_SOURCE_TYPE_COLUMN + " = ? ";
            needAnd = true;
            args.add(type);
            argTypes.add(Types.VARCHAR);
        }

        String grain = "";
        String[] grainArray = null;

        if (params.containsKey(MetricRowMapper.METRIC_GRAIN_COLUMN))
        {
            grainArray = params.get(MetricRowMapper.METRIC_GRAIN_COLUMN);
        }
        else if (params.containsKey(MetricRowMapper.METRIC_MODULE_GRAIN))
        {
            grainArray = params.get(MetricRowMapper.METRIC_MODULE_GRAIN);
        }

        if (grainArray != null && grainArray.length > 0)
        {
            grain = grainArray[0];
            if (needAnd)
            {
                setClause += ", ";
            }
            setClause += MetricRowMapper.METRIC_GRAIN_COLUMN + " = ? ";
            needAnd = true;
            args.add(grain);
            argTypes.add(Types.VARCHAR);
        }

        String formula = "";
        String[] formulaArray = null;
        if (params.containsKey(MetricRowMapper.METRIC_FORMULA_COLUMN))
        {
            formulaArray = params.get(MetricRowMapper.METRIC_FORMULA_COLUMN);
        }
        else if (params.containsKey(MetricRowMapper.METRIC_MODULE_FORMULA))
        {
            formulaArray = params.get(MetricRowMapper.METRIC_MODULE_FORMULA);
        }

        if (formulaArray != null && formulaArray.length > 0)
        {
            formula = formulaArray[0];
            if (needAnd)
            {
                setClause += ", ";
            }
            setClause += MetricRowMapper.METRIC_FORMULA_COLUMN + " = ? ";
            needAnd = true;
            args.add(formula);
            argTypes.add(Types.VARCHAR);
        }

        String displayFactorString = "";
        Double displayFactor = 0.0;
        String[] displayFactorArray = null;
        if (params.containsKey(MetricRowMapper.METRIC_DISPLAY_FACTOR_COLUMN))
        {
            displayFactorArray = params.get(MetricRowMapper.METRIC_DISPLAY_FACTOR_COLUMN);
        }
        else if (params.containsKey(MetricRowMapper.METRIC_MODULE_DISPLAY_FACTOR))
        {
            displayFactorArray = params.get(MetricRowMapper.METRIC_MODULE_DISPLAY_FACTOR);
        }

        if (displayFactorArray != null && displayFactorArray.length > 0)
        {
            displayFactorString = displayFactorArray[0];
            try
            {
                displayFactor = Double.parseDouble(displayFactorString);
                if (needAnd)
                {
                    setClause += ", ";
                }
                setClause += MetricRowMapper.METRIC_DISPLAY_FACTOR_COLUMN + " = ? ";
                needAnd = true;
                args.add(displayFactor);
                argTypes.add(Types.DECIMAL);
            }
            catch(NumberFormatException e)
            {
                Logger.error("MetricDAO updateMetricValues wrong page parameter. Error message: " +
                        e.getMessage());
                displayFactor = 0.0;
            }
        }

        String displayFactorSym = "";
        String[] factorSymArray = null;
        if (params.containsKey(MetricRowMapper.METRIC_DISPLAY_FACTOR_SYM_COLUMN))
        {
            factorSymArray = params.get(MetricRowMapper.METRIC_DISPLAY_FACTOR_SYM_COLUMN);
        }
        else if (params.containsKey(MetricRowMapper.METRIC_MODULE_DISPLAY_FACTOR_SYM))
        {
            factorSymArray = params.get(MetricRowMapper.METRIC_MODULE_DISPLAY_FACTOR_SYM);
        }

        if (factorSymArray != null && factorSymArray.length > 0)
        {
            displayFactorSym = factorSymArray[0];
            if (needAnd)
            {
                setClause += ", ";
            }
            setClause += MetricRowMapper.METRIC_DISPLAY_FACTOR_SYM_COLUMN + " = ? ";
            needAnd = true;
            args.add(displayFactorSym);
            argTypes.add(Types.VARCHAR);
        }

        String groupSkString = "";
        Integer groupSk = 0;
        String[] groupSkArray = null;
        if (params.containsKey(MetricRowMapper.METRIC_SUB_CATEGORY_COLUMN))
        {
            groupSkArray = params.get(MetricRowMapper.METRIC_SUB_CATEGORY_COLUMN);
        }
        else if (params.containsKey(MetricRowMapper.METRIC_MODULE_SUB_CATEGORY))
        {
            groupSkArray = params.get(MetricRowMapper.METRIC_MODULE_SUB_CATEGORY);
        }

        if (groupSkArray != null && groupSkArray.length > 0)
        {
            groupSkString = groupSkArray[0];
            try
            {
                groupSk = Integer.parseInt(groupSkString);
                if (needAnd)
                {
                    setClause += ", ";
                }
                setClause += MetricRowMapper.METRIC_SUB_CATEGORY_COLUMN + " = ? ";
                needAnd = true;
                args.add(groupSk);
                argTypes.add(Types.INTEGER);
            }
            catch(NumberFormatException e)
            {
                Logger.error("MetricDAO updateMetricValues wrong page parameter. Error message: " +
                        e.getMessage());
                groupSk = 0;
            }
        }

        String metricSource = "";
        String[] metricSourceArray = null;
        if (params.containsKey(MetricRowMapper.METRIC_SOURCE_COLUMN))
        {
            metricSourceArray = params.get(MetricRowMapper.METRIC_SOURCE_COLUMN);
        }
        else if (params.containsKey(MetricRowMapper.METRIC_MODULE_SOURCE))
        {
            metricSourceArray = params.get(MetricRowMapper.METRIC_MODULE_SOURCE);
        }

        if (metricSourceArray != null && metricSourceArray.length > 0)
        {
            metricSource = metricSourceArray[0];
            if (needAnd)
            {
                setClause += ", ";
            }
            setClause += MetricRowMapper.METRIC_SOURCE_COLUMN + " = ? ";
            needAnd = true;
            args.add(metricSource);
            argTypes.add(Types.VARCHAR);
        }

        if (StringUtils.isNotBlank(setClause))
        {
            args.add(id);
            argTypes.add(Types.SMALLINT);
            int row = getJdbcTemplate().update(UPDATE_METRIC.replace("$SET_CLAUSE", setClause), args.toArray(), Ints.toArray(argTypes));
            if (row > 0)
            {
                message = "";
            }
        }
        else
        {
            message = "Wrong post body";
        }
        return message;
    }

}
