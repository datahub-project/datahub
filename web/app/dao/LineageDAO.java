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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import play.Logger;
import play.Play;
import play.libs.Json;
import utils.Lineage;

public class LineageDAO extends AbstractMySQLOpenSourceDAO
{
	private final static String GET_APPLICATION_ID = "SELECT DISTINCT app_id FROM " +
			"job_execution_data_lineage WHERE abstracted_object_name = ?";

	private final static String GET_FLOW_NAME = "SELECT flow_name FROM " +
			"flow WHERE app_id = ? and flow_id = ?";

	private final static String GET_JOB = "SELECT ca.app_id, ca.app_code as cluster, je.flow_id, je.job_id, jedl.job_name, " +
			"fj.job_path, fj.job_type, jedl.flow_path, jedl.storage_type, jedl.source_target_type, jedl.operation, " +
			"max(jedl.job_exec_id) as job_exec_id, FROM_UNIXTIME(jedl.job_start_unixtime) as start_time, " +
			"FROM_UNIXTIME(jedl.job_finished_unixtime) as end_time FROM job_execution_data_lineage jedl " +
			"JOIN cfg_application ca on ca.app_id = jedl.app_id " +
			"JOIN job_execution je on jedl.app_id = je.app_id " +
			"and jedl.flow_exec_id = je.flow_exec_id and jedl.job_exec_id = je.job_exec_id " +
			"JOIN flow_job fj on je.app_id = fj.app_id and je.flow_id = fj.flow_id and je.job_id = fj.job_id " +
			"WHERE abstracted_object_name = ? and " +
			"FROM_UNIXTIME(job_finished_unixtime) >  CURRENT_DATE - INTERVAL ? DAY GROUP BY ca.app_id, je.job_id, je.flow_id";

	private final static String GET_DATA = "SELECT storage_type, operation, " +
			"abstracted_object_name, source_target_type " +
			"FROM job_execution_data_lineage WHERE app_id = ? and job_exec_id = ?";


	private final static String GET_APP_ID  = "SELECT app_id FROM cfg_application WHERE LOWER(app_code) = ?";


	private final static String GET_FLOW_JOB = "SELECT ca.app_id, ca.app_code, je.flow_id, je.job_id, " +
			"jedl.job_name, fj.job_path, fj.job_type, jedl.flow_path, jedl.storage_type, " +
			"jedl.source_target_type, jedl.operation, jedl.job_exec_id, fj.pre_jobs, fj.post_jobs, " +
			"FROM_UNIXTIME(jedl.job_start_unixtime) as start_time, " +
			"FROM_UNIXTIME(jedl.job_finished_unixtime) as end_time " +
			"FROM job_execution_data_lineage jedl " +
			"JOIN cfg_application ca on ca.app_id = jedl.app_id " +
			"JOIN job_execution je on jedl.app_id = je.app_id and " +
			"jedl.flow_exec_id = je.flow_exec_id and jedl.job_exec_id = je.job_exec_id " +
			"JOIN flow_job fj on je.app_id = fj.app_id and je.flow_id = fj.flow_id and je.job_id = fj.job_id " +
			"WHERE jedl.app_id = ? and jedl.flow_exec_id = ? and " +
			"FROM_UNIXTIME(job_finished_unixtime) >  CURRENT_DATE - INTERVAL ? DAY";

	private final static String GET_LATEST_FLOW_EXEC_ID = "SELECT max(flow_exec_id) FROM " +
			"flow_execution where app_id = ? and flow_id = ?";

	private final static String GET_FLOW_DATA_LINEAGE = "SELECT ca.app_code, jedl.job_exec_id, jedl.job_name, " +
			"jedl.storage_type, jedl.abstracted_object_name, jedl.source_target_type, jedl.record_count, " +
			"jedl.app_id, jedl.partition_type, jedl.operation, jedl.partition_start, " +
			"jedl.partition_end, jedl.full_object_name, " +
			"FROM_UNIXTIME(jedl.job_start_unixtime) as start_time, " +
			"FROM_UNIXTIME(jedl.job_finished_unixtime) as end_time FROM job_execution_data_lineage jedl " +
			"JOIN cfg_application ca on ca.app_id = jedl.app_id " +
			"WHERE jedl.app_id = ? and jedl.flow_exec_id = ? ORDER BY jedl.partition_end DESC";

    private final static String GET_ONE_LEVEL_IMPACT_DATABASES = "SELECT DISTINCT j.storage_type, " +
            "j.abstracted_object_name, d.id FROM job_execution_data_lineage j " +
            "Left join dict_dataset d on substring_index(d.urn, '://', -1) = j.abstracted_object_name " +
            "WHERE (app_id, job_exec_id) in ( " +
            "SELECT app_id, job_exec_id FROM job_execution_data_lineage " +
            "WHERE abstracted_object_name in (:pathlist) and source_target_type = 'source' and " +
            "FROM_UNIXTIME(job_finished_unixtime) >  CURRENT_DATE - INTERVAL 60 DAY ) and " +
            "abstracted_object_name not like '/tmp/%' and abstracted_object_name not like '%tmp' " +
            "and source_target_type = 'target' and " +
			"FROM_UNIXTIME(job_finished_unixtime) >  CURRENT_DATE - INTERVAL 60 DAY";


	public static JsonNode getObjectAdjacnet(String urn, int upLevel, int downLevel, int lookBackTime)
	{
		ObjectNode resultNode = Json.newObject();
		LineagePathInfo pathInfo = utils.Lineage.convertFromURN(urn);
		List<LineageNode> nodes = new ArrayList<LineageNode>();
		List<LineageEdge> edges = new ArrayList<LineageEdge>();
		Map<Long, Integer> addedJobNodes = new HashMap<Long, Integer>();
		Map<String, Integer> addedDataNodes = new HashMap<String, Integer>();
		int index = 0;
		int edgeIndex = 0;
		LineageNode node = new LineageNode();
		node.id = index;
		node._sort_list = new ArrayList<String>();
		node.node_type = "data";
		node.abstracted_path = pathInfo.filePath;
		node.storage_type = pathInfo.storageType;
		node._sort_list.add("abstracted_path");
		node._sort_list.add("storage_type");
		node._sort_list.add("urn");
		node.urn = urn;
		nodes.add(node);
		addedDataNodes.put(node.urn, node.id);
		String message = null;
		getObjectAdjacentNode(
				pathInfo,
				urn,
				upLevel,
				downLevel,
				nodes.size()-1,
				edges.size(),
				nodes,
				edges,
				addedDataNodes,
				addedJobNodes,
        lookBackTime);
		if (nodes.size() > 0)
		{
			message = "Found lineage on azkaban";
		}
		else
		{
			message = "No lineage information found for this dataset";
		}
		resultNode.set("nodes", Json.toJson(nodes));
		resultNode.set("links", Json.toJson(edges));
		resultNode.put("urn", urn);
		resultNode.put("message", message);
		return resultNode;
	}

	public static void searchInAzkaban(
			LineagePathInfo pathInfo,
			int upLevel,
			int downLevel,
			int index,
			int nodeIndex,
			int edgeIndex,
			List<LineageNode> nodes,
			List<LineageEdge> edges,
			Map<String, Integer> addedDataNodes,
			Map<Long, Integer> addedJobNodes,
      int lookBackTime)
	{
		if (upLevel < 1 && downLevel < 1)
		{
			return;
		}

		List<Map<String, Object>> rows = null;
		rows = getJdbcTemplate().queryForList(
				GET_JOB,
				pathInfo.filePath,
				lookBackTime);
		if (rows != null)
		{
			for (Map row : rows)
			{
				LineageNode node = new LineageNode();
				Object jobExecIdObject = row.get("job_exec_id");
				if (jobExecIdObject == null)
				{
					continue;
				}
				Long jobExecId = ((BigInteger)jobExecIdObject).longValue();
				Integer jobNodeId = addedJobNodes.get(jobExecId);
				if (jobNodeId != null && jobNodeId > 0)
				{
					continue;
				}
				node._sort_list = new ArrayList<String>();
				node.node_type = "script";
				node.job_type = (String)row.get("job_type");
				node.cluster = (String)row.get("cluster");
				node.job_path = (String)row.get("job_path");
				node.job_name = (String)row.get("job_name");
				node.job_start_time = row.get("start_time").toString();
				node.job_end_time = row.get("end_time").toString();
				node._sort_list.add("cluster");
				node._sort_list.add("job_path");
				node._sort_list.add("job_name");
				node._sort_list.add("job_type");
				node._sort_list.add("job_start_time");
				node._sort_list.add("job_end_time");
				String sourceType = (String)row.get("source_target_type");
				if (sourceType.equalsIgnoreCase("target") && upLevel > 0)
				{
					node.id = nodeIndex;
					nodes.add(node);
					addedJobNodes.put(jobExecId, node.id);
					LineageEdge edge = new LineageEdge();
					edge.source = nodeIndex;
					edge.target = index;
					edge.id = edgeIndex++;
					edge.label = (String) row.get("operation");
					edge.chain = (String) row.get("flow_path");
					edges.add(edge);
				}
				else if (sourceType.equalsIgnoreCase("source") && downLevel > 0)
				{
					node.id = nodeIndex;
					nodes.add(node);
					addedJobNodes.put(jobExecId, node.id);
					LineageEdge edge = new LineageEdge();
					edge.source = index;
					edge.target = nodeIndex;
					edge.id = edgeIndex++;
					edge.label = (String) row.get("operation");
					edge.chain = (String) row.get("flow_path");
					edges.add(edge);
				}
				else
				{
					continue;
				}
				int jobIndex = nodeIndex;
				nodeIndex++;
				int applicationID = (int)row.get("app_id");
				Long jobId = ((BigInteger)row.get("job_exec_id")).longValue();
				List<Map<String, Object>> relatedDataRows = null;
				relatedDataRows = getJdbcTemplate().queryForList(
						GET_DATA,
						applicationID,
						jobId);
				if (relatedDataRows != null)
				{
					for (Map relatedDataRow : relatedDataRows)
					{
						String abstractedObjectName = (String)relatedDataRow.get("abstracted_object_name");
						Integer dataNodeId = addedDataNodes.get(abstractedObjectName);
						if (abstractedObjectName.startsWith("/tmp/"))
						{
							continue;
						}
						String relatedSourceType = (String)relatedDataRow.get("source_target_type");
						if (sourceType.equalsIgnoreCase("target") && relatedSourceType.equalsIgnoreCase("source"))
						{
							LineageNode relatedNode = new LineageNode();
							relatedNode._sort_list = new ArrayList<String>();
							relatedNode.node_type = "data";
              relatedNode.abstracted_path = (String)relatedDataRow.get("abstracted_object_name");
              relatedNode.storage_type = ((String)relatedDataRow.get("storage_type")).toLowerCase();
              LineagePathInfo info = new LineagePathInfo();
							info.filePath = relatedNode.abstracted_path;
							info.storageType = relatedNode.storage_type;
							relatedNode.urn = utils.Lineage.convertToURN(info);
							relatedNode._sort_list.add("abstracted_path");
							relatedNode._sort_list.add("storage_type");
							if (dataNodeId != null && dataNodeId > 0)
							{
								relatedNode.id = dataNodeId;
							}
							else
							{
								relatedNode.id = nodeIndex;
								nodes.add(relatedNode);
								addedDataNodes.put(abstractedObjectName, relatedNode.id);
							}
							LineageEdge relatedEdge = new LineageEdge();
							relatedEdge.source = relatedNode.id;
							relatedEdge.target = jobIndex;
							relatedEdge.id = edgeIndex++;
							relatedEdge.label = (String)relatedDataRow.get("operation");
							relatedEdge.chain = "";
							edges.add(relatedEdge);
							nodeIndex++;
							if (upLevel > 1)
							{
								LineagePathInfo subPath = new LineagePathInfo();
								subPath.storageType = relatedNode.storage_type;
								subPath.filePath = relatedNode.abstracted_path;
								String subUrn = utils.Lineage.convertToURN(subPath);
								getObjectAdjacentNode(
										subPath,
										subUrn,
										upLevel-1,
										0,
										nodeIndex-1,
										edgeIndex,
										nodes,
										edges,
										addedDataNodes,
										addedJobNodes,
                    lookBackTime);
							}
							nodeIndex = nodes.size();
						}
						else if (sourceType.equalsIgnoreCase("source") && relatedSourceType.equalsIgnoreCase("target"))
						{
							LineageNode relatedNode = new LineageNode();
							relatedNode._sort_list = new ArrayList<String>();
							relatedNode.node_type = "data";
							relatedNode.abstracted_path = (String)relatedDataRow.get("abstracted_object_name");
							relatedNode.storage_type = ((String)relatedDataRow.get("storage_type")).toLowerCase();
							LineagePathInfo info = new LineagePathInfo();
							info.filePath = relatedNode.abstracted_path;
							info.storageType = relatedNode.storage_type;
							relatedNode.urn = utils.Lineage.convertToURN(info);
							relatedNode._sort_list.add("abstracted_path");
							relatedNode._sort_list.add("storage_type");
							if (dataNodeId != null && dataNodeId > 0)
							{
								relatedNode.id = dataNodeId;
							}
							else
							{
								relatedNode.id = nodeIndex;
								nodes.add(relatedNode);
								addedDataNodes.put(abstractedObjectName, relatedNode.id);
							}

							LineageEdge relatedEdge = new LineageEdge();
							relatedEdge.source = jobIndex;
							relatedEdge.target = relatedNode.id;
							relatedEdge.id = edgeIndex++;
							relatedEdge.label = (String)relatedDataRow.get("operation");
							relatedEdge.chain = "";
							edges.add(relatedEdge);
							nodeIndex++;
							if (downLevel > 1)
							{
								LineagePathInfo subPath = new LineagePathInfo();
								subPath.storageType = relatedNode.storage_type;
								subPath.filePath = relatedNode.abstracted_path;
								String subUrn = utils.Lineage.convertToURN(subPath);
								getObjectAdjacentNode(
										subPath,
										subUrn,
										0,
										downLevel-1,
										nodeIndex-1,
										edgeIndex,
										nodes,
										edges,
										addedDataNodes,
										addedJobNodes,
                    lookBackTime);
							}
							nodeIndex = nodes.size();
						}
					}

				}
			}
		}
	}

	public static void getObjectAdjacentNode(
			LineagePathInfo pathInfo,
			String urn,
			int upLevel,
			int downLevel,
			int index,
			int edgeIndex,
			List<LineageNode> nodes,
			List<LineageEdge> edges,
			Map<String, Integer> addedDataNodes,
			Map<Long, Integer> addedJobNodes,
      int lookBackTime)
	{
		if (upLevel < 1 && downLevel < 1)
		{
			return;
		}

		if (pathInfo == null || StringUtils.isBlank(pathInfo.filePath))
		{
			return;
		}

		int nodeIndex = index + 1;

		List<Integer> appIDList = getJdbcTemplate().queryForList(
				GET_APPLICATION_ID, new Object[] {pathInfo.filePath}, Integer.class);
		if (appIDList != null)
		{
			int nodeId = 0;
			for(Integer id : appIDList)
			{
				searchInAzkaban(
						pathInfo,
						upLevel,
						downLevel,
						index,
						nodeIndex,
						edgeIndex,
						nodes,
						edges,
						addedDataNodes,
						addedJobNodes,
            lookBackTime);
				break;
			}
		}
	}

	public static ObjectNode getFlowLineage(String application, String project, Long flowId)
	{
		ObjectNode resultNode = Json.newObject();
		List<LineageNode> nodes = new ArrayList<LineageNode>();
		List<LineageEdge> edges = new ArrayList<LineageEdge>();
		String flowName = null;

		Map<Long, Integer> addedJobNodes = new HashMap<Long, Integer>();
		Map<Pair, Integer> addedDataNodes = new HashMap<Pair, Integer>();

		if (StringUtils.isBlank(application) || StringUtils.isBlank(project) || (flowId <= 0))
		{
			resultNode.set("nodes", Json.toJson(nodes));
			resultNode.set("links", Json.toJson(edges));
			return resultNode;
		}

		String applicationName = application.replace(".", " ");

		int appID = 0;
		try
		{
			appID = getJdbcTemplate().queryForObject(
					GET_APP_ID,
					new Object[] {applicationName},
					Integer.class);
		}
		catch(EmptyResultDataAccessException e)
		{
			Logger.error("getFlowLineage get application id failed, application name = " + application);
			Logger.error("Exception = " + e.getMessage());
		}

		Map<Long, List<LineageNode>> nodeHash = new HashMap<Long, List<LineageNode>>();
		Map<String, List<LineageNode>> partitionedNodeHash = new HashMap<String, List<LineageNode>>();

		if (appID != 0)
		{
			try
			{
				flowName = getJdbcTemplate().queryForObject(
						GET_FLOW_NAME,
						new Object[] {appID, flowId},
						String.class);
			}
			catch(EmptyResultDataAccessException e)
			{
				Logger.error("getFlowLineage get flow name failed, application name = " + application +
						" flowId " + Long.toString(flowId));
				Logger.error("Exception = " + e.getMessage());
			}

			Long flowExecId = 0L;
			try
			{
				flowExecId = getJdbcTemplate().queryForObject(
						GET_LATEST_FLOW_EXEC_ID,
						new Object[] {appID, flowId},
						Long.class);
			}
			catch(EmptyResultDataAccessException e)
			{
				Logger.error("getFlowLineage get flow execution id failed, application name = " + application +
						" flowId " + Long.toString(flowExecId));
				Logger.error("Exception = " + e.getMessage());
			}
			List<Map<String, Object>> rows = null;
			rows = getJdbcTemplate().queryForList(
					GET_FLOW_DATA_LINEAGE,
					appID,
					flowExecId);
			if (rows != null)
			{
				for (Map row : rows) {
					Long jobExecId = ((BigInteger)row.get("job_exec_id")).longValue();
					LineageNode node = new LineageNode();
					node.abstracted_path = (String)row.get("abstracted_object_name");
					node.source_target_type = (String)row.get("source_target_type");
					node.exec_id = jobExecId;
					Object recordCountObject = row.get("record_count");
					if (recordCountObject != null)
					{
						node.record_count = ((BigInteger)recordCountObject).longValue();
					}

					node.application_id = (int)row.get("app_id");
					node.cluster = (String)row.get("app_code");
					node.partition_type = (String)row.get("partition_type");
					node.operation = (String)row.get("operation");
					node.partition_start = (String)row.get("partition_start");
					node.partition_end = (String)row.get("partition_end");
					node.full_object_name = (String)row.get("full_object_name");
					node.job_start_time = row.get("start_time").toString();
					node.job_end_time = row.get("end_time").toString();
					node.storage_type = ((String)row.get("storage_type")).toLowerCase();
					node.node_type = "data";
					node._sort_list = new ArrayList<String>();
					node._sort_list.add("cluster");
					node._sort_list.add("abstracted_path");
					node._sort_list.add("storage_type");
					node._sort_list.add("partition_type");
					node._sort_list.add("partition_start");
					node._sort_list.add("partition_end");
					node._sort_list.add("source_target_type");
					List<LineageNode> nodeList = nodeHash.get(jobExecId);
					if (nodeList != null)
					{
						nodeList.add(node);
					}
					else
					{
						nodeList = new ArrayList<LineageNode>();
						nodeList.add(node);
						nodeHash.put(jobExecId, nodeList);
					}
				}
			}

			List<LineageNode> jobNodes = new ArrayList<LineageNode>();
			List<Map<String, Object>> jobRows = null;
			jobRows = getJdbcTemplate().queryForList(
					GET_FLOW_JOB,
					appID,
					flowExecId,
					30);
			int index = 0;
			int edgeIndex = 0;
			Map<Long, LineageNode> jobNodeMap = new HashMap<Long, LineageNode>();
			List<Pair> addedEdges = new ArrayList<Pair>();
			if (rows != null)
			{
				for (Map row : jobRows) {
					Long jobExecId = ((BigInteger)row.get("job_exec_id")).longValue();
					LineageNode node = new LineageNode();
					node._sort_list = new ArrayList<String>();
					node.node_type = "script";
					node.job_type = (String)row.get("job_type");
					node.cluster = (String)row.get("app_code");
					node.job_path = (String)row.get("job_path");
					node.job_name = (String)row.get("job_name");
					node.pre_jobs = (String)row.get("pre_jobs");
					node.post_jobs = (String)row.get("post_jobs");
					node.job_id = (Long)row.get("job_id");
					node.job_start_time = row.get("start_time").toString();
					node.job_end_time = row.get("end_time").toString();
					node.exec_id = jobExecId;
					node._sort_list.add("cluster");
					node._sort_list.add("job_path");
					node._sort_list.add("job_name");
					node._sort_list.add("job_type");
					node._sort_list.add("job_start_time");
					node._sort_list.add("job_end_time");
					Integer id = addedJobNodes.get(jobExecId);
					if (id == null)
					{
						node.id = index++;
						nodes.add(node);
						jobNodeMap.put(node.job_id, node);
						jobNodes.add(node);
						addedJobNodes.put(jobExecId, node.id);
					}
					else
					{
						node.id = id;
					}

					String sourceType = (String)row.get("source_target_type");
					if (sourceType.equalsIgnoreCase("target"))
					{
						List<LineageNode> sourceNodeList = nodeHash.get(jobExecId);
						if (sourceNodeList != null && sourceNodeList.size() > 0)
						{
							for(LineageNode sourceNode : sourceNodeList)
							{
								if (sourceNode.source_target_type.equalsIgnoreCase("source"))
								{
									Pair matchedSourcePair = new ImmutablePair<>(
											sourceNode.abstracted_path,
											sourceNode.partition_end);
									Integer nodeId = addedDataNodes.get(matchedSourcePair);
									if (nodeId == null)
									{
										List<LineageNode> nodeList = partitionedNodeHash.get(sourceNode.abstracted_path);
										if (StringUtils.isBlank(sourceNode.partition_end))
										{
											Boolean bFound = false;
											if (nodeList != null)
											{
												for(LineageNode n : nodeList)
												{
													if (StringUtils.isNotBlank(n.partition_end) &&
															n.partition_end.compareTo(sourceNode.job_start_time) < 0)
													{
														sourceNode.id = n.id;
														bFound = true;
														break;
													}
												}
											}
											if (!bFound)
											{
												sourceNode.id = index++;
												nodes.add(sourceNode);
												Pair sourcePair = new ImmutablePair<>(
														sourceNode.abstracted_path,
														sourceNode.partition_end);
												addedDataNodes.put(sourcePair, sourceNode.id);
											}
										}
										else
										{
											if (nodeList == null)
											{
												nodeList = new ArrayList<LineageNode>();
											}
											nodeList.add(sourceNode);
											partitionedNodeHash.put(sourceNode.abstracted_path, nodeList);
											sourceNode.id = index++;
											nodes.add(sourceNode);
											Pair sourcePair = new ImmutablePair<>(
													sourceNode.abstracted_path,
													sourceNode.partition_end);
											addedDataNodes.put(sourcePair, sourceNode.id);
										}
									}
									else
									{
										sourceNode.id = nodeId;
									}
									LineageEdge edge = new LineageEdge();
									edge.id = edgeIndex++;
									edge.source = sourceNode.id;
									edge.target = node.id;
									if (StringUtils.isNotBlank(sourceNode.operation))
									{
										edge.label = sourceNode.operation;
									}
									else
									{
										edge.label = "load";
									}
									edge.chain = "data";
									edges.add(edge);
								}
							}
						}
					}
					else if (sourceType.equalsIgnoreCase("source"))
					{

						List<LineageNode> targetNodeList = nodeHash.get(jobExecId);
						if (targetNodeList != null && targetNodeList.size() > 0)
						{
							for(LineageNode targetNode : targetNodeList)
							{
								if (targetNode.source_target_type.equalsIgnoreCase("target"))
								{
									Pair matchedTargetPair = new ImmutablePair<>(
											targetNode.abstracted_path,
											targetNode.partition_end);
									Integer nodeId = addedDataNodes.get(matchedTargetPair);
									if (nodeId == null)
									{
										List<LineageNode> nodeList = partitionedNodeHash.get(targetNode.abstracted_path);
										if (StringUtils.isBlank(targetNode.partition_end))
										{
											Boolean bFound = false;
											if (nodeList != null)
											{
												for(LineageNode n : nodeList)
												{
													if (StringUtils.isNotBlank(n.partition_end) &&
															n.partition_end.compareTo(targetNode.job_start_time) < 0)
													{
														targetNode.id = n.id;
														bFound = true;
														break;
													}
												}
											}
											if (!bFound)
											{
												targetNode.id = index++;
												nodes.add(targetNode);
												Pair targetPair = new ImmutablePair<>(
														targetNode.abstracted_path,
														targetNode.partition_end);
												addedDataNodes.put(targetPair, targetNode.id);
											}
										}
										else
										{
											if (nodeList == null)
											{
												nodeList = new ArrayList<LineageNode>();
											}
											nodeList.add(targetNode);
											partitionedNodeHash.put(targetNode.abstracted_path, nodeList);
											targetNode.id = index++;
											nodes.add(targetNode);
											Pair targetPair = new ImmutablePair<>(
													targetNode.abstracted_path,
													targetNode.partition_end);
											addedDataNodes.put(targetPair, targetNode.id);
										}
									}
									else
									{
										targetNode.id = nodeId;
									}
									LineageEdge edge = new LineageEdge();
									edge.id = edgeIndex++;
									edge.source = node.id;
									edge.target = targetNode.id;
									if (StringUtils.isNotBlank(targetNode.operation))
									{
										edge.label = targetNode.operation;
									}
									else
									{
										edge.label = "load";
									}
									edge.chain = "data";
									edges.add(edge);
								}
							}
						}
					}
				}
				for (LineageNode node : jobNodes)
				{
					Long jobId = node.job_id;
					if (StringUtils.isNotBlank(node.pre_jobs))
					{
						String [] prevJobIds = node.pre_jobs.split(",");
						if (prevJobIds != null)
						{
							for(String jobIdString: prevJobIds)
							{
								if(StringUtils.isNotBlank(jobIdString))
								{
									Long id = Long.parseLong(jobIdString);
									LineageNode sourceNode = jobNodeMap.get(id);
									if (sourceNode != null)
									{
										Pair pair = new ImmutablePair<>(sourceNode.id, node.id);
										if (!addedEdges.contains(pair))
										{
											LineageEdge edge = new LineageEdge();
											edge.id = edgeIndex++;
											edge.source = sourceNode.id;
											edge.target = node.id;
											edge.label = "";
											edge.type = "job";
											edges.add(edge);
											addedEdges.add(pair);
										}
									}
								}
							}
						}
					}

					if (StringUtils.isNotBlank(node.post_jobs))
					{
						String [] postJobIds = node.post_jobs.split(",");
						if (postJobIds != null)
						{
							for(String jobIdString: postJobIds)
							{
								if(StringUtils.isNotBlank(jobIdString))
								{
									Long id = Long.parseLong(jobIdString);
									LineageNode targetNode = jobNodeMap.get(id);
									if (targetNode != null)
									{
										Pair pair = new ImmutablePair<>(node.id, targetNode.id);
										if (!addedEdges.contains(pair))
										{
											LineageEdge edge = new LineageEdge();
											edge.id = edgeIndex++;
											edge.source = node.id;
											edge.target = targetNode.id;
											edge.label = "";
											edge.type = "job";
											edges.add(edge);
											addedEdges.add(pair);
										}
									}
								}
							}
						}
					}
				}
			}
		}
		resultNode.set("nodes", Json.toJson(nodes));
		resultNode.set("links", Json.toJson(edges));
		resultNode.put("flowName", flowName);
		return resultNode;
	}

    public static void getImpactDatasets(
            List<String> searchUrnList,
            int level,
            List<ImpactDataset> impactDatasets)
    {
        if (searchUrnList != null && searchUrnList.size() > 0) {

            if (impactDatasets == null)
            {
                impactDatasets = new ArrayList<ImpactDataset>();
            }

            List<String> pathList = new ArrayList<String>();
            List<String> nextSearchList = new ArrayList<String>();

            for (String urn : searchUrnList)
            {
                LineagePathInfo pathInfo = Lineage.convertFromURN(urn);
                if (pathInfo != null && StringUtils.isNotBlank(pathInfo.filePath))
                {
                    if (!pathList.contains(pathInfo.filePath))
                    {
                        pathList.add(pathInfo.filePath);
                    }
                }
            }

            if (pathList != null && pathList.size() > 0)
            {
                Map<String, List> param = Collections.singletonMap("pathlist", pathList);
                NamedParameterJdbcTemplate namedParameterJdbcTemplate = new
                        NamedParameterJdbcTemplate(getJdbcTemplate().getDataSource());
                List<ImpactDataset> impactDatasetList = namedParameterJdbcTemplate.query(
                        GET_ONE_LEVEL_IMPACT_DATABASES,
                        param,
                        new ImpactDatasetRowMapper());

                if (impactDatasetList != null) {
                    for (ImpactDataset dataset : impactDatasetList) {
                        dataset.level = level;
                        if (impactDatasets.stream().filter(o -> o.urn.equals(dataset.urn)).findFirst().isPresent())
                        {
                            continue;
                        }
                        impactDatasets.add(dataset);
                        nextSearchList.add(dataset.urn);
                    }
                }
            }

            if (nextSearchList.size() > 0)
            {
                getImpactDatasets(nextSearchList, level + 1, impactDatasets);
            }
        }
    }

    public static List<ImpactDataset> getImpactDatasetsByUrn(String urn)
    {
        List<ImpactDataset> impactDatasetList = new ArrayList<ImpactDataset>();

        if (StringUtils.isNotBlank(urn))
        {
            List<String> searchUrnList = new ArrayList<String>();
            searchUrnList.add(urn);
            getImpactDatasets(searchUrnList, 1, impactDatasetList);
        }

        return impactDatasetList;
    }

}
