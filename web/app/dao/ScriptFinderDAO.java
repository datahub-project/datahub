package dao;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.primitives.Ints;
import models.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import play.Logger;
import play.libs.Json;

public class ScriptFinderDAO extends AbstractMySQLOpenSourceDAO{


	private final static String GET_SCRIPT_TYPE = "SELECT DISTINCT script_type FROM job_attempt_source_code";

	private final static String GET_PAGED_SCRIPTS = "SELECT SQL_CALC_FOUND_ROWS * " +
			"FROM (SELECT " +
			"script_name, script_url, script_path, script_type, chain_name, job_name, job_id, app_id, " +
			"GROUP_CONCAT(committer_ldap ORDER BY commit_time DESC SEPARATOR ', ') as committer_names, " +
			"GROUP_CONCAT(committer_email ORDER BY commit_time DESC SEPARATOR ', ') as committer_emails " +
			"FROM job_execution_script $WHERE_CLAUSE " +
			"GROUP BY script_url, script_path, script_name, script_type, chain_name, job_name, job_id, app_id ) a " +
			"ORDER BY a.script_name " +
			"LIMIT :index, :size";

	private final static String GET_PAGED_SCRIPT_RUNTIME = "SELECT SQL_CALC_FOUND_ROWS " +
			"FROM_UNIXTIME(jedl.job_start_unixtime) as job_started, " +
			"(jedl.job_finished_unixtime - jedl.job_start_unixtime) as elapsed_time, " +
			"jedl.flow_path, jedl.job_name FROM job_execution_data_lineage jedl " +
			"JOIN job_execution je on je.app_id = jedl.app_id and " +
			"je.flow_exec_id = jedl.flow_exec_id and je.job_exec_id = jedl.job_exec_id " +
			"WHERE (je.app_id = ? and je.job_id = ?) " +
			"GROUP BY job_started, elapsed_time, jedl.flow_path, jedl.job_name " +
			"ORDER BY jedl.job_start_unixtime DESC LIMIT 10";

	private final static String GET_LATEST_5_JOB_EXEC_IDS = "SELECT job_exec_id " +
			"FROM job_execution WHERE app_id = ? and job_id = ? ORDER BY start_time DESC limit 5;";

	private final static String GET_SCRIPT_LINEAGE = "SELECT * FROM " +
			"job_execution_data_lineage WHERE app_id = ? AND job_exec_id = ?";
	
	public static List<String> getAllScriptTypes()
	{
		return getJdbcTemplate().queryForList(GET_SCRIPT_TYPE, String.class);
	}

	public static ObjectNode getPagedScripts(JsonNode filterOpt, int page, int size)
	{
		ObjectNode result = Json.newObject();

		javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
		TransactionTemplate txTemplate = new TransactionTemplate(tm);
		String scriptName = null;
		String scriptPath = null;
		String scriptType = null;
		String chainName = null;
		String jobName = null;
		String committerName = null;
		String committerEmail = null;
		if (filterOpt != null && (filterOpt.isContainerNode())) {
			if (filterOpt.has("scriptName")) {
				scriptName = filterOpt.get("scriptName").asText();
			}
			if (filterOpt.has("scriptPath")) {
				scriptPath = filterOpt.get("scriptPath").asText();
			}
			if (filterOpt.has("scriptType")) {
				scriptType = filterOpt.get("scriptType").asText();
			}
			if (filterOpt.has("chainName")) {
				chainName = filterOpt.get("chainName").asText();
			}
			if (filterOpt.has("jobName")) {
				jobName = filterOpt.get("jobName").asText();
			}
			if (filterOpt.has("committerName")) {
				committerName = filterOpt.get("committerName").asText();
			}
			if (filterOpt.has("committerEmail")) {
				committerEmail = filterOpt.get("committerEmail").asText();
			}
		}

		final String finalScriptName = scriptName;
		final String finalScriptPath = scriptPath;
		final String finalScriptType = scriptType;
		final String finalChainName = chainName;
		final String finalJobName = jobName;
		final String finalCommitterName = committerName;
		result = txTemplate.execute(new TransactionCallback<ObjectNode>() {
			public ObjectNode doInTransaction(TransactionStatus status) {

				List<Map<String, Object>> rows = null;
				String whereClause = "";
				boolean needAnd = false;
				Map<String,Object> params = new HashMap<String,Object>();
				if (StringUtils.isNotBlank(finalScriptName))
				{
					if (StringUtils.isBlank(whereClause))
					{
						whereClause = " WHERE ";
					}
					if (needAnd)
					{
						whereClause += " AND ";
					}
					whereClause += " script_name like :scriptname ";
					needAnd = true;
					params.put("scriptname", "%" + finalScriptName + "%");
				}
				if (StringUtils.isNotBlank(finalScriptPath))
				{
					if (StringUtils.isBlank(whereClause))
					{
						whereClause = " WHERE ";
					}
					if (needAnd)
					{
						whereClause += " AND ";
					}
					whereClause += " script_path like :scriptpath ";
					needAnd = true;
					params.put("scriptpath", "%" + finalScriptPath + "%");
				}
				if (StringUtils.isNotBlank(finalScriptType))
				{
					if (StringUtils.isBlank(whereClause))
					{
						whereClause = " WHERE ";
					}
					if (needAnd)
					{
						whereClause += " AND ";
					}
					whereClause += " script_type like :scripttype ";
					needAnd = true;
					params.put("scripttype", "%" + finalScriptType + "%");
				}
				if (StringUtils.isNotBlank(finalChainName))
				{
					if (StringUtils.isBlank(whereClause))
					{
						whereClause = " WHERE ";
					}
					if (needAnd)
					{
						whereClause += " AND ";
					}
					whereClause += " chain_name like :chainname ";
					needAnd = true;
					params.put("chainname", "%" + finalChainName + "%");
				}
				if (StringUtils.isNotBlank(finalJobName))
				{
					if (StringUtils.isBlank(whereClause))
					{
						whereClause = " WHERE ";
					}
					if (needAnd)
					{
						whereClause += " AND ";
					}
					whereClause += " job_name like :jobname ";
					needAnd = true;
					params.put("jobname", "%" + finalJobName + "%");
				}
				if (StringUtils.isNotBlank(finalCommitterName))
				{
					if (StringUtils.isBlank(whereClause))
					{
						whereClause = " WHERE ";
					}
					if (needAnd)
					{
						whereClause += " AND ";
					}
					whereClause += " ( committer_ldap like :committername or committer_name like :committername )";
					needAnd = true;
					params.put("committername", "%" + finalCommitterName + "%");
				}
				String query = GET_PAGED_SCRIPTS.replace("$WHERE_CLAUSE", whereClause);
				params.put("index", (page - 1) * size);
				params.put("size", size);
				NamedParameterJdbcTemplate namedParameterJdbcTemplate = new
						NamedParameterJdbcTemplate(getJdbcTemplate().getDataSource());
				rows = namedParameterJdbcTemplate.queryForList(
						query,
						params);
				long count = 0;
				try {
					count = getJdbcTemplate().queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				} catch (EmptyResultDataAccessException e) {
					Logger.error("Exception = " + e.getMessage());
				}

				List<ScriptInfo> pagedScripts = new ArrayList<ScriptInfo>();
				for (Map row : rows)
				{
					int applicationID = (Integer)row.get(ScriptInfoRowMapper.APPLICATION_ID_COLUMN);
					int jobID = (Integer)row.get(ScriptInfoRowMapper.JOB_ID_COLUMN);
					String scriptUrl = (String)row.get(ScriptInfoRowMapper.SCRIPT_URL_COLUMN);
					String scriptPath = (String)row.get(ScriptInfoRowMapper.SCRIPT_PATH_COLUMN);
					String scriptType = (String)row.get(ScriptInfoRowMapper.SCRIPT_TYPE_COLUMN);
					String chainName = (String)row.get(ScriptInfoRowMapper.CHAIN_NAME_COLUMN);
					String jobName = (String)row.get(ScriptInfoRowMapper.JOB_NAME_COLUMN);
					String scriptName = (String)row.get(ScriptInfoRowMapper.SCRIPT_NAME_COLUMN);
					String committerName = (String)row.get(ScriptInfoRowMapper.COMMITTER_NAMES_COLUMN);
					String committerEmail = (String)row.get(ScriptInfoRowMapper.COMMITTER_EMAILS_COLUMN);
					ScriptInfo scriptInfo = new ScriptInfo();
					scriptInfo.applicationID = applicationID;
					scriptInfo.jobID = jobID;
					scriptInfo.scriptUrl = scriptUrl;
					scriptInfo.scriptPath = scriptPath;
					scriptInfo.scriptType = scriptType;
					scriptInfo.scriptName = scriptName;
					scriptInfo.chainName = chainName;
					scriptInfo.jobName = jobName;
					scriptInfo.committerName = committerName;
					scriptInfo.committerEmail = committerEmail;
					pagedScripts.add(scriptInfo);
				}

				ObjectNode resultNode = Json.newObject();
				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
				resultNode.set("scripts", Json.toJson(pagedScripts));

				return resultNode;
			}
		});

		return result;
	}

	public static List<ScriptRuntime> getPagedScriptRuntime(
			int applicationID,
			int jobID)
	{
		return getJdbcTemplate().query(
				GET_PAGED_SCRIPT_RUNTIME,
				new ScriptRuntimeRowMapper(),
				applicationID,
				jobID);
	}

	public static List<ScriptLineage> getScriptLineage(
			int applicationID,
			int jobID)
	{
		List<Long> jobExecIdList = getJdbcTemplate().queryForList(
				GET_LATEST_5_JOB_EXEC_IDS,
				Long.class,
				applicationID,
				jobID);
		List<ScriptLineage> lineages = new ArrayList<ScriptLineage>();
		if (jobExecIdList != null)
		{
			for(Long jobExecId : jobExecIdList)
			{
				lineages = getJdbcTemplate().query(
						GET_SCRIPT_LINEAGE,
						new ScriptLineageRowMapper(),
						applicationID,
						jobExecId);
				if (lineages.size() > 0)
				{
					break;
				}
			}
		}

		return lineages;
	}
}