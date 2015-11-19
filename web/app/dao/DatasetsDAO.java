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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import play.Logger;
import play.Play;
import play.libs.F;
import play.libs.Json;
import models.*;
import play.libs.WS;
import utils.Lineage;

public class DatasetsDAO extends AbstractMySQLOpenSourceDAO
{
	public final static String NERTZ_URL_KEY = "dataset.nertz.link";


	private final static String SELECT_PAGED_DATASET  = "SELECT SQL_CALC_FOUND_ROWS " +
			"id, name, urn, source, properties, `schema` FROM dict_dataset ORDER BY urn LIMIT ?, ?";

	private final static String SELECT_PAGED_DATASET_BY_CURRENT_USER  = "SELECT SQL_CALC_FOUND_ROWS " +
			"d.id, d.name, d.urn, d.source, d.schema, d.properties, f.dataset_id, w.id as watch_id " +
			"FROM dict_dataset d LEFT JOIN favorites f ON (" +
			"d.id = f.dataset_id and f.user_id = ?) " +
			"LEFT JOIN watch w on (d.id = w.item_id and w.item_type = 'dataset' and w.user_id = ?) " +
			"ORDER BY d.urn LIMIT ?, ?";

	private final static String SELECT_PAGED_DATASET_BY_URN  = "SELECT SQL_CALC_FOUND_ROWS " +
			"id, name, urn, source, properties, `schema` FROM dict_dataset WHERE urn LIKE ? ORDER BY urn limit ?, ?";

	private final static String SELECT_PAGED_DATASET_BY_URN_CURRENT_USER  = "SELECT SQL_CALC_FOUND_ROWS " +
			"d.id, d.name, d.urn, d.source, d.schema, d.properties, f.dataset_id, w.id as watch_id " +
			"FROM dict_dataset d LEFT JOIN favorites f ON (" +
			"d.id = f.dataset_id and f.user_id = ?) " +
			"LEFT JOIN watch w ON (d.id = w.item_id and w.item_type = 'dataset' and w.user_id = ?) " +
			"WHERE d.urn LIKE ? ORDER BY urn LIMIT ?, ?";

	private final static String GET_DATASET_BY_ID = "SELECT id, name, urn, source, `schema`, " +
			"FROM_UNIXTIME(source_created_time) as created, FROM_UNIXTIME(source_modified_time) as modified " +
			"FROM dict_dataset WHERE id = ?";

	private final static String GET_DATASET_BY_ID_CURRENT_USER  = "SELECT DISTINCT d.id, " +
			"d.name, d.urn, d.source, d.schema, FROM_UNIXTIME(d.source_created_time) as created, " +
			"FROM_UNIXTIME(d.source_modified_time) as modified, f.dataset_id, w.id as watch_id FROM dict_dataset d " +
			"LEFT JOIN favorites f ON (d.id = f.dataset_id and f.user_id = ?) " +
			"LEFT JOIN watch w ON (w.item_id = d.id and w.item_type = 'dataset' and w.user_id = ?) " +
			"WHERE d.id = ?";

	private final static String GET_DATASET_COLUMNS_BY_DATASET_ID = "select dfd.field_id, dfd.sort_id, " +
			"dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, " +
			"dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, " +
			"dfd.is_distributed as distributed, c.comment, " +
			"( SELECT count(*) FROM dict_dataset_field_comment ddfc " +
			"WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count " +
			"FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON " +
			"(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN field_comments c ON " +
			"c.id = ddfc.comment_id WHERE dfd.dataset_id = ? ORDER BY 1";

	private final static String GET_DATASET_COLUMNS_BY_DATASETID_AND_COLUMNID = "SELECT dfd.field_id, " +
			"dfd.sort_id, dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, " +
			"dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, " +
			"dfd.is_distributed as distributed, c.text as comment, " +
			"( SELECT count(*) FROM dict_dataset_field_comment ddfc " +
			"WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count " +
			"FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON " +
			"(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN comments c ON " +
			"c.id = ddfc.comment_id WHERE dfd.dataset_id = ? AND dfd.field_id = ? ORDER BY 1";

	private final static String GET_DATASET_PROPERTIES_BY_DATASET_ID =
			"SELECT source, `properties` FROM dict_dataset WHERE id=?";

	private final static String GET_DATASET_SAMPLE_DATA_BY_ID =
			"SELECT dataset_id, urn, ref_id, data FROM dict_dataset_sample WHERE dataset_id=?";

	private final static String GET_DATASET_SAMPLE_DATA_BY_REFID =
			"SELECT data FROM dict_dataset_sample WHERE dataset_id=?";

	private final static String GET_DATASET_URN_BY_ID =
			"SELECT urn FROM dict_dataset WHERE id=?";

	private final static String GET_USER_ID = "select id FROM users WHERE username = ?";

	private final static String FAVORITE_A_DATASET =
			"INSERT INTO favorites (user_id, dataset_id, created) VALUES(?, ?, NOW())";

	private final static String UNFAVORITE_A_DATASET =
			"DELETE FROM favorites WHERE user_id = ? and dataset_id = ?";

	private final static String GET_FAVORITES = "SELECT DISTINCT d.id, d.name, d.urn, d.source " +
						"FROM dict_dataset d JOIN favorites f ON d.id = f.dataset_id " +
			"JOIN users u ON f.dataset_id = d.id and f.user_id = u.id WHERE u.username = ? ORDER BY d.urn";

	private final static String GET_COMMENTS_BY_DATASET_ID = "SELECT SQL_CALC_FOUND_ROWS " +
			"c.id, c.dataset_id, c.text, c.created, c.modified, c.comment_type, " +
			"u.name, u.email, u.username FROM comments c JOIN users u ON c.user_id = u.id " +
			"WHERE c.dataset_id = ? ORDER BY modified DESC, id DESC LIMIT ?, ?";

	private final static String CREATE_DATASET_COMMENT = "INSERT INTO comments " +
			"(text, user_id, dataset_id, created, modified, comment_type) VALUES(?, ?, ?, NOW(), NOW(), ?)";

	private final static String GET_WATCHED_URN_ID = "SELECT id FROM watch " +
			"WHERE user_id = ? and item_type = 'urn' and urn = '$URN'";

	private final static String GET_WATCHED_DATASET_ID = "SELECT id FROM watch " +
			"WHERE user_id = ? and item_id = ? and item_type = 'dataset'";

	private final static String WATCH_DATASET = "INSERT INTO watch " +
			"(user_id, item_id, urn, item_type, notification_type, created) VALUES(?, ?, NULL, 'dataset', ?, NOW())";

	private final static String UPDATE_DATASET_WATCH = "UPDATE watch " +
			"set user_id = ?, item_id = ?, notification_type = ? WHERE id = ?";

	private final static String WATCH_URN = "INSERT INTO watch " +
			"(user_id, item_id, urn, item_type, notification_type, created) VALUES(?, NULL, ?, 'urn', ?, NOW())";

	private final static String UPDATE_URN_WATCH = "update watch " +
			"set user_id = ?, urn = ?, notification_type = ? WHERE id = ?";

	private final static String CHECK_IF_COLUMN_COMMENT_EXIST = "SELECT id FROM field_comments " +
			"WHERE comment_crc32_checksum = CRC32(?) and comment = ?";

	private final static String CREATE_COLUMN_COMMENT = "INSERT INTO field_comments " +
			"(comment, user_id, created, modified, comment_crc32_checksum) VALUES(?, ?, NOW(), NOW(), CRC32(?))";

	private final static String UPDATE_DATASET_COMMENT = "UPDATE comments " +
			"SET text = ?, comment_type = ?, modified = NOW() WHERE id = ?";

	private final static String UPDATE_COLUMN_COMMENT = "UPDATE field_comments " +
			"SET comment = ?, modified = NOW() WHERE id = ?";

	private final static String DELETE_DATASET_COMMENT = "DELETE FROM comments WHERE id = ?";

	private final static String UNWATCH_DATASET = "DELETE FROM watch WHERE id = ?";

	private final static String GET_FIELD_COMMENT_BY_ID = "SELECT comment FROM dict_dataset_field_comment WHERE id = ?";

	private final static String GET_COLUMN_COMMENTS_BY_DATASETID_AND_COLUMNID = "SELECT c.id, u.name as author, " +
			"u.email as authorEmail, u.username as authorUsername, c.comment as `text`, " +
			"c.created, c.modified, dfc.field_id, dfc.is_default FROM dict_dataset_field_comment dfc " +
			"LEFT JOIN field_comments c ON dfc.comment_id = c.id LEFT JOIN users u ON c.user_id = u.id " +
			"WHERE dataset_id = ? AND field_id = ? ORDER BY is_default DESC, created LIMIT ?,?";

	private final static String CREATE_DATASET_COLUMN_COMMENT_REFERENCE =
			"INSERT IGNORE INTO dict_dataset_field_comment (dataset_id, field_id, comment_id) " +
			"VALUES (?,?,?)";

	private final static String CHECK_COLUMN_COMMENT_HAS_DEFAULT =
			"SELECT comment_id FROM dict_dataset_field_comment WHERE dataset_id = ? AND field_id = ? " +
			"AND is_default = True";

	private final static String SET_COLUMN_COMMENT_DEFAULT =
			"UPDATE dict_dataset_field_comment SET is_default = True " +
			"WHERE dataset_id = ? AND field_id = ? AND comment_id = ? " +
			"LIMIT 1";

	private final static String GET_COUNT_COLUMN_COMMENTS_BY_ID =
			"SELECT COUNT(*) FROM dict_dataset_field_comment WHERE dataset_id = ? and comment_id = ?";

	private final static String DELETE_COLUMN_COMMENT_AND_REFERENCE =
			"DELETE dfc, c FROM dict_dataset_field_comment dfc JOIN field_comments c " +
			"ON c.id = dfc.comment_id WHERE dfc.dataset_id = ? AND dfc.field_id = ? AND dfc.comment_id = ?";

	private final static String DELETE_COLUMN_COMMENT_REFERENCE =
			"DELETE FROM dict_dataset_field_comment WHERE dataset_id = ? AND column_id = ? " +
			"AND comment_id = ? LIMIT 1";

	private final static String GET_COLUMN_NAME_BY_ID =
			"SELECT UPPER(field_name) FROM dict_field_detail WHERE field_id = ?";

	private final static String GET_SIMILAR_COMMENTS_BY_FIELD_NAME =
			"SELECT count(*) as count, fd.dataset_id, " +
			"fd.default_comment_id as comment_id, fc.comment FROM dict_field_detail fd LEFT JOIN " +
			"field_comments fc ON fc.id = fd.default_comment_id WHERE UPPER(fd.field_name) = ? " +
			"AND fd.default_comment_id IS NOT NULL GROUP BY fd.default_comment_id ORDER BY count DESC";

	private final static String SET_COLUMN_COMMENT_TO_FALSE = "UPDATE dict_dataset_field_comment " +
			"SET is_default = false WHERE dataset_id = ? AND field_id = ? AND is_default = true";

	private final static String INSERT_DATASET_COLUMN_COMMENT = "INSERT INTO " +
			"dict_dataset_field_comment (dataset_id, field_id, comment_id, is_default) " +
			"VALUES (?, ?, ?, true) ON DUPLICATE KEY UPDATE is_default = true";

	private final static String GET_SIMILAR_COLUMNS_BY_FIELD_NAME = "SELECT d.id as dataset_id, " +
			"d.name as dataset_name, dfd.field_id, dfd.data_type, fc.id as comment_id, fc.comment, d.source " +
			"FROM dict_field_detail dfd JOIN dict_dataset d ON dfd.dataset_id = d.id " +
			"LEFT JOIN dict_dataset_field_comment ddfc ON ddfc.dataset_id = d.id " +
			"AND ddfc.field_id = dfd.field_id AND ddfc.is_default = 1 " +
			"LEFT JOIN field_comments fc ON ddfc.comment_id = fc.id " +
			"WHERE dfd.dataset_id <> ? AND dfd.field_name = ? ORDER BY d.name asc";

	public static ObjectNode getPagedDatasets(String urn, Integer page, Integer size, String user)
	{
		ObjectNode result = Json.newObject();

		Integer userId = UserDAO.getUserIDByUserName(user);

		javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
		TransactionTemplate txTemplate = new TransactionTemplate(tm);
		final Integer id = userId;

		result = txTemplate.execute(new TransactionCallback<ObjectNode>() {
			public ObjectNode doInTransaction(TransactionStatus status) {

				ObjectNode resultNode = Json.newObject();
				List<Dataset> pagedDatasets = new ArrayList<Dataset>();
				List<Map<String, Object>> rows = null;
				if (id != null && id > 0)
				{
					if (StringUtils.isBlank(urn)) {
						rows = getJdbcTemplate().queryForList(
								SELECT_PAGED_DATASET_BY_CURRENT_USER,
								id,
								id,
								(page - 1) * size, size);
					} else {
						rows = getJdbcTemplate().queryForList(
								SELECT_PAGED_DATASET_BY_URN_CURRENT_USER,
								id,
								id,
								urn + "%",
								(page - 1) * size, size);
					}
				}
				else
				{
					if (StringUtils.isBlank(urn)) {
						rows = getJdbcTemplate().queryForList(
								SELECT_PAGED_DATASET,
								(page - 1) * size, size);
					} else {
						rows = getJdbcTemplate().queryForList(
								SELECT_PAGED_DATASET_BY_URN,
								urn + "%",
								(page - 1) * size, size);
					}

				}

				long count = 0;
				try {
					count = getJdbcTemplate().queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				} catch (EmptyResultDataAccessException e) {
					Logger.error("Exception = " + e.getMessage());
				}

				for (Map row : rows) {

					Dataset ds = new Dataset();
					ds.id = (Long)row.get(DatasetWithUserRowMapper.DATASET_ID_COLUMN);
					ds.name = (String)row.get(DatasetWithUserRowMapper.DATASET_NAME_COLUMN);
					ds.source = (String)row.get(DatasetWithUserRowMapper.DATASET_SOURCE_COLUMN);
					ds.urn = (String)row.get(DatasetWithUserRowMapper.DATASET_URN_COLUMN);
          ds.schema = (String)row.get(DatasetWithUserRowMapper.DATASET_SCHEMA_COLUMN);
					String properties = (String)row.get(DatasetWithUserRowMapper.DATASET_PROPERTIES_COLUMN);
					if (StringUtils.isNotBlank(properties))
					{
						ds.properties = Json.parse(properties);
					}

					Integer favoriteId = (Integer)row.get(DatasetWithUserRowMapper.FAVORITE_DATASET_ID_COLUMN);
					Long watchId = (Long)row.get(DatasetWithUserRowMapper.DATASET_WATCH_ID_COLUMN);
					if (StringUtils.isNotBlank(ds.urn))
					{
						if (ds.urn.substring(0, 4).equalsIgnoreCase(DatasetRowMapper.HDFS_PREFIX))
						{
							ds.nertzLink = Play.application().configuration().getString(NERTZ_URL_KEY) + ds.urn.substring(7);
						}
					}
					if (favoriteId != null && favoriteId > 0)
					{
						ds.isFavorite = true;
					}
					else
					{
						ds.isFavorite = false;
					}
					if (watchId != null && watchId > 0)
					{
						ds.watchId = watchId;
						ds.isWatched = true;
					}
					else
					{
						ds.isWatched = false;
						ds.watchId = 0L;
					}
					pagedDatasets.add(ds);
				}

				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
				resultNode.set("datasets", Json.toJson(pagedDatasets));
				return resultNode;
			}
		});
		return result;
	}

	public static Dataset getDatasetByID(int id, String user)
	{
		Dataset dataset = null;
		Integer userId = 0;
		if (StringUtils.isNotBlank(user))
		{
			try
			{
				userId = (Integer)getJdbcTemplate().queryForObject(
						GET_USER_ID,
						Integer.class,
						user);
			}
			catch(EmptyResultDataAccessException e)
			{
				Logger.error("Dataset getDatasetByID get user id failed, username = " + user);
				Logger.error("Exception = " + e.getMessage());
			}
		}
		try
		{
			if (userId != null && userId > 0)
			{
				dataset = (Dataset)getJdbcTemplate().queryForObject(
          GET_DATASET_BY_ID_CURRENT_USER,
						new DatasetWithUserRowMapper(),
						userId,
						userId,
						id);

			}
			else
			{
				dataset = (Dataset)getJdbcTemplate().queryForObject(
						GET_DATASET_BY_ID,
						new DatasetRowMapper(),
						id);
			}
		}
		catch(EmptyResultDataAccessException e)
		{
			Logger.error("Dataset getDatasetByID failed, id = " + id);
			Logger.error("Exception = " + e.getMessage());
		}

		return dataset;
	}

	public static List<DatasetColumn> getDatasetColumnByID(int datasetId, int columnId)
	{
		return getJdbcTemplate().query(GET_DATASET_COLUMNS_BY_DATASETID_AND_COLUMNID,
      new DatasetColumnRowMapper(), datasetId, columnId);
	}

	public static List<DatasetColumn> getDatasetColumnsByID(int datasetId)
	{
		return getJdbcTemplate().query(GET_DATASET_COLUMNS_BY_DATASET_ID,
      new DatasetColumnRowMapper(), datasetId);
	}

	public static JsonNode getDatasetPropertiesByID(int id)
	{
		String properties = "";
		String source = "";
		List<Map<String, Object>> rows = null;
		JsonNode propNode = null;

		rows = getJdbcTemplate().queryForList(GET_DATASET_PROPERTIES_BY_DATASET_ID, id);

		for (Map row : rows) {
			properties = (String)row.get("properties");
			source = (String)row.get("source");
			break;
		}

		if (StringUtils.isNotBlank(properties))
		{
			try {
				propNode = Json.parse(properties);

				if (propNode != null
						&& propNode.isContainerNode()
						&& propNode.has("url")
						&& StringUtils.isNotBlank(source)
						&& source.equalsIgnoreCase("pinot"))
				{
					URL url = new URL(propNode.get("url").asText());
					BufferedReader in =
							new BufferedReader(new InputStreamReader(url.openStream()));
					String resultString = "";
					String str;

					while ((str = in.readLine()) != null) {
						resultString += str;
					}

					in.close();
					JsonNode resultNode = Json.parse(resultString);

					if (resultNode == null)
					{
						return propNode;
					}
					else{
						return resultNode;
					}
				}
			}
			catch(Exception e) {
				Logger.error("Dataset getDatasetPropertiesByID parse properties failed, id = " + id);
				Logger.error("Exception = " + e.getMessage());
			}
		}

		return propNode;
	}

	public static JsonNode getDatasetSampleDataByID(int id)
	{
		List<Map<String, Object>> rows = null;
		JsonNode sampleNode = null;
		String strSampleData = null;
		Integer refID = 0;

		rows = getJdbcTemplate().queryForList(GET_DATASET_SAMPLE_DATA_BY_ID, id);

		for (Map row : rows) {
			refID = (Integer)row.get("ref_id");
			strSampleData = (String)row.get("data");
			break;
		}

		if (refID != null && refID != 0)
		{
			rows = null;
			rows = getJdbcTemplate().queryForList(GET_DATASET_SAMPLE_DATA_BY_REFID, refID);
			for (Map row : rows) {
				strSampleData = (String)row.get("data");
				break;
			}
		}

		if (StringUtils.isNotBlank(strSampleData)) {
			try {
				sampleNode = Json.parse(strSampleData);
				return utils.SampleData.secureSampleData(sampleNode);
			} catch (Exception e) {
				Logger.error("Dataset getDatasetSampleDataByID parse properties failed, id = " + id);
				Logger.error("Exception = " + e.getMessage());
			}
		}

		return sampleNode;
	}

	public static List<ImpactDataset> getImpactAnalysisByID(int id)
	{
		String urn = null;

		try
		{
			urn = (String)getJdbcTemplate().queryForObject(
					GET_DATASET_URN_BY_ID,
					String.class,
					id);
		}
		catch(EmptyResultDataAccessException e)
		{
			Logger.error("Dataset getImpactAnalysisByID get urn failed, id = " + id);
			Logger.error("Exception = " + e.getMessage());
		}

		return LineageDAO.getImpactDatasetsByUrn(urn);
	}

	public static boolean favorite(int id, String user)
	{
		ObjectNode resultNode = Json.newObject();
		boolean result = false;
		Integer userId = UserDAO.getUserIDByUserName(user);

		if (userId != null && userId !=0)
		{
			int row = getJdbcTemplate().update(FAVORITE_A_DATASET, userId, id);
			if (row > 0)
			{
				result = true;
			}
		}
		return result;
	}

	public static boolean unfavorite(int id, String user)
	{
		ObjectNode resultNode = Json.newObject();
		boolean result = false;
		Integer userId = UserDAO.getUserIDByUserName(user);

		if (userId != null && userId !=0)
		{
			int row = getJdbcTemplate().update(UNFAVORITE_A_DATASET, userId, id);
			if (row > 0)
			{
				result = true;
			}
		}
		return result;
	}

	public static ObjectNode getFavorites(String user)
	{
		List<Dataset> favorites = new ArrayList<Dataset>();

		if (StringUtils.isNotBlank(user))
		{
			List<Map<String, Object>> rows = null;
			rows = getJdbcTemplate().queryForList(GET_FAVORITES, user);

			for (Map row : rows) {

				Dataset ds = new Dataset();
				ds.id = (long)row.get(DatasetRowMapper.DATASET_ID_COLUMN);
				ds.name = (String)row.get(DatasetRowMapper.DATASET_NAME_COLUMN);
				ds.source = (String)row.get(DatasetRowMapper.DATASET_SOURCE_COLUMN);
				ds.urn = (String)row.get(DatasetRowMapper.DATASET_URN_COLUMN);
				favorites.add(ds);
			}
		}

		ObjectNode result = Json.newObject();
		result.put("count", favorites.size());
		result.set("datasets", Json.toJson(favorites));

		return result;
	}

	public static ObjectNode getPagedDatasetComments(int id, int page, int size)
	{
		ObjectNode result = Json.newObject();

		javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		result = txTemplate.execute(new TransactionCallback<ObjectNode>() {
			public ObjectNode doInTransaction(TransactionStatus status) {

				List<DatasetComment> pagedComments = getJdbcTemplate().query(
							GET_COMMENTS_BY_DATASET_ID,
							new DatasetCommentRowMapper(),
							id,
							(page - 1) * size, size);

				long count = 0;
				try {
					count = getJdbcTemplate().queryForObject(
							"SELECT FOUND_ROWS()",
							Long.class);
				} catch (EmptyResultDataAccessException e) {
					Logger.error("Exception = " + e.getMessage());
				}

				ObjectNode resultNode = Json.newObject();
				resultNode.set("comments", Json.toJson(pagedComments));
				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));

				return resultNode;
			}
		});
		return result;
	}

	public static boolean postComment(int datasetId, Map<String, String[]> commentMap, String user)
	{
		boolean result = false;
		if ((commentMap == null) || commentMap.size() == 0)
		{
			return false;
		}

		String text = "";
		if (commentMap.containsKey("text")) {
			String[] textArray = commentMap.get("text");
			if (textArray != null && textArray.length > 0)
			{
				text = textArray[0];
			}
		}
		if (StringUtils.isBlank(text))
		{
      return false;
		}

		String type = "Comment";
		if (commentMap.containsKey("type")) {
			String[] typeArray = commentMap.get("type");
			if (typeArray != null && typeArray.length > 0)
			{
				type = typeArray[0];
			}
		}

		Integer commentId = 0;
		if (commentMap.containsKey("id")) {
			String[] idArray = commentMap.get("id");
			if (idArray != null && idArray.length > 0)
			{
				String idStr = idArray[0];
				try
				{
					commentId = Integer.parseInt(idStr);
				}
				catch(NumberFormatException e)
				{
					Logger.error("DatasetDAO postComment wrong id parameter. Error message: " +
							e.getMessage());
					commentId = 0;
				}
			}
		}

		Integer userId = UserDAO.getUserIDByUserName(user);

		if (userId != null && userId !=0)
		{
			if (commentId != null && commentId != 0)
			{
				int row = getJdbcTemplate().update(UPDATE_DATASET_COMMENT, text, type, commentId);
				if (row > 0)
				{
					result = true;
				}
			}
			else
			{
				int row = getJdbcTemplate().update(CREATE_DATASET_COMMENT, text, userId, datasetId, type);
				if (row > 0)
				{
					result = true;
				}
			}
		}
		return result;
	}

	public static boolean deleteComment(int id)
	{
		boolean result = false;
		int row = getJdbcTemplate().update(DELETE_DATASET_COMMENT, id);
		if (row > 0)
		{
			result = true;
		}
		return result;
	}

	public static Long getWatchId(String urn, String user)
	{
		Long id = 0L;
		Integer userId = UserDAO.getUserIDByUserName(user);

		if (userId != null && userId !=0)
		{
			List<Map<String, Object>> rows = null;
			rows = getJdbcTemplate().queryForList(GET_WATCHED_URN_ID.replace("$URN", urn), userId);
			if (rows != null)
			{
				for (Map row : rows) {
					id = (Long)row.get("id");
					break;
				}
			}
		}
		return id;
	}

	public static String watchDataset(int datasetId, Map<String, String[]> params, String user)
	{
		String message = "Internal error";
		if (params == null || params.size() == 0)
		{
			return "Empty post body";
		}

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

		Long watchId = 0L;
		if (params.containsKey("id")) {
			String[] watchIdArray = params.get("id");
			if (watchIdArray != null && watchIdArray.length > 0)
			{
				try
				{
					watchId = Long.parseLong(watchIdArray[0]);
				}
				catch(NumberFormatException e)
				{
					Logger.error("DatasetDAO watchDataset wrong watch_id parameter. Error message: " +
							e.getMessage());
					watchId = 0L;
				}
			}
		}

		Integer userId = UserDAO.getUserIDByUserName(user);

		if (userId != null && userId !=0)
		{
			List<Map<String, Object>> rows = null;
			rows = getJdbcTemplate().queryForList(GET_WATCHED_DATASET_ID, userId, datasetId);
			if (rows != null && rows.size() > 0)
			{
				message = "watch item is already exist";
			}
			else
			{
				int row = 0;
				if (watchId > 0)
				{
					row = getJdbcTemplate().update(UPDATE_DATASET_WATCH, userId, datasetId, notificationType, watchId);
				}
				else
				{
					row = getJdbcTemplate().update(WATCH_DATASET, userId, datasetId, notificationType);
				}
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
		int row = getJdbcTemplate().update(UNWATCH_DATASET, id);
		if (row > 0)
		{
			result = true;
		}
		return result;
	}

	public static String watchURN(Map<String, String[]> params, String user)
	{
		String message = "Internal error";
		if (params == null || params.size() == 0)
		{
			return "Empty post body";
		}

			String urn = "";
		if (params.containsKey("urn")) {
			String[] urnArray = params.get("urn");
			if (urnArray != null && urnArray.length > 0)
			{
				urn = urnArray[0];
			}
		}
		if (StringUtils.isBlank(urn))
		{
			return "urn parameter is missing";
		}

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

		Long watchId = 0L;
		if (params.containsKey("id")) {
			String[] watchIdArray = params.get("id");
			if (watchIdArray != null && watchIdArray.length > 0)
			{
				try
				{
					watchId = Long.parseLong(watchIdArray[0]);
				}
				catch(NumberFormatException e)
				{
					Logger.error("DatasetDAO watchURN wrong watch_id parameter. Error message: " +
							e.getMessage());
					watchId = 0L;
				}
			}
		}

		Integer userId = UserDAO.getUserIDByUserName(user);

		if (userId != null && userId !=0)
		{
			List<Map<String, Object>> rows = null;
			rows = getJdbcTemplate().queryForList(GET_WATCHED_URN_ID.replace("$URN", urn), userId);
			if (rows != null && rows.size() > 0)
			{
				message = "watch item is already exist";
			}
			else
			{
				int row = 0;
				if (watchId > 0)
				{
					row = getJdbcTemplate().update(UPDATE_URN_WATCH, userId, urn, notificationType, watchId);
				}
				else
				{
					row = getJdbcTemplate().update(WATCH_URN, userId, urn, notificationType);
				}
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

	public static ObjectNode getPagedDatasetColumnComments(int datasetId, int columnId, int page, int size)
	{
		ObjectNode result = Json.newObject();

		javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		result = txTemplate.execute(new TransactionCallback<ObjectNode>() {
			public ObjectNode doInTransaction(TransactionStatus status) {

				ObjectNode resultNode = Json.newObject();
				long count = 0;
				int start = (page - 1) * size;
				int end = start + size;
				List<DatasetColumnComment> pagedComments = new ArrayList<DatasetColumnComment>();
				List<Map<String, Object>> rows = null;

				rows = getJdbcTemplate().queryForList(
          GET_COLUMN_COMMENTS_BY_DATASETID_AND_COLUMNID,
					datasetId,
					columnId,
					start,
					end
				);
				for (Map row : rows) {
					Long id = (Long)row.get("id");
					String author = (String)row.get("author");
					String authorEmail = (String)row.get("authorEmail");
					String authorUsername = (String)row.get("authorUsername");
					String text = (String)row.get("text");
					String created = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format((Timestamp)row.get("created"));
					String modified = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format((Timestamp)row.get("modified"));
					Long columnId = (Long)row.get("field_id");
					boolean isDefault = (Boolean)row.get("is_default");

					DatasetColumnComment datasetColumnComment = new DatasetColumnComment();
					datasetColumnComment.id = id;
					datasetColumnComment.author = author;
					datasetColumnComment.authorEmail = authorEmail;
					datasetColumnComment.authorUsername = authorUsername;
					datasetColumnComment.text = text;
					datasetColumnComment.created = created;
					datasetColumnComment.modified = modified;
					datasetColumnComment.columnId = columnId;
					datasetColumnComment.isDefault = isDefault;
					pagedComments.add(datasetColumnComment);
				}

				try {
					count = getJdbcTemplate().queryForObject("SELECT FOUND_ROWS()", Long.class);
				}
				catch (EmptyResultDataAccessException e) {
					Logger.error("Exception = " + e.getMessage());
				}

				resultNode.set("comments", Json.toJson(pagedComments));
				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));

				return resultNode;
			}
		});
		return result;
	}

	public static boolean isSameColumnCommentExist(String text)
	{
		boolean exist = false;
		if (StringUtils.isNotBlank(text))
		{
			try {
				List<Map<String, Object>> comments = getJdbcTemplate().queryForList(
					CHECK_IF_COLUMN_COMMENT_EXIST,
					text,
					text);
				if (comments != null && comments.size() > 0)
				{
					exist = true;
				}
			} catch(DataAccessException e) {
				Logger.error("Dataset isSameColumnCommentExist text is " + text);
				Logger.error("Exception = " + e.getMessage());
			}
		}
		return exist;
	}

	public static String postColumnComment(int datasetId, int columnId, Map<String, String[]> params, String user)
	{
    String result = "Post comment failed. Please try again.";
		if (params == null || params.size() == 0)
		{
			return result;
		}

		String text = "";
		if (params.containsKey("text")) {
			String[] textArray = params.get("text");
			if (textArray != null && textArray.length > 0)
			{
				text = textArray[0];
			}
		}
		if (StringUtils.isBlank(text))
		{
			return "Please input valid comment.";
		}

		Long commentId = 0L;
		if (params.containsKey("id")) {
			String[] idArray = params.get("id");
			if (idArray != null && idArray.length > 0)
			{
				String idStr = idArray[0];
				try
				{
					commentId = Long.parseLong(idStr);
				}
				catch(NumberFormatException e)
				{
					Logger.error("DatasetDAO postColumnComment wrong id parameter. Error message: " +
							e.getMessage());
					commentId = 0L;
				}
			}
		}

		Integer userId = 0;
		try
		{
			userId = (Integer)getJdbcTemplate().queryForObject(
					GET_USER_ID,
					Integer.class,
					user);
		}
		catch(EmptyResultDataAccessException e)
		{
			Logger.error("Dataset postColumnComment get user id failed, username = " + user);
			Logger.error("Exception = " + e.getMessage());
		}

		if (userId != null && userId !=0)
		{
			if (commentId != null && commentId != 0)
			{
				int row = getJdbcTemplate().update(UPDATE_COLUMN_COMMENT, text, commentId);
				if (row > 0)
				{
					result = "";
				}
			}
			else
			{
				if (isSameColumnCommentExist(text))
				{
					return "Same comment already exists.";
				}
				KeyHolder keyHolder = new GeneratedKeyHolder();
				final String comment = text;
				final int authorId = userId;
				getJdbcTemplate().update(
						new PreparedStatementCreator() {
							public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
								PreparedStatement pst =
										con.prepareStatement(CREATE_COLUMN_COMMENT, new String[]{"id"});
								pst.setString(1, comment);
								pst.setInt(2, authorId);
								pst.setString(3, comment);
								return pst;
							}
						},
						keyHolder);
				commentId =  (Long)keyHolder.getKey();
				result = "";
			}
		}

		try
		{
			getJdbcTemplate().update(
				CREATE_DATASET_COLUMN_COMMENT_REFERENCE,
					datasetId,
					columnId,
					commentId
			);
		}
		catch(DataAccessException e)
		{
			Logger.error("Dataset postColumnComment insert ignore reference, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
			Logger.error("Exception = " + e.getMessage());
		}

		List<Map<String, Object>> defaultComment = null;
		try {
			defaultComment = getJdbcTemplate().queryForList(
					CHECK_COLUMN_COMMENT_HAS_DEFAULT,
					datasetId,
					columnId
			) ;
		} catch(DataAccessException e) {
			Logger.error("Dataset postColumnComment - check for default, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
			Logger.error("Exception = " + e.getMessage());
		}

		Boolean hasDefault = false;
		if(defaultComment.size() > 0) {
			hasDefault = true;
		}

		if(hasDefault) {
			result = "";
		} else {
			try {
				getJdbcTemplate().update(
						SET_COLUMN_COMMENT_DEFAULT,
						datasetId,
						columnId,
						commentId
				);
				result = "";
			} catch(DataAccessException e) {
				result = "Post comment failed. Please try again.";
				Logger.error("Dataset postColumnComment set default comment, datasetId = " +
						Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
				Logger.error("Exception = " + e.getMessage());
			}
		}
		return result;
	}

	public static boolean deleteColumnComment(int datasetId, int columnId, int id)
	{
		boolean result = false;

		Integer commentCount = getJdbcTemplate().queryForObject(
			GET_COUNT_COLUMN_COMMENTS_BY_ID,
			new Object[] {datasetId, id},
			Integer.class);

		if(commentCount == null || commentCount == 0)
		{
			result = false;
		}
		else if (commentCount == 1)
		{
			try
			{
				getJdbcTemplate().update(
					DELETE_COLUMN_COMMENT_AND_REFERENCE,
					datasetId,
					columnId,
					id
				);
			}
			catch(DataAccessException e) {
				result = false;
				Logger.error("Dataset deleteColumnComment remove reference and comment, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
				Logger.error("Exception = " + e.getMessage());
			}
		}
		else {
			try {
				getJdbcTemplate().update(
					DELETE_COLUMN_COMMENT_REFERENCE,
					datasetId,
					columnId,
					id
				);
			}
			catch(DataAccessException e) {
				result = false;
				Logger.error("Dataset deleteColumnComment remove reference, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
				Logger.error("Exception = " + e.getMessage());
			}
		}

		return result;
	}

	public static List similarColumnComments(int datasetId, int columnId)
	{
		List<SimilarComments> comments = new ArrayList<SimilarComments>();
		List<Map<String, Object>> rows = null;
		String fieldName = "";
		try {
			fieldName = (String)getJdbcTemplate().queryForObject(
					GET_COLUMN_NAME_BY_ID,
					String.class,
					columnId);
		} catch(DataAccessException e) {
			Logger.error("Dataset similarColumnComments - get field name for columnId, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
			Logger.error("Exception = " + e.getMessage());
			return comments;
		}

		try {
			rows = getJdbcTemplate().queryForList(
					GET_SIMILAR_COMMENTS_BY_FIELD_NAME,
					fieldName
			);
			for(Map row : rows) {

				SimilarComments sc = new SimilarComments();
				sc.count = (Long)row.get("count");
				sc.commentId = (Long)row.get("comment_id");
				sc.comment = (String)row.get("comment");
				sc.datasetId = (Long)row.get("dataset_id");
				comments.add(sc);
			}
		} catch(DataAccessException e) {
			Logger.error("Dataset similarColumnComments - get comments by field name, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
			Logger.error("Exception = " + e.getMessage());
			return comments;
		}
		return comments;
	}

	public static boolean assignColumnComment(int datasetId, int columnId, int commentId)
	{
		Boolean result = false;
		try {
			getJdbcTemplate().update(
					SET_COLUMN_COMMENT_TO_FALSE,
					datasetId,
					columnId
			);
		} catch(DataAccessException e) {
			Logger.error("Dataset assignColumnComment - set current default to false, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
			Logger.error("Exception = " + e.getMessage());
			return result;
		}

		try {
			getJdbcTemplate().update(
					INSERT_DATASET_COLUMN_COMMENT,
					datasetId,
					columnId,
					commentId
			);
			result = true;
		} catch(DataAccessException e) {
			Logger.error("Dataset assignColumnComment - set current default to false, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
			Logger.error("Exception = " + e.getMessage());
			result = false;
		}
		return result;
	}

	public static List similarColumns(int datasetId, int columnId)
	{
		List<SimilarColumns> columns = new ArrayList<SimilarColumns>();
		List<Map<String, Object>> rows = null;
		String fieldName = "";
		try {
			fieldName = (String)getJdbcTemplate().queryForObject(
					GET_COLUMN_NAME_BY_ID,
					String.class,
					columnId);
		} catch(DataAccessException e) {
			Logger.error("Dataset similarColumns - get field name for columnId, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
			Logger.error("Exception = " + e.getMessage());
			return columns;
		}
		try {
			rows = getJdbcTemplate().queryForList(
					GET_SIMILAR_COLUMNS_BY_FIELD_NAME,
					datasetId,
					fieldName
			);
			for(Map row : rows) {
				SimilarColumns sc = new SimilarColumns();
				sc.datasetId = (Long)row.get("dataset_id");
				sc.datasetName = (String)row.get("dataset_name");
				sc.columnId = (Long)row.get("field_id");
				sc.dataType = (String)row.get("data_type");
				sc.source = (String)row.get("source");
				sc.commentId = (Long)row.get("comment_id");
				sc.comment = (String)row.get("comment");
				columns.add(sc);
			}
		} catch(DataAccessException e) {
			Logger.error("Dataset similarColumns - get columns by field name, datasetId = " +
					Integer.toString(datasetId) + " columnId = " + Integer.toString(columnId));
			Logger.error("Exception = " + e.getMessage());
			return columns;
		}
		return columns;

	}
}
