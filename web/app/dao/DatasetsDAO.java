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
import java.time.Instant;
import java.util.*;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
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
import play.libs.Json;
import models.*;

public class DatasetsDAO extends AbstractMySQLOpenSourceDAO
{
	public final static String HDFS_BROWSER_URL_KEY = "dataset.hdfs_browser.link";


	private final static String SELECT_PAGED_DATASET  = "SELECT " +
			"d.id, d.name, d.urn, d.source, d.properties, d.schema, " +
			"GROUP_CONCAT(o.owner_id ORDER BY o.sort_id ASC SEPARATOR ',') as owner_id, " +
			"GROUP_CONCAT(IFNULL(u.display_name, '*') ORDER BY o.sort_id ASC SEPARATOR ',') as owner_name, " +
			"FROM_UNIXTIME(source_created_time) as created, d.source_modified_time, " +
			"FROM_UNIXTIME(source_modified_time) as modified " +
			"FROM ( SELECT * FROM dict_dataset ORDER BY urn LIMIT ?, ? ) d " +
			"LEFT JOIN dataset_owner o on (d.id = o.dataset_id and (o.is_deleted is null OR o.is_deleted != 'Y')) " +
			"LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id and u.app_id = 300) " +
			"GROUP BY d.id, d.name, d.urn, d.source, d.properties, d.schema, " +
			"created, d.source_modified_time, modified";

	private final static String SELECT_PAGED_DATASET_BY_CURRENT_USER  = "SELECT " +
			"d.id, d.name, d.urn, d.source, d.schema, d.properties, " +
			"f.dataset_id, w.id as watch_id, " +
			"GROUP_CONCAT(o.owner_id ORDER BY o.sort_id ASC SEPARATOR ',') as owner_id, " +
			"GROUP_CONCAT(IFNULL(u.display_name, '*') ORDER BY o.sort_id ASC SEPARATOR ',') as owner_name, " +
			"FROM_UNIXTIME(source_created_time) as created, d.source_modified_time, " +
			"FROM_UNIXTIME(source_modified_time) as modified " +
			"FROM ( SELECT * FROM dict_dataset ORDER BY urn LIMIT ?, ?) d LEFT JOIN favorites f ON (" +
			"d.id = f.dataset_id and f.user_id = ?) " +
			"LEFT JOIN watch w on (d.id = w.item_id and w.item_type = 'dataset' and w.user_id = ?) " +
			"LEFT JOIN dataset_owner o on (d.id = o.dataset_id and (o.is_deleted is null OR o.is_deleted != 'Y')) " +
			"LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id and u.app_id = 300) " +
			"GROUP BY d.id, d.name, d.urn, d.source, d.schema, d.properties, f.dataset_id, " +
			"watch_id, created, d.source_modified_time, modified";

	private final static String GET_PAGED_DATASET_COUNT  = "SELECT count(*) FROM dict_dataset";

	private final static String SELECT_PAGED_DATASET_BY_URN  = "SELECT " +
			"d.id, d.name, d.urn, d.source, d.properties, d.schema, " +
			"GROUP_CONCAT(o.owner_id ORDER BY o.sort_id ASC SEPARATOR ',') as owner_id, " +
			"GROUP_CONCAT(IFNULL(u.display_name, '*') ORDER BY o.sort_id ASC SEPARATOR ',') as owner_name, " +
			"FROM_UNIXTIME(source_created_time) as created, d.source_modified_time, " +
			"FROM_UNIXTIME(source_modified_time) as modified " +
			"FROM ( SELECT * FROM dict_dataset WHERE urn LIKE ? ORDER BY urn limit ?, ? ) d " +
			"LEFT JOIN dataset_owner o on (d.id = o.dataset_id and (o.is_deleted is null OR o.is_deleted != 'Y')) " +
			"LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id and u.app_id = 300) " +
			"GROUP BY d.id, d.name, d.urn, d.source, d.properties, d.schema, created, " +
			"d.source_modified_time, modified";

	private final static String SELECT_PAGED_DATASET_BY_URN_CURRENT_USER  = "SELECT " +
			"d.id, d.name, d.urn, d.source, d.schema, " +
			"GROUP_CONCAT(o.owner_id ORDER BY o.sort_id ASC SEPARATOR ',') as owner_id, " +
			"GROUP_CONCAT(IFNULL(u.display_name, '*') ORDER BY o.sort_id ASC SEPARATOR ',') as owner_name, " +
			"d.properties, f.dataset_id, w.id as watch_id, " +
			"FROM_UNIXTIME(source_created_time) as created, d.source_modified_time, " +
			"FROM_UNIXTIME(source_modified_time) as modified " +
			"FROM ( SELECT * FROM dict_dataset WHERE urn LIKE ?  ORDER BY urn LIMIT ?, ? ) d " +
			"LEFT JOIN favorites f ON (" +
			"d.id = f.dataset_id and f.user_id = ?) " +
			"LEFT JOIN watch w ON (d.id = w.item_id and w.item_type = 'dataset' and w.user_id = ?) " +
			"LEFT JOIN dataset_owner o on (d.id = o.dataset_id and (o.is_deleted is null OR o.is_deleted != 'Y')) " +
			"LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id and u.app_id = 300) " +
			"GROUP BY d.id, d.name, d.urn, d.source, d.schema, d.properties, f.dataset_id, " +
			"watch_id, created, d.source_modified_time, modified";

	private final static String GET_PAGED_DATASET_COUNT_BY_URN  = "SELECT count(*) FROM dict_dataset WHERE urn LIKE ?";

	private final static String CHECK_SCHEMA_HISTORY  = "SELECT COUNT(*) FROM dict_dataset_schema_history " +
			"WHERE dataset_id = ? ";

	private final static String GET_DATASET_BY_ID = "SELECT d.id, max(s.id) as schema_history_id, d.name, " +
			"d.urn, d.source, d.schema, GROUP_CONCAT(o.owner_id ORDER BY o.sort_id ASC SEPARATOR ',') as owner_id, " +
			"GROUP_CONCAT(IFNULL(u.display_name, '*') ORDER BY o.sort_id ASC SEPARATOR ',') as owner_name, " +
			"FROM_UNIXTIME(source_created_time) as created, d.source_modified_time, " +
			"FROM_UNIXTIME(source_modified_time) as modified " +
			"FROM dict_dataset d LEFT JOIN dict_dataset_schema_history s on (d.id = s.dataset_id) " +
			"LEFT JOIN dataset_owner o on (d.id = o.dataset_id) " +
			"LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id) " +
			"WHERE d.id = ? GROUP BY d.id, d.name, d.urn, d.source, d.schema, " +
			"created, d.source_modified_time, modified";

	private final static String GET_DATASET_BY_ID_CURRENT_USER  = "SELECT DISTINCT d.id, " +
			"max(s.id) as schema_history_id, " +
			"d.name, d.urn, d.source, d.schema, " +
			"GROUP_CONCAT(o.owner_id ORDER BY o.sort_id ASC SEPARATOR ',') as owner_id, " +
			"GROUP_CONCAT(IFNULL(u.display_name, '*') ORDER BY o.sort_id ASC SEPARATOR ',') as owner_name, " +
			"FROM_UNIXTIME(d.source_created_time) as created, " +
			"d.source_modified_time, " +
			"FROM_UNIXTIME(d.source_modified_time) as modified, f.dataset_id, w.id as watch_id FROM dict_dataset d " +
			"LEFT JOIN favorites f ON (d.id = f.dataset_id and f.user_id = ?) " +
			"LEFT JOIN dict_dataset_schema_history s on (d.id = s.dataset_id) " +
			"LEFT JOIN watch w ON (w.item_id = d.id and w.item_type = 'dataset' and w.user_id = ?) " +
			"LEFT JOIN dataset_owner o on (d.id = o.dataset_id) " +
			"LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id) " +
			"WHERE d.id = ? GROUP BY d.id, d.name, d.urn, d.source, d.schema, created, " +
			"d.source_modified_time, modified, f.dataset_id, watch_id";

	private final static String GET_DATASET_COLUMNS_BY_DATASET_ID = "select dfd.field_id, dfd.sort_id, " +
			"dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, " +
			"dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, " +
			"dfd.is_distributed as distributed, c.comment, " +
			"( SELECT count(*) FROM dict_dataset_field_comment ddfc " +
			"WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count " +
			"FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON " +
			"(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN field_comments c ON " +
			"c.id = ddfc.comment_id WHERE dfd.dataset_id = ? ORDER BY dfd.sort_id";

	private final static String GET_DATASET_COLUMNS_BY_DATASETID_AND_COLUMNID = "SELECT dfd.field_id, " +
			"dfd.sort_id, dfd.parent_sort_id, dfd.parent_path, dfd.field_name, dfd.data_type, " +
			"dfd.is_nullable as nullable, dfd.is_indexed as indexed, dfd.is_partitioned as partitioned, " +
			"dfd.is_distributed as distributed, c.text as comment, " +
			"( SELECT count(*) FROM dict_dataset_field_comment ddfc " +
			"WHERE ddfc.dataset_id = dfd.dataset_id AND ddfc.field_id = dfd.field_id ) as comment_count " +
			"FROM dict_field_detail dfd LEFT JOIN dict_dataset_field_comment ddfc ON " +
			"(ddfc.field_id = dfd.field_id AND ddfc.is_default = true) LEFT JOIN comments c ON " +
			"c.id = ddfc.comment_id WHERE dfd.dataset_id = ? AND dfd.field_id = ? ORDER BY dfd.sort_id";

	private final static String GET_DATASET_OWNERS_BY_ID = "SELECT o.owner_id, u.display_name, o.sort_id, " +
			"o.owner_type, o.namespace, o.owner_id_type, o.owner_source, o.owner_sub_type, o.confirmed_by " +
			"FROM dataset_owner o " +
			"LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id and u.app_id = 300) " +
			"WHERE o.dataset_id = ? and (o.is_deleted is null OR o.is_deleted != 'Y') ORDER BY o.sort_id";

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

	private final static String GET_DATASET_OWNERS = "SELECT o.owner_id, o.namespace, " +
			"o.owner_type, o.owner_sub_type, " +
			"o.dataset_urn, u.display_name FROM dataset_owner o " +
			"LEFT JOIN dir_external_user_info u on (o.owner_id = u.user_id and u.app_id = 300) " +
			"WHERE dataset_id = ? and (o.is_deleted is null OR o.is_deleted != 'Y') ORDER BY sort_id";

	private final static String UPDATE_DATASET_OWNER_SORT_ID = "UPDATE dataset_owner " +
			"set sort_id = ? WHERE dataset_id = ? AND owner_id = ? AND namespace = ?";

	private final static String OWN_A_DATASET = "INSERT INTO dataset_owner (" +
			"dataset_id, owner_id, app_id, namespace, " +
			"owner_type, is_group, is_active, sort_id, created_time, modified_time, wh_etl_exec_id, dataset_urn) " +
			"VALUES(?, ?, 300, 'urn:li:corpuser', 'Producer', 'N', 'Y', 0, UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), 0, ?)";

	private final static String UNOWN_A_DATASET = "UPDATE dataset_owner " +
			"set is_deleted = 'Y' WHERE dataset_id = ? AND owner_id = ?  AND app_id = 300";

	private final static String UPDATE_DATASET_OWNERS = "INSERT INTO dataset_owner (dataset_id, owner_id, app_id, " +
			"namespace, owner_type, is_group, is_active, is_deleted, sort_id, created_time, " +
			"modified_time, wh_etl_exec_id, dataset_urn, owner_sub_type) " +
			"VALUES(?, ?, ?, ?, ?, ?, 'Y', 'N', ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), 0, ?, ?) " +
			"ON DUPLICATE KEY UPDATE owner_type = ?, is_group = ?, is_deleted = 'N', " +
			"sort_id = ?, modified_time= UNIX_TIMESTAMP(), owner_sub_type=?";

	private final static String UPDATE_DATASET_CONFIRMED_OWNERS = "INSERT INTO dataset_owner " +
			"(dataset_id, owner_id, app_id, namespace, owner_type, is_group, is_active, " +
			"is_deleted, sort_id, created_time, modified_time, wh_etl_exec_id, dataset_urn, owner_sub_type, " +
			"confirmed_by, confirmed_on) " +
			"VALUES(?, ?, ?, ?, ?, ?, 'Y', 'N', ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP(), 0, ?, ?, ?, ?) " +
			"ON DUPLICATE KEY UPDATE owner_type = ?, is_group = ?, is_deleted = 'N', " +
			"sort_id = ?, modified_time= UNIX_TIMESTAMP(), owner_sub_type=?, confirmed_by=?, confirmed_on=?";

	private final static String MARK_DATASET_OWNERS_AS_DELETED = "UPDATE dataset_owner " +
			"set is_deleted = 'Y' WHERE dataset_id = ?";

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
			"WHERE user_id = ? and item_type = 'urn' and urn = ?";

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

	private final static String GET_COLUMN_COMMENTS_BY_DATASETID_AND_COLUMNID = "SELECT SQL_CALC_FOUND_ROWS " +
			"c.id, u.name as author, " +
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
			"SELECT count(*) as count, f.comment_id, c.comment FROM dict_field_detail d " +
			"JOIN dict_dataset_field_comment f on d.field_id = f.field_id and d.dataset_id = f.dataset_id " +
			"JOIN field_comments c on c.id = f.comment_id WHERE d.field_name = ? and f.is_default = 1 " +
			"GROUP BY f.comment_id, c.comment ORDER BY count DESC";

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


	private final static String GET_DATASET_OWNER_TYPES = "SELECT DISTINCT owner_type " +
			"FROM dataset_owner WHERE owner_type is not null";

	private final static String GET_DATASET_DEPENDS_VIEW = "SELECT object_type, object_sub_type, " +
			"object_name, map_phrase, is_identical_map, mapped_object_dataset_id, " +
			"mapped_object_type,  mapped_object_sub_type, mapped_object_name " +
			"FROM cfg_object_name_map WHERE object_name = ?";

	private final static String GET_DATASET_REFERENCES = "SELECT object_type, object_sub_type, " +
			"object_name, object_dataset_id, map_phrase, is_identical_map, mapped_object_dataset_id, " +
			"mapped_object_type,  mapped_object_sub_type, mapped_object_name " +
			"FROM cfg_object_name_map WHERE mapped_object_name = ?";

	private final static String GET_DATASET_LISTVIEW_TOP_LEVEL_NODES = "SELECT DISTINCT " +
			"SUBSTRING_INDEX(urn, ':///', 1) as name, 0 as id, " +
			"concat(SUBSTRING_INDEX(urn, ':///', 1), ':///') as urn FROM dict_dataset order by 1";

	private final static String GET_DATASET_LISTVIEW_NODES_BY_URN = "SELECT distinct " +
			"SUBSTRING_INDEX(SUBSTRING_INDEX(d.urn, ?, -1), '/', 1) as name, " +
			"concat(?, SUBSTRING_INDEX(SUBSTRING_INDEX(d.urn, ?, -1), '/', 1)) as urn, " +
			"s.id FROM dict_dataset d LEFT JOIN dict_dataset s " +
			"ON s.urn = concat(?, SUBSTRING_INDEX(SUBSTRING_INDEX(d.urn, ?, -1), '/', 1)) " +
			"WHERE d.urn LIKE ? ORDER BY d.urn";

	private final static String GET_DATASET_VERSIONS = "SELECT DISTINCT version " +
			"FROM dict_dataset_instance WHERE dataset_id = ? and version != '0' ORDER BY version_sort_id DESC";

	private final static String GET_DATASET_NATIVE_NAME = "SELECT native_name " +
			"FROM dict_dataset_instance WHERE dataset_id = ? ORDER BY version_sort_id DESC limit 1";

	private final static String GET_DATASET_SCHEMA_TEXT_BY_VERSION = "SELECT schema_text " +
			"FROM dict_dataset_instance WHERE dataset_id = ? and version = ? ORDER BY db_id DESC limit 1";

	private final static String GET_DATASET_INSTANCES = "SELECT DISTINCT i.db_id, c.db_code FROM " +
			"dict_dataset_instance i JOIN cfg_database c ON i.db_id = c.db_id " +
			"WHERE i.dataset_id = ?";

	private final static String GET_DATASET_ACCESS_PARTITION_GAIN = "SELECT DISTINCT partition_grain " +
			"FROM log_dataset_instance_load_status WHERE dataset_id = ? order by 1";

	private final static String GET_DATASET_ACCESS_PARTITION_INSTANCES = "SELECT DISTINCT d.db_code " +
			"FROM log_dataset_instance_load_status l " +
			"JOIN cfg_database d on l.db_id = d.db_id WHERE dataset_id = ? and partition_grain = ? ORDER BY 1";

	private final static String GET_DATASET_ACCESS = "SELECT l.db_id, d.db_code, l.dataset_type, l.partition_expr, " +
			"l.data_time_expr, l.data_time_epoch, l.record_count, l.size_in_byte, l.log_time_epoch, " +
			"from_unixtime(l.log_time_epoch) as log_time_str FROM log_dataset_instance_load_status l " +
			"JOIN cfg_database d on l.db_id = d.db_id WHERE dataset_id = ? and partition_grain = ? " +
			"ORDER by l.data_time_expr DESC";

	public static List<String> getDatasetOwnerTypes()
	{
		return getJdbcTemplate().queryForList(GET_DATASET_OWNER_TYPES, String.class);
	}

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
								(page - 1) * size, size,
								id,
								id);
					} else {
						rows = getJdbcTemplate().queryForList(
								SELECT_PAGED_DATASET_BY_URN_CURRENT_USER,
								urn + "%",
								(page - 1) * size, size,
								id,
								id);
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

					if (StringUtils.isBlank(urn)) {
						count = getJdbcTemplate().queryForObject(
								GET_PAGED_DATASET_COUNT,
								Long.class);
					}
					else
					{
						count = getJdbcTemplate().queryForObject(
								GET_PAGED_DATASET_COUNT_BY_URN,
								Long.class,
								urn + "%");
					}
				} catch (EmptyResultDataAccessException e) {
					Logger.error("Exception = " + e.getMessage());
				}

				for (Map row : rows) {

					Dataset ds = new Dataset();
					Timestamp modified = (Timestamp)row.get(DatasetWithUserRowMapper.DATASET_MODIFIED_TIME_COLUMN);
					ds.id = (Long)row.get(DatasetWithUserRowMapper.DATASET_ID_COLUMN);
					ds.name = (String)row.get(DatasetWithUserRowMapper.DATASET_NAME_COLUMN);
					ds.source = (String)row.get(DatasetWithUserRowMapper.DATASET_SOURCE_COLUMN);
					ds.urn = (String)row.get(DatasetWithUserRowMapper.DATASET_URN_COLUMN);
					ds.schema = (String)row.get(DatasetWithUserRowMapper.DATASET_SCHEMA_COLUMN);
					String strOwner = (String)row.get(DatasetWithUserRowMapper.DATASET_OWNER_ID_COLUMN);
					String strOwnerName = (String)row.get(DatasetWithUserRowMapper.DATASET_OWNER_NAME_COLUMN);
					Long sourceModifiedTime =
							(Long)row.get(DatasetWithUserRowMapper.DATASET_SOURCE_MODIFIED_TIME_COLUMN);
					String properties = (String)row.get(DatasetWithUserRowMapper.DATASET_PROPERTIES_COLUMN);
					try
					{
						if (StringUtils.isNotBlank(properties))
						{
							ds.properties = Json.parse(properties);
						}
					}
					catch (Exception e)
					{
						Logger.error(e.getMessage());
					}

					if (modified != null && sourceModifiedTime != null && sourceModifiedTime > 0)
					{
						ds.modified = modified;
						ds.formatedModified = modified.toString();
					}

					String[] owners = null;
					if (StringUtils.isNotBlank(strOwner))
					{
						owners = strOwner.split(",");
					}
					String[] ownerNames = null;
					if (StringUtils.isNotBlank(strOwnerName))
					{
						ownerNames = strOwnerName.split(",");
					}
					ds.owners = new ArrayList<User>();
					if (owners != null && ownerNames != null)
					{
						if (owners.length == ownerNames.length)
						{
							for (int i = 0; i < owners.length; i++)
							{
								User datasetOwner = new User();
								datasetOwner.userName = owners[i];
								if (datasetOwner.userName.equalsIgnoreCase(user))
								{
									ds.isOwned = true;
								}
								if (StringUtils.isBlank(ownerNames[i]) || ownerNames[i].equalsIgnoreCase("*"))
								{
									datasetOwner.name = owners[i];
								}
								else
								{
									datasetOwner.name = ownerNames[i];
								}
								ds.owners.add(datasetOwner);
							}
						}
						else
						{
							Logger.error("getPagedDatasets get wrong owner and names. Dataset ID: "
									+ Long.toString(ds.id) + " Owner: " + owners + " Owner names: " + ownerNames);
						}
					}

					Integer favoriteId = (Integer)row.get(DatasetWithUserRowMapper.FAVORITE_DATASET_ID_COLUMN);
					Long watchId = (Long)row.get(DatasetWithUserRowMapper.DATASET_WATCH_ID_COLUMN);

					Long schemaHistoryRecordCount = 0L;
					try
					{
						schemaHistoryRecordCount = getJdbcTemplate().queryForObject(
								CHECK_SCHEMA_HISTORY,
								Long.class,
								ds.id);
					}
					catch (EmptyResultDataAccessException e)
					{
						Logger.error("Exception = " + e.getMessage());
					}

					if (StringUtils.isNotBlank(ds.urn))
					{
						if (ds.urn.substring(0, 4).equalsIgnoreCase(DatasetRowMapper.HDFS_PREFIX))
						{
							ds.hdfsBrowserLink = Play.application().configuration().getString(HDFS_BROWSER_URL_KEY) +
									ds.urn.substring(DatasetRowMapper.HDFS_URN_PREFIX_LEN);
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
					if (schemaHistoryRecordCount != null && schemaHistoryRecordCount > 0)
					{
						ds.hasSchemaHistory = true;
					}
					else
					{
						ds.hasSchemaHistory = false;
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

	public static ObjectNode ownDataset(int id, String user)
	{
		ObjectNode resultNode = Json.newObject();
		boolean result = false;
		List<Map<String, Object>> rows = null;

		rows = getJdbcTemplate().queryForList(GET_DATASET_OWNERS, id);
		int sortId = 0;
		for (Map row : rows)
		{
			String ownerId = (String)row.get(DatasetWithUserRowMapper.DATASET_OWNER_ID_COLUMN);
			String namespace = (String)row.get("namespace");
			int ret = getJdbcTemplate().update(UPDATE_DATASET_OWNER_SORT_ID, ++sortId, id, ownerId, namespace);
			if (ret <= 0)
			{
				Logger.warn("ownDataset update sort_id failed. Dataset id is : " +
						Long.toString(id) + " owner_id is : " + ownerId + " namespace is : " + namespace);
			}
		}

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
			Logger.error("Dataset ownDataset get urn failed, id = " + id);
			Logger.error("Exception = " + e.getMessage());
		}
		int status = getJdbcTemplate().update(
				UPDATE_DATASET_OWNERS,
				id,
				user,
				300,
				"urn:li:corpuser",
				"Producer",
				"N",
				0,
				urn,
				"",
				"Producer",
				"N",
				0,
				"");
		if (status > 0)
		{
			result = true;
		}
		rows = getJdbcTemplate().queryForList(GET_DATASET_OWNERS, id);
		List<User> owners = new ArrayList<User>();
		for (Map row : rows)
		{
			String ownerId = (String)row.get(DatasetWithUserRowMapper.DATASET_OWNER_ID_COLUMN);
			String dislayName = (String)row.get("display_name");
			if (StringUtils.isBlank(dislayName))
			{
				dislayName = ownerId;
			}
			User owner = new User();
			owner.userName = ownerId;
			owner.name = dislayName;
			owners.add(owner);
		}
		if (result)
		{
			resultNode.put("status", "success");
		}
		else
		{
			resultNode.put("status", "failed");
		}
		resultNode.set("owners", Json.toJson(owners));
		return resultNode;
	}

	public static ObjectNode unownDataset(int id, String user)
	{
		ObjectNode resultNode = Json.newObject();
		boolean result = false;
		int ret = getJdbcTemplate().update(UNOWN_A_DATASET, id, user);
		if (ret > 0)
		{
			result = true;
		}
		List<Map<String, Object>> rows = null;
		rows = getJdbcTemplate().queryForList(GET_DATASET_OWNERS, id);
		List<User> owners = new ArrayList<User>();
		int sortId = 0;
		for (Map row : rows)
		{
			String ownerId = (String)row.get(DatasetWithUserRowMapper.DATASET_OWNER_ID_COLUMN);
			String dislayName = (String)row.get("display_name");
			String namespace = (String)row.get("namespace");
			if (StringUtils.isBlank(dislayName))
			{
				dislayName = ownerId;
			}
			User owner = new User();
			owner.userName = ownerId;
			owner.name = dislayName;
			owners.add(owner);
			int updatedRows = getJdbcTemplate().update(UPDATE_DATASET_OWNER_SORT_ID, sortId++, id, ownerId, namespace);
			if (ret <= 0)
			{
				Logger.warn("ownDataset update sort_id failed. Dataset id is : " +
						Long.toString(id) + " owner_id is : " + ownerId + " namespace is : " + namespace);
			}
		}
		if (result)
		{
			resultNode.put("status", "success");
		}
		else
		{
			resultNode.put("status", "failed");
		}
		resultNode.set("owners", Json.toJson(owners));
		return resultNode;
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

	public static List<DatasetOwner> getDatasetOwnersByID(int id)
	{
		List<DatasetOwner> owners = new ArrayList<DatasetOwner>();

		owners = getJdbcTemplate().query(GET_DATASET_OWNERS_BY_ID, new DatasetOwnerRowMapper(), id);

		return owners;
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

	public static ObjectNode getPagedDatasetComments(String userName, int id, int page, int size)
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

				if (pagedComments != null)
				{
					for(DatasetComment dc : pagedComments)
					{
						if(StringUtils.isNotBlank(userName) && userName.equalsIgnoreCase(dc.authorUserName))
						{
							dc.isAuthor = true;
						}
					}
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
			rows = getJdbcTemplate().queryForList(GET_WATCHED_URN_ID, userId, urn);
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
				message = "watch item already exist";
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
			rows = getJdbcTemplate().queryForList(GET_WATCHED_URN_ID, userId, urn);
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

	public static ObjectNode getPagedDatasetColumnComments(String userName, int datasetId, int columnId, int page, int size)
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

				if (pagedComments != null)
				{
					for(DatasetColumnComment dc : pagedComments)
					{
						if(StringUtils.isNotBlank(userName) && userName.equalsIgnoreCase(dc.authorUsername))
						{
							dc.isAuthor = true;
						}
					}
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

	public static List similarColumnComments(Long datasetId, int columnId)
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
					Long.toString(datasetId) + " columnId = " + Integer.toString(columnId));
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
				sc.datasetId = datasetId;
				comments.add(sc);
			}
		} catch(DataAccessException e) {
			Logger.error("Dataset similarColumnComments - get comments by field name, datasetId = " +
					Long.toString(datasetId) + " columnId = " + Integer.toString(columnId));
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

	public static void updateDatasetOwnerDatabase(int datasetId, String datasetUrn, List<DatasetOwner> owners)
	{
		getJdbcTemplate().batchUpdate(UPDATE_DATASET_CONFIRMED_OWNERS,
				new BatchPreparedStatementSetter() {
					@Override
					public void setValues(PreparedStatement ps, int i)
							throws SQLException {
						DatasetOwner owner = owners.get(i);
						ps.setInt(1, datasetId);
						ps.setString(2, owner.userName);
						ps.setInt(3, owner.isGroup ? 301 : 300);
						ps.setString(4, owner.namespace);
						ps.setString(5, owner.type);
						ps.setString(6, owner.isGroup ? "Y" : "N");
						ps.setInt(7, owner.sortId);
						ps.setString(8, datasetUrn);
						ps.setString(9, owner.subType);
						ps.setString(10, owner.confirmedBy);
						if (StringUtils.isBlank(owner.confirmedBy))
						{
							ps.setLong(11, 0L);
						} else {
							ps.setLong(11, Instant.now().getEpochSecond());
						}
						ps.setString(12, owner.type);
						ps.setString(13, owner.isGroup ? "Y" : "N");
						ps.setInt(14, owner.sortId);
						ps.setString(15, owner.subType);
						ps.setString(16, owner.confirmedBy);
						if (StringUtils.isBlank(owner.confirmedBy))
						{
							ps.setLong(17, 0L);
						} else {
							ps.setLong(17, Instant.now().getEpochSecond());
						}

					}

					@Override
					public int getBatchSize()
					{
						return owners.size();
					}
				}
		);
	}

	public static boolean updateDatasetOwners(int datasetId, Map<String, String[]> ownersMap, String user)
	{
		boolean result = true;
		if ((ownersMap == null) || ownersMap.size() == 0)
		{
			return false;
		}

		List<DatasetOwner> owners = new ArrayList<DatasetOwner>();
		if (ownersMap.containsKey("owners")) {
			String[] textArray = ownersMap.get("owners");
			if (textArray != null && textArray.length > 0)
			{
				JsonNode node = Json.parse(textArray[0]);
				for(int i = 0; i < node.size(); i++) {
					JsonNode ownerNode = node.get(i);
					if (ownerNode != null)
					{
						String userName = "";
						if (ownerNode.has("userName"))
						{
							userName = ownerNode.get("userName").asText();
						}
						if (StringUtils.isBlank(userName))
						{
							continue;
						}
						Boolean isGroup = false;
						if (ownerNode.has("isGroup"))
						{
							isGroup = ownerNode.get("isGroup").asBoolean();
						}
						String type = "";
						if (ownerNode.has("type") && (!ownerNode.get("type").isNull()))
						{
							type = ownerNode.get("type").asText();
						}
						String subType = "";
						if (ownerNode.has("subType") && (!ownerNode.get("subType").isNull()))
						{
							subType = ownerNode.get("subType").asText();
						}

						String confirmedBy = "";
						if (ownerNode.has("confirmedBy") && (!ownerNode.get("confirmedBy").isNull()))
						{
							confirmedBy = ownerNode.get("confirmedBy").asText();
						}

						DatasetOwner owner = new DatasetOwner();
						owner.userName = userName;
						owner.isGroup = isGroup;
						if (isGroup)
						{
							owner.namespace = "urn:li:griduser";
						}
						else
						{
							owner.namespace = "urn:li:corpuser";
						}
						owner.type = type;
						owner.subType = subType;
						owner.confirmedBy = confirmedBy;
						owner.sortId = i;
						owners.add(owner);
					}
				}
			}
		}

		getJdbcTemplate().update(MARK_DATASET_OWNERS_AS_DELETED, datasetId);
		if (owners.size() > 0)
		{
			String urn = null;
			try
			{
				urn = (String)getJdbcTemplate().queryForObject(
						GET_DATASET_URN_BY_ID,
						String.class,
						datasetId);
			}
			catch(EmptyResultDataAccessException e)
			{
				Logger.error("Dataset updateDatasetOwners get urn failed, id = " + datasetId);
				Logger.error("Exception = " + e.getMessage());
			}
			updateDatasetOwnerDatabase(datasetId, urn, owners);
		}

		return result;
	}

	public static void getDependencies(
			Long datasetId,
			List<DatasetDependency> depends)
	{
		String nativeName = null;
		try
		{
			nativeName = getJdbcTemplate().queryForObject(
					GET_DATASET_NATIVE_NAME,
					String.class,
					datasetId);
		}
		catch (EmptyResultDataAccessException e)
		{
			nativeName = null;
		}

		if (StringUtils.isNotBlank(nativeName))
		{
			getDatasetDependencies("/" + nativeName.replace(".", "/"), 1, 0, depends);
		}

	}

	public static void getReferences(
			Long datasetId,
			List<DatasetDependency> references)
	{
		String nativeName = null;
		try
		{
			nativeName = getJdbcTemplate().queryForObject(
					GET_DATASET_NATIVE_NAME,
					String.class,
					datasetId);
		}
		catch (EmptyResultDataAccessException e)
		{
			nativeName = null;
		}

		if (StringUtils.isNotBlank(nativeName))
		{
			getDatasetReferences("/" + nativeName.replace(".", "/"), 1, 0, references);
		}

	}


	public static void getDatasetDependencies(
			String objectName,
			int level,
			int parent,
			List<DatasetDependency> depends)
	{
		if (depends == null)
		{
			depends = new ArrayList<DatasetDependency>();
		}

		List<Map<String, Object>> rows = null;
		rows = getJdbcTemplate().queryForList(
				GET_DATASET_DEPENDS_VIEW,
				objectName);

		if (rows != null)
		{
			for (Map row : rows) {
				DatasetDependency dd = new DatasetDependency();
				dd.datasetId = (Long) row.get("mapped_object_dataset_id");
				dd.objectName = (String) row.get("mapped_object_name");
				dd.objectType = (String) row.get("mapped_object_type");
				dd.objectSubType = (String) row.get("mapped_object_sub_type");
				if (dd.datasetId != null && dd.datasetId > 0)
				{
					dd.isValidDataset = true;
					dd.datasetLink = "#/datasets/" + Long.toString(dd.datasetId);
				}
				else
				{
					dd.isValidDataset = false;
				}
				dd.level = level;
				dd.sortId = depends.size() + 1;
				dd.treeGridClass = "treegrid-" + Integer.toString(dd.sortId);
				if (parent != 0)
				{
					dd.treeGridClass += " treegrid-parent-" + Integer.toString(parent);
				}
				depends.add(dd);
				getDatasetDependencies(dd.objectName, level + 1, dd.sortId, depends);
			}
		}
	}

	public static void getDatasetReferences(
			String objectName,
			int level,
			int parent,
			List<DatasetDependency> references)
	{
		if (references == null)
		{
			references = new ArrayList<DatasetDependency>();
		}

		List<Map<String, Object>> rows = null;
		rows = getJdbcTemplate().queryForList(
				GET_DATASET_REFERENCES,
				objectName);

		if (rows != null)
		{
			for (Map row : rows) {
				DatasetDependency dd = new DatasetDependency();
				dd.datasetId = (Long) row.get("object_dataset_id");
				dd.objectName = (String) row.get("object_name");
				dd.objectType = (String) row.get("object_type");
				dd.objectSubType = (String) row.get("object_sub_type");
				if (dd.datasetId != null && dd.datasetId > 0)
				{
					dd.isValidDataset = true;
					dd.datasetLink = "#/datasets/" + Long.toString(dd.datasetId);
				}
				else
				{
					dd.isValidDataset = false;
				}
				dd.level = level;
				dd.sortId = references.size() + 1;
				dd.treeGridClass = "treegrid-" + Integer.toString(dd.sortId);
				if (parent != 0)
				{
					dd.treeGridClass += " treegrid-parent-" + Integer.toString(parent);
				}
				references.add(dd);
				getDatasetReferences(dd.objectName, level + 1, dd.sortId, references);
			}
		}
	}

	public static List<DatasetListViewNode> getDatasetListViewNodes(String urn) {

		List<DatasetListViewNode> nodes = new ArrayList<DatasetListViewNode>();
		List<Map<String, Object>> rows = null;

		if (StringUtils.isBlank(urn)) {
			rows = getJdbcTemplate().queryForList(
					GET_DATASET_LISTVIEW_TOP_LEVEL_NODES);
		} else {
			rows = getJdbcTemplate().queryForList(
					GET_DATASET_LISTVIEW_NODES_BY_URN,
					urn,
					urn,
					urn,
					urn,
					urn,
					urn + "%");
		}

		for (Map row : rows) {

			DatasetListViewNode node = new DatasetListViewNode();
			node.datasetId = (Long) row.get(DatasetWithUserRowMapper.DATASET_ID_COLUMN);
			node.nodeName = (String) row.get(DatasetWithUserRowMapper.DATASET_NAME_COLUMN);
			String nodeUrn = (String) row.get(DatasetWithUserRowMapper.DATASET_URN_COLUMN);
			if (node.datasetId !=null && node.datasetId > 0)
			{
				node.nodeUrl = "#/datasets/" + node.datasetId;
			}
			else
			{
				node.nodeUrl = "#/datasets/name/" + node.nodeName + "/page/1?urn=" + nodeUrn;
			}
			nodes.add(node);
		}

		return nodes;
	}

	public static List<String> getDatasetVersions(Long datasetId, Integer dbId)
	{
		return getJdbcTemplate().queryForList(GET_DATASET_VERSIONS, String.class, datasetId);
	}

	public static String getDatasetSchemaTextByVersion(
			Long datasetId, String version)
	{
		String schemaText = null;
		try
		{
			schemaText = getJdbcTemplate().queryForObject(
					GET_DATASET_SCHEMA_TEXT_BY_VERSION,
					String.class,
					datasetId, version);
		}
		catch (EmptyResultDataAccessException e)
		{
			schemaText = null;
		}
		return schemaText;
	}

	public static List<DatasetInstance> getDatasetInstances(Long id)
	{
		List<DatasetInstance> datasetInstances = new ArrayList<DatasetInstance>();

		List<Map<String, Object>> rows = null;
		rows = getJdbcTemplate().queryForList(
				GET_DATASET_INSTANCES,
				id);

		if (rows != null)
		{
			for (Map row : rows) {
				DatasetInstance datasetInstance = new DatasetInstance();
				datasetInstance.datasetId = id;
				datasetInstance.dbId = (Integer) row.get("db_id");
				datasetInstance.dbCode = (String) row.get("db_code");
				datasetInstances.add(datasetInstance);
			}
		}
		return datasetInstances;
	}

	public static List<String> getDatasetPartitionGains(Long id) {
		return getJdbcTemplate().queryForList(GET_DATASET_ACCESS_PARTITION_GAIN, String.class, id);
	}

	public static List<String> getDatasetPartitionInstance(Long id, String partition) {
		return getJdbcTemplate().queryForList(GET_DATASET_ACCESS_PARTITION_INSTANCES, String.class, id, partition);
	}

	public static List<DatasetPartition> getDatasetAccessibilty(Long id) {

		ObjectNode resultNode = Json.newObject();
		List<String> partitions = getDatasetPartitionGains(id);
		List<DatasetPartition> datasetPartitions = new ArrayList<DatasetPartition>();
		if (partitions != null && partitions.size() > 0)
		{
			for(String partition:partitions)
			{
				List<Map<String, Object>> rows = null;
				Map<String, DatasetAccessibility> addedAccessibilities= new HashMap<String, DatasetAccessibility>();
				rows = getJdbcTemplate().queryForList(
						GET_DATASET_ACCESS,
						id,
						partition);
				List<DatasetAccessibility> datasetAccessibilities = new ArrayList<DatasetAccessibility>();
				List<String> instances = new ArrayList<String>();
				instances = getDatasetPartitionInstance(id, partition);

				if (rows != null)
				{
					for (Map row : rows) {
						DatasetAccessibility datasetAccessibility = new DatasetAccessibility();
						datasetAccessibility.datasetId = id;
						datasetAccessibility.itemList = new ArrayList<DatasetAccessItem>();
						datasetAccessibility.dbId = (Integer) row.get("db_id");
						datasetAccessibility.dbName = (String) row.get("db_code");
						datasetAccessibility.datasetType = (String) row.get("dataset_type");
						datasetAccessibility.partitionExpr = (String) row.get("partition_expr");
						datasetAccessibility.partitionGain = partition;
						datasetAccessibility.dataTimeExpr = (String) row.get("data_time_expr");
						datasetAccessibility.dataTimeEpoch = (Integer) row.get("data_time_epoch");
						datasetAccessibility.recordCount = (Long) row.get("record_count");
						if (datasetAccessibility.recordCount == null)
						{
							datasetAccessibility.recordCount = 0L;
						}
						datasetAccessibility.sizeInByte = (Long) row.get("size_in_byte");
						datasetAccessibility.logTimeEpoch = (Integer) row.get("log_time_epoch");
						datasetAccessibility.logTimeEpochStr = row.get("log_time_str").toString();
						DatasetAccessibility exist = addedAccessibilities.get(datasetAccessibility.dataTimeExpr);
						if (exist == null)
						{
							for(int i = 0; i < instances.size(); i++)
							{
								DatasetAccessItem datasetAccessItem = new DatasetAccessItem();
								if(instances.get(i).equalsIgnoreCase(datasetAccessibility.dbName))
								{
									datasetAccessItem.recordCountStr = Long.toString(datasetAccessibility.recordCount);
									datasetAccessItem.logTimeEpochStr = datasetAccessibility.logTimeEpochStr;
									datasetAccessItem.isPlaceHolder = false;
								}
								else
								{
									datasetAccessItem.recordCountStr = "";
									datasetAccessItem.logTimeEpochStr = "";
									datasetAccessItem.isPlaceHolder = true;
								}
								datasetAccessibility.itemList.add(datasetAccessItem);
							}
							addedAccessibilities.put(datasetAccessibility.dataTimeExpr, datasetAccessibility);
							datasetAccessibilities.add(datasetAccessibility);
						}
						else
						{
							for(int i = 0; i < instances.size(); i++)
							{
								if(instances.get(i).equalsIgnoreCase(datasetAccessibility.dbName))
								{
									DatasetAccessItem datasetAccessItem = new DatasetAccessItem();
									datasetAccessItem.logTimeEpochStr = datasetAccessibility.logTimeEpochStr;
									datasetAccessItem.recordCountStr = Long.toString(datasetAccessibility.recordCount);
									datasetAccessItem.isPlaceHolder = false;
									exist.itemList.set(i, datasetAccessItem);
								}
							}
						}
					}
				}
				DatasetPartition datasetPartition = new DatasetPartition();
				datasetPartition.datasetId = id;
				datasetPartition.accessibilityList = datasetAccessibilities;
				datasetPartition.instanceList = instances;
				datasetPartition.partition = partition;
				datasetPartitions.add(datasetPartition);
			}
		}

		return datasetPartitions;
	}
}
