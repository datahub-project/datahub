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

import java.util.List;

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

public class SchemaHistoryDAO extends AbstractMySQLOpenSourceDAO{


	private final static String GET_PAGED_SCHEMA_DATASET  = "SELECT SQL_CALC_FOUND_ROWS " +
			"DISTINCT dataset_id, urn, " +
			"MAX(DATE_FORMAT(modified_date,'%Y-%m-%d')) as modified_date FROM dict_dataset_schema_history " +
			"WHERE dataset_id is not null GROUP BY 1 ORDER BY urn LIMIT ?, ?";

	private final static String GET_SPECIFIED_SCHEMA_DATASET  = "SELECT SQL_CALC_FOUND_ROWS " +
			"DISTINCT dataset_id, urn, " +
			"MAX(DATE_FORMAT(modified_date,'%Y-%m-%d')) as modified_date FROM dict_dataset_schema_history " +
			"WHERE dataset_id = ? GROUP BY 1 ORDER BY urn LIMIT ?, ?";

	private final static String GET_PAGED_SCHEMA_DATASET_WITH_FILTER  = "SELECT SQL_CALC_FOUND_ROWS " +
			"DISTINCT dataset_id, urn, DATE_FORMAT(modified_date,'%Y-%m-%d') as modified_date " +
			"FROM dict_dataset_schema_history WHERE dataset_id is not null and urn LIKE ? " +
            "GROUP BY 1 ORDER BY urn LIMIT ?, ?";

	private final static String GET_SPECIFIED_SCHEMA_DATASET_WITH_FILTER  = "SELECT SQL_CALC_FOUND_ROWS " +
			"DISTINCT dataset_id, urn, DATE_FORMAT(modified_date,'%Y-%m-%d') as modified_date " +
			"FROM dict_dataset_schema_history WHERE dataset_id = ? and urn LIKE ? " +
			"GROUP BY 1 ORDER BY urn LIMIT ?, ?";

	private final static String GET_SCHEMA_HISTORY_BY_DATASET_ID = "SELECT DATE_FORMAT(modified_date,'%Y-%m-%d') " +
            "as modified_date, `schema` FROM dict_dataset_schema_history WHERE dataset_id = ? ORDER BY 1";

	public static ObjectNode getPagedSchemaDataset(String name, Long datasetId, int page, int size)
	{
		ObjectNode result = Json.newObject();

		javax.sql.DataSource ds = getJdbcTemplate().getDataSource();
		DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
		TransactionTemplate txTemplate = new TransactionTemplate(tm);

		result = txTemplate.execute(new TransactionCallback<ObjectNode>() {
			public ObjectNode doInTransaction(TransactionStatus status) {

				List<SchemaDataset> pagedScripts = null;
				if (StringUtils.isNotBlank(name))
				{
					if (datasetId != null && datasetId > 0)
					{
						pagedScripts = getJdbcTemplate().query(
								GET_SPECIFIED_SCHEMA_DATASET_WITH_FILTER,
								new SchemaDatasetRowMapper(),
								datasetId,
								"%" + name + "%",
								(page - 1) * size, size);

					}
					else
					{
						pagedScripts = getJdbcTemplate().query(
								GET_PAGED_SCHEMA_DATASET_WITH_FILTER,
								new SchemaDatasetRowMapper(),
								"%" + name + "%",
								(page - 1) * size, size);
					}
				}
				else
				{
					if (datasetId != null && datasetId > 0)
					{
						pagedScripts = getJdbcTemplate().query(
								GET_SPECIFIED_SCHEMA_DATASET,
								new SchemaDatasetRowMapper(),
								datasetId, (page - 1) * size, size);
					}
					else
					{
						pagedScripts = getJdbcTemplate().query(
								GET_PAGED_SCHEMA_DATASET,
								new SchemaDatasetRowMapper(),
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

				ObjectNode resultNode = Json.newObject();
				resultNode.put("count", count);
				resultNode.put("page", page);
				resultNode.put("itemsPerPage", size);
				resultNode.put("totalPages", (int) Math.ceil(count / ((double) size)));
				resultNode.set("datasets", Json.toJson(pagedScripts));

				return resultNode;
			}
		});

		return result;
	}

	public static List<SchemaHistoryData> getSchemaHistoryByDatasetID(int id)
	{
		return getJdbcTemplate().query(
				GET_SCHEMA_HISTORY_BY_DATASET_ID,
				new SchemaHistoryDataRowMapper(),
				id);
	}

}
