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

import models.Dataset;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;
import play.Play;

import java.sql.Time;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatasetWithUserRowMapper implements RowMapper<Dataset>
{
    public static String DATASET_ID_COLUMN = "id";
    public static String DATASET_NAME_COLUMN = "name";
    public static String DATASET_URN_COLUMN = "urn";
    public static String DATASET_SCHEMA_COLUMN = "schema";
    public static String DATASET_SOURCE_COLUMN = "source";
    public static String DATASET_CREATED_TIME_COLUMN = "created";
    public static String DATASET_MODIFIED_TIME_COLUMN = "modified";
    public static String FAVORITE_DATASET_ID_COLUMN = "dataset_id";
    public static String DATASET_WATCH_ID_COLUMN = "watch_id";
    public static String DATASET_PROPERTIES_COLUMN = "properties";
    public static String HDFS_PREFIX = "hdfs";

    @Override
    public Dataset mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        int id = rs.getInt(DATASET_ID_COLUMN);
        String name = rs.getString(DATASET_NAME_COLUMN);
        String urn = rs.getString(DATASET_URN_COLUMN);
        String source = rs.getString(DATASET_SOURCE_COLUMN);
        String schema = rs.getString(DATASET_SCHEMA_COLUMN);
        Time created = rs.getTime(DATASET_CREATED_TIME_COLUMN);
        Time modified = rs.getTime(DATASET_MODIFIED_TIME_COLUMN);
        Integer favoriteId = rs.getInt(FAVORITE_DATASET_ID_COLUMN);
        Long watchId = rs.getLong(DATASET_WATCH_ID_COLUMN);
        Dataset dataset = new Dataset();
        dataset.id = id;
        dataset.name = name;
        dataset.urn = urn;
        dataset.schema = schema;
        if (StringUtils.isNotBlank(dataset.urn))
        {
            if (dataset.urn.substring(0, 4).equalsIgnoreCase(HDFS_PREFIX))
            {
                dataset.nertzLink = Play.application().configuration().getString(DatasetsDAO.NERTZ_URL_KEY) +
                        dataset.urn.substring(6);
            }
        }
        dataset.source = source;
        if (modified != null)
        {
            dataset.modified = new java.util.Date(modified.getTime());
        }
        if (created != null)
        {
            dataset.created = new java.util.Date(created.getTime());
        } else if (modified != null) {
            dataset.created = new java.util.Date(modified.getTime());
        }
        if (favoriteId != null && favoriteId > 0)
        {
            dataset.isFavorite = true;
        }
        else
        {
            dataset.isFavorite = false;
        }
        if (watchId != null && watchId > 0)
        {
            dataset.watchId = watchId;
            dataset.isWatched = true;
        }
        else
        {
            dataset.watchId = 0L;
            dataset.isWatched = false;
        }

        return dataset;
    }
}
