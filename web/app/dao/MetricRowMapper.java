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

import models.Metric;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MetricRowMapper implements RowMapper<Metric>
{
    public static String METRIC_ID_COLUMN = "metric_id";
    public static String METRIC_NAME_COLUMN = "metric_name";
    public static String METRIC_DESCRIPTION_COLUMN = "metric_description";
    public static String METRIC_DASHBOARD_NAME_COLUMN = "dashboard_name";
    public static String METRIC_GROUP_COLUMN = "metric_group";
    public static String METRIC_CATEGORY_COLUMN = "metric_category";
    public static String METRIC_SUB_CATEGORY_COLUMN = "metric_sub_category";
    public static String METRIC_LEVEL_COLUMN = "metric_level";
    public static String METRIC_SOURCE_TYPE_COLUMN = "metric_source_type";
    public static String METRIC_SOURCE_COLUMN = "metric_source";
    public static String METRIC_SOURCE_DATASET_ID_COLUMN = "metric_source_dataset_id";
    public static String METRIC_REF_ID_TYPE_COLUMN = "metric_ref_id_type";
    public static String METRIC_REF_ID_COLUMN = "metric_ref_id";
    public static String METRIC_TYPE_COLUMN = "metric_type";
    public static String METRIC_GRAIN_COLUMN = "metric_grain";
    public static String METRIC_DISPLAY_FACTOR_COLUMN = "metric_display_factor";
    public static String METRIC_DISPLAY_FACTOR_SYM_COLUMN = "metric_display_factor_sym";
    public static String METRIC_GOOD_DIRECTION_COLUMN = "metric_good_direction";
    public static String METRIC_FORMULA_COLUMN = "metric_formula";
    public static String METRIC_DIMENSIONS_COLUMN = "dimensions";
    public static String METRIC_OWNERS_COLUMN = "owners";
    public static String METRIC_TAGS_COLUMN = "tags";
    public static String METRIC_URN_COLUMN = "urn";
    public static String METRIC_URL_COLUMN = "metric_url";
    public static String METRIC_WIKI_URL_COLUMN = "wiki_url";
    public static String METRIC_SCM_URL_COLUMN = "scm_url";
    public static String METRIC_WATCH_ID_COLUMN = "watch_id";

    public static String METRIC_MODULE_ID = "id";
    public static String METRIC_MODULE_NAME = "name";
    public static String METRIC_MODULE_DESCRIPTION = "description";
    public static String METRIC_MODULE_REF_ID = "refID";
    public static String METRIC_MODULE_REF_ID_TYPE = "refIDType";
    public static String METRIC_MODULE_DASHBOARD_NAME = "dashboardName";
    public static String METRIC_MODULE_CATEGORY = "category";
    public static String METRIC_MODULE_GROUP = "group";
    public static String METRIC_MODULE_SOURCE = "source";
    public static String METRIC_MODULE_SOURCE_TYPE = "sourceType";
    public static String METRIC_MODULE_GRAIN = "grain";
    public static String METRIC_MODULE_FORMULA = "formula";
    public static String METRIC_MODULE_DISPLAY_FACTOR = "displayFactor";
    public static String METRIC_MODULE_DISPLAY_FACTOR_SYM = "displayFactorSym";
    public static String METRIC_MODULE_SUB_CATEGORY = "subCategory";

    @Override
    public Metric mapRow(ResultSet rs, int rowNum) throws SQLException
    {
        int id = rs.getInt(METRIC_ID_COLUMN);
        String name = rs.getString(METRIC_NAME_COLUMN);
        String description = rs.getString(METRIC_DESCRIPTION_COLUMN);
        String dashboardName = rs.getString(METRIC_DASHBOARD_NAME_COLUMN);
        String group = rs.getString(METRIC_GROUP_COLUMN);
        String category = rs.getString(METRIC_CATEGORY_COLUMN);
        String subCategory = rs.getString(METRIC_SUB_CATEGORY_COLUMN);
        String level = rs.getString(METRIC_LEVEL_COLUMN);
        String sourceType = rs.getString(METRIC_SOURCE_TYPE_COLUMN);
        String source = rs.getString(METRIC_SOURCE_COLUMN);
        Long sourceDatasetId = rs.getLong(METRIC_SOURCE_DATASET_ID_COLUMN);
        String refIdType = rs.getString(METRIC_REF_ID_TYPE_COLUMN);
        String refId = rs.getString(METRIC_REF_ID_COLUMN);
        String type = rs.getString(METRIC_TYPE_COLUMN);
        String grain = rs.getString(METRIC_GRAIN_COLUMN);
        String displayFactor = rs.getString(METRIC_DISPLAY_FACTOR_COLUMN);
        String displayFactorSym = rs.getString(METRIC_DISPLAY_FACTOR_SYM_COLUMN);
        String goodDirection = rs.getString(METRIC_GOOD_DIRECTION_COLUMN);
        String formula = rs.getString(METRIC_FORMULA_COLUMN);
        String dimensions = rs.getString(METRIC_DIMENSIONS_COLUMN);
        if (StringUtils.isNotBlank(dimensions))
        {
            dimensions = dimensions.replace(",", ", ");
        }
        String owners = rs.getString(METRIC_OWNERS_COLUMN);
        if (StringUtils.isNotBlank(owners))
        {
            owners = owners.replace(",", ", ");
        }
        String tags = rs.getString(METRIC_TAGS_COLUMN);
        if (StringUtils.isNotBlank(tags))
        {
            tags = tags.replace(",", ", ");
        }
        String urn = rs.getString(METRIC_URN_COLUMN);
        String url = rs.getString(METRIC_URL_COLUMN);
        String wikiUrl = rs.getString(METRIC_WIKI_URL_COLUMN);
        String scmUrl = rs.getString(METRIC_SCM_URL_COLUMN);
        Long watchId = rs.getLong(METRIC_WATCH_ID_COLUMN);

        Metric metric = new Metric();
        metric.id = id;
        metric.name = name;
        metric.description = description;
        metric.dashboardName = dashboardName;
        metric.group = group;
        metric.category = category;
        metric.subCategory = subCategory;
        metric.level = level;
        metric.sourceType = sourceType;
        metric.source = source;
        metric.sourceDatasetId = sourceDatasetId;
        if (metric.sourceDatasetId != null && metric.sourceDatasetId > 0)
        {
            metric.sourceDatasetLink = "#/datasets/" + Long.toString(metric.sourceDatasetId);
        }
        metric.refIDType = refIdType;
        metric.refID = refId;
        metric.type = type;
        metric.grain = grain;
        metric.displayFactor = displayFactor;
        metric.displayFactorSym = displayFactorSym;
        metric.goodDirection = goodDirection;
        metric.formula = formula;
        metric.dimensions = dimensions;
        metric.owners = owners;
        metric.tags = tags;
        metric.urn = urn;
        metric.metricUrl = url;
        metric.wikiUrl = wikiUrl;
        metric.scmUrl = scmUrl;
        metric.watchId = watchId;

        return metric;
    }
}
