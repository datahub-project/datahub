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
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MetricRowMapper implements RowMapper<Metric>
{
    public static String METRIC_ID_COLUMN = "metric_id";
    public static String METRIC_NAME_COLUMN = "metric_name";
    public static String METRIC_DESCRIPTION_COLUMN = "metric_description";
    public static String METRIC_REF_ID_COLUMN = "metric_ref_id";
    public static String METRIC_REF_ID_TYPE_COLUMN = "metric_ref_id_type";
    public static String METRIC_DASHBOARD_NAME_COLUMN = "dashboard_name";
    public static String METRIC_CATEGORY_COLUMN = "metric_category";
    public static String METRIC_GROUP_COLUMN = "metric_group";
    public static String METRIC_SOURCE_COLUMN = "metric_source";
    public static String METRIC_SOURCE_TYPE_COLUMN = "metric_source_type";
    public static String METRIC_GRAIN_COLUMN = "metric_grain";
    public static String METRIC_FORMULA_COLUMN = "metric_formula";
    public static String METRIC_DISPLAY_FACTOR_COLUMN = "metric_display_factor";
    public static String METRIC_DISPLAY_FACTOR_SYM_COLUMN = "metric_display_factor_sym";
    public static String METRIC_SUB_CATEGORY_COLUMN = "metric_sub_category";
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
        String refId = rs.getString(METRIC_REF_ID_COLUMN);
        String refIdType = rs.getString(METRIC_REF_ID_TYPE_COLUMN);
        String dashboardName = rs.getString(METRIC_DASHBOARD_NAME_COLUMN);
        String category = rs.getString(METRIC_CATEGORY_COLUMN);
        String group = rs.getString(METRIC_GROUP_COLUMN);
        String source = rs.getString(METRIC_SOURCE_COLUMN);
        String sourceType = rs.getString(METRIC_SOURCE_TYPE_COLUMN);
        String grain = rs.getString(METRIC_GRAIN_COLUMN);
        String formula = rs.getString(METRIC_FORMULA_COLUMN);
        String displayFactor = rs.getString(METRIC_DISPLAY_FACTOR_COLUMN);
        String displayFactorSym = rs.getString(METRIC_DISPLAY_FACTOR_SYM_COLUMN);
        String subCategory = rs.getString(METRIC_SUB_CATEGORY_COLUMN);
        Long watchId = rs.getLong(METRIC_WATCH_ID_COLUMN);

        Metric metric = new Metric();
        metric.id = id;
        metric.name = name;
        metric.description = description;
        metric.refID = refId;
        metric.refIDType = refIdType;
        metric.dashboardName = dashboardName;
        metric.category = category;
        metric.subCategory = subCategory;
        metric.group = group;
        metric.source = source;
        metric.sourceType = sourceType;
        metric.grain = grain;
        metric.formula = formula;
        metric.displayFactor = displayFactor;
        metric.displayFactorSym = displayFactorSym;
        metric.watchId = watchId;

        return metric;
    }
}
