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

import wherehows.models.table.Metric;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;


public class MetricRowMapper implements RowMapper<Metric> {
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
  public Metric mapRow(ResultSet rs, int rowNum) throws SQLException {
    String dimensions = rs.getString(METRIC_DIMENSIONS_COLUMN);
    if (StringUtils.isNotBlank(dimensions)) {
      dimensions = dimensions.replace(",", ", ");
    }
    String owners = rs.getString(METRIC_OWNERS_COLUMN);
    if (StringUtils.isNotBlank(owners)) {
      owners = owners.replace(",", ", ");
    }
    String tags = rs.getString(METRIC_TAGS_COLUMN);
    if (StringUtils.isNotBlank(tags)) {
      tags = tags.replace(",", ", ");
    }

    Metric metric = new Metric();
    metric.setId(rs.getInt(METRIC_ID_COLUMN));
    metric.setName(rs.getString(METRIC_NAME_COLUMN));
    metric.setDescription(rs.getString(METRIC_DESCRIPTION_COLUMN));
    metric.setDashboardName(rs.getString(METRIC_DASHBOARD_NAME_COLUMN));
    metric.setGroup(rs.getString(METRIC_GROUP_COLUMN));
    metric.setCategory(rs.getString(METRIC_CATEGORY_COLUMN));
    metric.setSubCategory(rs.getString(METRIC_SUB_CATEGORY_COLUMN));
    metric.setLevel(rs.getString(METRIC_LEVEL_COLUMN));
    metric.setSourceType(rs.getString(METRIC_SOURCE_TYPE_COLUMN));
    metric.setSource(rs.getString(METRIC_SOURCE_COLUMN));
    metric.setSourceDatasetId(rs.getLong(METRIC_SOURCE_DATASET_ID_COLUMN));
    if (metric.getSourceDatasetId() != null && metric.getSourceDatasetId() > 0) {
      metric.setSourceDatasetLink("#/datasets/" + Long.toString(metric.getSourceDatasetId()));
    }
    metric.setRefIDType(rs.getString(METRIC_REF_ID_TYPE_COLUMN));
    metric.setRefID(rs.getString(METRIC_REF_ID_COLUMN));
    metric.setType(rs.getString(METRIC_TYPE_COLUMN));
    metric.setGrain(rs.getString(METRIC_GRAIN_COLUMN));
    metric.setDisplayFactor(rs.getString(METRIC_DISPLAY_FACTOR_COLUMN));
    metric.setDisplayFactorSym(rs.getString(METRIC_DISPLAY_FACTOR_SYM_COLUMN));
    metric.setGoodDirection(rs.getString(METRIC_GOOD_DIRECTION_COLUMN));
    metric.setFormula(rs.getString(METRIC_FORMULA_COLUMN));
    metric.setDimensions(dimensions);
    metric.setOwners(owners);
    metric.setTags(tags);
    metric.setUrn(rs.getString(METRIC_URN_COLUMN));
    metric.setMetricUrl(rs.getString(METRIC_URL_COLUMN));
    metric.setWikiUrl(rs.getString(METRIC_WIKI_URL_COLUMN));
    metric.setScmUrl(rs.getString(METRIC_SCM_URL_COLUMN));
    metric.setWatchId(rs.getLong(METRIC_WATCH_ID_COLUMN));

    return metric;
  }
}
