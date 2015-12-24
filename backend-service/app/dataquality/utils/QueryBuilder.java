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
package dataquality.utils;

import dataquality.dq.AuxiliaryOp;
import dataquality.models.AggComb;
import dataquality.models.AggDef;
import dataquality.models.Formula;
import dataquality.models.MetricDef;
import dataquality.models.TimeDimension;
import dataquality.models.TimeRange;
import dataquality.models.enums.Frequency;
import dataquality.models.enums.TimeGrain;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import play.Logger;


/**
 * Created by zechen on 5/5/15.
 */
public class QueryBuilder {

  private AggDef aggDef;
  private MetricDef metricDef;

  public final static String VIEW_PREFIX = "_view_";
  public final static String TABLE_PREFIX = "dq_value_";
  public final static String CUR_TIME = "$CURRENT_TIMESTAMP";
  public final static String LOWER_BOUND = "$LOWER_BOUND";
  public final static String UPPER_BOUND = "$UPPER_BOUND";
  public final static String RUN_ID = "_run_id";
  public final static String DATA_TIME = "_data_time";
  public final static String DATA_VALUE = "_data_value";
  public final static String TIME_GRAIN = "_time_grain";

  private StringBuilder selectPart = new StringBuilder("");
  private StringBuilder fromPart = new StringBuilder("");
  private StringBuilder wherePart = new StringBuilder("");
  private StringBuilder havePart = new StringBuilder("");
  private StringBuilder groupPart = new StringBuilder("");
  private StringBuilder orderPart = new StringBuilder("");

  private TimeDimension timeDimension = null;
  private TimeGrain timeGrain = null;

  List<TimeRange> timeRanges = new ArrayList<>();

  public QueryBuilder(AggDef aggDef) {
    this.aggDef = aggDef;
    read(aggDef);
  }

  public QueryBuilder(MetricDef metricDef) {
    this.metricDef = metricDef;
    read(metricDef.getComb());
    if (metricDef.getAuxComb() != null) {
      add(metricDef.getAuxComb(), metricDef.getAuxOp());
    }
  }

  private QueryBuilder(AggComb aggComb) {
    read(aggComb);
  }

  private void read(AggDef aggDef) {
    fromPart = new StringBuilder("");
    wherePart = new StringBuilder("");

    if (aggDef.getView() != null) {
      fromPart
          .append(" (" + aggDef.getView() + ") as " + VIEW_PREFIX + aggDef.getDataset().getName().replace(".", "_"));
    } else {
      fromPart.append(aggDef.getDataset().getName());
      if (aggDef.getDataset().getAlias() != null) {
        fromPart.append(" " + aggDef.getDataset().getAlias());
      }
    }

    wherePart.append(" 1 = 1 ");
    if (aggDef.getFilter() != null) {
      wherePart.append(" and " + aggDef.getFilter());
    }

    if (aggDef.getTimeDimension() != null) {
      timeDimension = aggDef.getTimeDimension();
      timeGrain = timeDimension.getGrain();
      wherePart.append(" and " + timeDimension.getCastFormat() + " >= " + LOWER_BOUND);
      wherePart.append(" and " + timeDimension.getCastFormat() + " <= " + UPPER_BOUND);
    } else {
      Frequency frequency = aggDef.getFrequency();
      if (aggDef.getFrequency().equals(Frequency.DATA_TRIGGERED)) {
        if (aggDef.getFlowFrequency() != null) {
          frequency = aggDef.getFlowFrequency();
        } else {
          frequency = Frequency.DAILY;
        }
      }

      switch (frequency) {
        case DATA_TRIGGERED:
          Logger.error("Invalid frequency");
          break;
        case HOURLY:
          timeGrain = TimeGrain.HOUR;
          break;
        case DAILY:
          timeGrain = TimeGrain.DAY;
          break;
        case WEEKLY:
          timeGrain = TimeGrain.WEEK;
          break;
        case BI_WEEKLY:
          timeGrain = TimeGrain.BI_WEEK;
          break;
        case MONTHLY:
          timeGrain = TimeGrain.MONTH;
          break;
        case QUARTERLY:
          timeGrain = TimeGrain.QUARTER;
          break;
        case YEARLY:
          timeGrain = TimeGrain.YEAR;
          break;
      }
    }
  }

  private void read(AggComb comb)
      throws IllegalArgumentException {
    selectPart = new StringBuilder(RUN_ID + "," + DATA_TIME + ","+ TIME_GRAIN);
    groupPart = new StringBuilder(RUN_ID + "," + DATA_TIME + ","+ TIME_GRAIN);
    if (comb.getDimension() != null) {
      selectPart.append("," + comb.getDimension());
      groupPart.append("," + comb.getDimension());
    }

    if (comb.getFormula() != null) {
      if (comb.getRollUpFunc() == null) {
        selectPart.append("," + comb.getFormula() + " as " + DATA_VALUE);
      } else {
        selectPart.append("," + comb.getRollUpFunc() + "(" + comb.getFormula() + ")" + " as " + DATA_VALUE);
      }
    } else {
      throw new IllegalArgumentException("formula is null");
    }

    fromPart = new StringBuilder(TABLE_PREFIX + comb.getAggQueryId());
  }

  public String add(AggComb auxComb, AuxiliaryOp op)
      throws IllegalArgumentException {
    String d1 = metricDef.getComb().getDimension();
    String d2 = auxComb.getDimension();

    if (!StringUtil.containsAll(d1, d2)) {
      throw new IllegalArgumentException("dimension mismatch for auxiliary aggregation combination");
    }

    groupPart = new StringBuilder("");

    String s1 = getSql();
    QueryBuilder qb = new QueryBuilder(auxComb);
    String s2 = qb.getSql();
    fromPart = new StringBuilder(
        "(" + s1 + ") as _c1 left join " + "(" + s2 + ") as _c2 on _c1." + RUN_ID + " = _c2." + RUN_ID
            + " and _c1." + DATA_TIME + " = _c2." + DATA_TIME);
    selectPart = new StringBuilder("_c1." + RUN_ID + ", _c1." + DATA_TIME + ", _c1." + TIME_GRAIN);
    if (d1 != null) {
      String[] dims = d1.split(",");
      for (String dim : dims) {
        selectPart.append(",_c1." + dim);
        if (d2 != null && d2.contains(dim)) {
          fromPart.append(" and _c1." + dim + " = _c2." + dim);
        }
      }
    }

    switch (op) {
      case DIFF:
        selectPart.append(", (_c1." + DATA_VALUE + " - _c2." + DATA_VALUE + ") as " + DATA_VALUE);
        break;
      case RATIO:
        selectPart.append(", (_c1." + DATA_VALUE + " / _c2." + DATA_VALUE + ") as " + DATA_VALUE);
        break;
    }

    return getSql();
  }

  public List<String> preview() {
    // Add timestamp columns into formulas
    List<Formula> allFormulas = aggDef.getFormulas();

    List<String> ret = new ArrayList<>();

    if (aggDef.getIndDimensions() != null && !aggDef.getIndDimensions().isEmpty()) {
      for (List<String> dims : aggDef.getIndDimensions().keySet()) {
        ret.add(getAggSql(allFormulas, dims));

        List<Formula> nonAdtFormulas = aggDef.getNonAdtFormulas();
        if (nonAdtFormulas != null && !nonAdtFormulas.isEmpty()) {
          for (List<String> child : aggDef.getIndDimensions().get(dims)) {
            ret.add(getAggSql(nonAdtFormulas, child));
          }
        }
      }

      List<Formula> nonAdtOverallFormulas = aggDef.getNonAdtOverallFormulas();
      if (nonAdtOverallFormulas != null && !nonAdtOverallFormulas.isEmpty()) {
        ret.add(getAggSql(nonAdtOverallFormulas, null));
      }
    } else {
      ret.add(getAggSql(allFormulas, null));
    }

    return ret;
  }

  public String getInsertMetricDefSql() {
    String ret = "INSERT INTO dq_metric_def(name, agg_def_id, time_created, time_modified, metric_def_json) VALUES "
        + "($name, $agg_def_id, $time_created, $time_modified, $metric_def_json)";
    ret = replace(ret, "$name", metricDef.getName());
    ret = replace(ret, "$agg_def_id", String.valueOf(metricDef.getAggDefId()));
    ret = replace(ret, "$time_created", (new Timestamp(System.currentTimeMillis())).toString());
    ret = replace(ret, "$time_modified", null);
    ret = replace(ret, "$metric_def_json", metricDef.getOriginalJson().toString());
    return ret;
  }

  public String getInsertAggDefSql() {
    String ret =
        "INSERT INTO dq_agg_def(dataset_name, storage_type, owner, tag, frequency, flow_frequency, next_run, time_created, time_modified, update_time_column, event_time_column, time_dimension, time_grain, time_shift, comment, agg_def_json) "
            + "VALUES ($dataset_name, $storage_type, $owner, $tag, $frequency, $flow_frequency, $next_run, $time_created, $time_modified, $update_time_column, $event_time_column, $time_dimension, $time_grain, $time_shift, $comment, $agg_def_json)";
    ret = replace(ret, "$dataset_name", aggDef.getDataset().getName());
    ret = replace(ret, "$storage_type", aggDef.getDataset().getStorageType().toString());
    ret = replace(ret, "$owner", aggDef.getOwner());
    ret = replace(ret, "$tag", aggDef.getTag());
    ret = replace(ret, "$frequency", aggDef.getFrequency().toString());
    ret = replace(ret, "$flow_frequency",
        (aggDef.getFlowFrequency() == null) ? null : aggDef.getFlowFrequency().toString());
    if (!aggDef.getFrequency().equals(Frequency.DATA_TRIGGERED)) {
      if (aggDef.getStartTime() != null) {
        ret = replace(ret, "$next_run", aggDef.getStartTime().toString());
      } else {
        ret = replace(ret, "$next_run", (new Timestamp(System.currentTimeMillis())).toString());
      }
    } else {
      ret = replace(ret, "$next_run", null);
    }
    ret = replace(ret, "$time_created", (new Timestamp(System.currentTimeMillis())).toString());
    ret = replace(ret, "$time_modified", null);
    ret = replace(ret, "$update_time_column",
        (aggDef.getUpdateTs() == null) ? null : aggDef.getUpdateTs().getColumnName());
    ret =
        replace(ret, "$event_time_column", (aggDef.getEventTs() == null) ? null : aggDef.getEventTs().getColumnName());
    ret = replace(ret, "$time_dimension", (timeDimension == null) ? null : timeDimension.getColumnName());
    ret = replace(ret, "$time_grain", timeGrain.toString());
    ret = replace(ret, "$time_shift", (timeDimension == null) ? null : String.valueOf(timeDimension.getShift()));
    ret = replace(ret, "$comment", aggDef.getComment());
    ret = replace(ret, "$agg_def_json", aggDef.getOriginalJson().toString());
    return ret;
  }

  public String getInsertAggQuerySql(int aggDefId, List<Formula> formulas, List<String> dimensions) {

    String ret =
        "INSERT INTO dq_agg_query(agg_def_id, dataset_name, storage_type, formula, dimension, time_dimension, time_grain, generated_query) "
            + "VALUES ($agg_def_id, $dataset_name, $storage_type, $formula, $dimension, $time_dimension, $time_grain, $generated_query)";
    ret = replace(ret, "$agg_def_id", String.valueOf(aggDefId));
    ret = replace(ret, "$dataset_name", aggDef.getDataset().getName());
    ret = replace(ret, "$storage_type", aggDef.getDataset().getStorageType().toString());
    ret = replace(ret, "$formula", formulas.stream().map(a -> a.getAlias()).collect(Collectors.joining(",")));
    ret =
        replace(ret, "$dimension", (dimensions == null) ? null : dimensions.stream().collect(Collectors.joining(",")));
    ret = replace(ret, "$time_dimension", (timeDimension == null) ? null : timeDimension.getColumnName());
    ret = replace(ret, "$time_grain", timeGrain.toString());
    ret = replace(ret, "$generated_query", getAggSql(formulas, dimensions));
    return ret;
  }

  public String getInsertAggCombSql(int aggDefId, int aggQueryId, Formula formulas, List<String> dimensions,
      boolean rollUp) {
    String ret =
        "INSERT INTO dq_agg_comb(agg_def_id, agg_query_id, formula, data_type, dimension, time_dimension, time_grain, roll_up_func) "
            + "VALUES ($agg_def_id, $agg_query_id, $formula, $data_type, $dimension, $time_dimension, $time_grain, $roll_up_func)";
    ret = replace(ret, "$agg_def_id", String.valueOf(aggDefId));
    ret = replace(ret, "$agg_query_id", String.valueOf(aggQueryId));
    ret = replace(ret, "$formula", formulas.getAlias());
    ret = replace(ret, "$data_type", formulas.getDataType().toString());
    ret = replace(ret, "$dimension",
        (dimensions == null || dimensions.isEmpty()) ? null : dimensions.stream().collect(Collectors.joining(",")));
    ret = replace(ret, "$time_dimension", (timeDimension == null) ? null : timeDimension.getColumnName());
    ret = replace(ret, "$time_grain", timeGrain.toString());
    ret = replace(ret, "$roll_up_func", rollUp ? formulas.getRollupFunc() : null);
    return ret;
  }

  public static String replace(String s, String target, String replacement) {
    if (replacement != null) {
      return s.replace(target, "'" + replacement.replace("\'", "\\\'") + "'");
    } else {
      return s.replace(target, "null");
    }
  }

  public String add(List<TimeRange> ranges) {
    for (TimeRange range : ranges) {
      add(range);
    }

    return getSql();
  }

  public String add(TimeRange range) {
    timeRanges.add(range);
    return getSql();
  }

  public String clearTimeRanges() {
    setTimeRanges(new ArrayList<>());
    return getSql();
  }

  public String getAggSql(List<Formula> formulas, List<String> dims) {
    selectPart = new StringBuilder("");
    groupPart = new StringBuilder("");

    if (formulas != null && !formulas.isEmpty()) {
      for (Formula f : formulas) {
        selectPart.append(f.getExpr() + " as " + f.getAlias() + ",");
      }
    }

    if (timeDimension != null) {
      selectPart.append(timeDimension.getCastFormat() + " as " + DATA_TIME + ",");
      groupPart.append(timeDimension.getCastFormat() + ",");
    }

    if (dims != null && !dims.isEmpty()) {
      for (String d : dims) {
        selectPart.append(d + ",");
        groupPart.append(d + ",");
      }
    }

    if (selectPart.toString().endsWith(",")) {
      selectPart.deleteCharAt(selectPart.length() - 1);
    }
    if (groupPart.toString().endsWith(",")) {
      groupPart.deleteCharAt(groupPart.length() - 1);
    }

    return getSql();
  }

  public String getCreateAggStoreSql(int suffix, List<Formula> formulas, List<String> dims) {
    StringBuilder sb = new StringBuilder("CREATE TABLE dq_value_");
    StringBuilder pk = new StringBuilder("");
    sb.append(suffix);
    sb.append(
        " ( " + RUN_ID + " INT NOT NULL, " + DATA_TIME + " TIMESTAMP NULL DEFAULT NULL, " + TIME_GRAIN + " VARCHAR(127),");
    pk.append(RUN_ID + "," + DATA_TIME);

    if (dims != null && !dims.isEmpty()) {
      for (String dim : dims) {
        sb.append(dim + " VARCHAR(255), ");
        pk.append("," + dim);
      }
    }

    if (formulas != null && !formulas.isEmpty()) {
      for (Formula f : formulas) {
        sb.append(f.getAlias() + " ");
        sb.append(f.getDataType().getMyType() + ", ");
      }
    }

    sb.append(" PRIMARY KEY (" + pk.toString() + ") ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED ");

    return sb.toString();
  }

  private String getTimeFilter() {
    StringBuilder sb = new StringBuilder("");
    if (timeRanges.size() != 0) {
      sb.append("(" + DATA_TIME + ">='" + timeRanges.get(0).getStart() + "' and " + DATA_TIME + "<='" + timeRanges.get(0).getEnd() + "')");
    }

    for (int i = 1; i < timeRanges.size(); i++) {
      sb.append(" or (" + DATA_TIME + ">='" + timeRanges.get(i).getStart() + "' and " + DATA_TIME + "<='" + timeRanges.get(i).getEnd() + "')");
    }

    return sb.toString();
  }

  private String getTimeFilter(TimeRange range) {
    String comparatorStart = ">=", comparatorEnd = "<=";
    if (range.isExclusiveStart()) {
      comparatorStart = ">";
    }

    if (range.isExclusiveEnd()) {
      comparatorEnd = "<";
    }

    return "(" + DATA_TIME + comparatorStart + "'" + timeRanges.get(0).getStart() + "' and " + DATA_TIME + comparatorEnd + "'" + timeRanges.get(0).getEnd() + "')";
  }

  public String getSql() {
    StringBuilder sb = new StringBuilder("");
    if (selectPart.length() != 0) {
      sb.append(" SELECT " + selectPart);
    }

    if (fromPart.length() != 0) {
      sb.append(" FROM " + fromPart);
    }

    if (wherePart.length() != 0) {
      sb.append(" WHERE " + wherePart);
    }

    if (groupPart.length() != 0) {
      sb.append(" GROUP BY " + groupPart);
    }

    StringBuilder combinedHave = new StringBuilder("");

    if (timeRanges.size() != 0 ) {
      combinedHave.append("(" + getTimeFilter() + ")");
    }

    if (havePart.length() != 0) {
      if (combinedHave.length() != 0) {
        combinedHave.append(" and " + havePart);
      } else {
        combinedHave.append(havePart);
      }
    }

    if (combinedHave.length() != 0) {
      sb.append(" HAVING " + combinedHave);
    }

    if (orderPart.length() != 0) {
      sb.append(" ORDER BY " + orderPart);
    }

    return sb.toString();
  }

  public StringBuilder getSelectPart() {
    return selectPart;
  }

  public void setSelectPart(StringBuilder selectPart) {
    this.selectPart = selectPart;
  }

  public StringBuilder getFromPart() {
    return fromPart;
  }

  public void setFromPart(StringBuilder fromPart) {
    this.fromPart = fromPart;
  }

  public StringBuilder getWherePart() {
    return wherePart;
  }

  public void setWherePart(StringBuilder wherePart) {
    this.wherePart = wherePart;
  }

  public StringBuilder getGroupPart() {
    return groupPart;
  }

  public void setGroupPart(StringBuilder groupPart) {
    this.groupPart = groupPart;
  }

  public TimeDimension getTimeDimension() {
    return timeDimension;
  }

  public void setTimeDimension(TimeDimension timeDimension) {
    this.timeDimension = timeDimension;
  }

  public TimeGrain getTimeGrain() {
    return timeGrain;
  }

  public void setTimeGrain(TimeGrain timeGrain) {
    this.timeGrain = timeGrain;
  }

  public List<TimeRange> getTimeRanges() {
    return timeRanges;
  }

  public void setTimeRanges(List<TimeRange> timeRanges) {
    this.timeRanges = timeRanges;
  }
}
