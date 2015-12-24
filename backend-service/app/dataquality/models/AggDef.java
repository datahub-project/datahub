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
package dataquality.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import dataquality.models.enums.Frequency;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import play.Logger;


/**
 * Created by zechen on 5/20/15.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class AggDef implements Serializable {

  public final static String MAX_TIME_PREFIX = "_max_";
  public final static String MIN_TIME_PREFIX = "_min_";

  // The dataset
  DataSet dataset;

  // View query
  String view;

  // Aggregation formulas
  List<Formula> formulas;

  // List of dimensions combinations
  List<List<String>> dimensions;

  @JsonIgnore
  // List of independent dimensions combinations
  Map<List<String>, List<List<String>>> indDimensions;

  // Filter expression in where clause
  String filter;

  // The column name which indicate when the data is loaded into database
  TimeColumn updateTs;

  // The column which indicate when the event is taking place
  TimeColumn eventTs;

  // Time partition column
  TimeColumn timePartition;

  TimeDimension timeDimension = null;

  Timestamp startTime = null;

  // how often the aggregation will run
  Frequency frequency = Frequency.DATA_TRIGGERED;

  // how often the flow will refresh the dataset
  Frequency flowFrequency;

  String owner;

  String tag;

  String comment;

  @JsonIgnore
  JsonNode originalJson;

  public AggDef(JsonNode json) throws IllegalArgumentException {

    this.originalJson = json.deepCopy();

    ObjectMapper mapper = new ObjectMapper();
    mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    try {
      this.dataset = mapper.convertValue(json.findPath("dataset"), DataSet.class);
    } catch (IllegalArgumentException ilaE) {
      ilaE.printStackTrace();
      throw new IllegalArgumentException("Invalid dataset attributes: " + ilaE.getMessage());
    }

    if (dataset == null) {
      throw new IllegalArgumentException("dataset attribute is required");
    }

    if (!(json.findPath("view") instanceof MissingNode)) {
      String v = json.findPath("view").textValue().trim();
      if (!v.isEmpty()) {
        setView(v);
      }
    }

    try {
      this.formulas = mapper.convertValue(json.findPath("formulas"),
          TypeFactory.defaultInstance().constructCollectionType(List.class, Formula.class));
    } catch (IllegalArgumentException ilaE) {
      ilaE.printStackTrace();
      throw new IllegalArgumentException("Invalid formulas attribute: " + ilaE.getMessage());
    }

    if (formulas == null) {
      throw new IllegalArgumentException("formulas attribute is required");
    }

    // Accept single dimension without "[]" symbol
    List rawDim = mapper.convertValue(json.findPath("dimensions"), List.class);
    if (rawDim != null && !rawDim.isEmpty()) {
      this.dimensions = new ArrayList<>(rawDim.size());
      for (Object obj : rawDim) {
        if (obj instanceof List) {
          this.dimensions.add((List<String>) obj);
        } else {
          this.dimensions.add(Arrays.asList((String) obj));
        }
      }
    }

    if (!(json.findPath("filter") instanceof MissingNode)) {
      String f = json.findPath("filter").textValue().trim();
      if (!f.isEmpty()) {
        setFilter(f);
      }
    }

    TimeColumn u = mapper.convertValue(json.findPath("update_ts"), TimeColumn.class);
    if (u != null && !u.getColumnName().trim().isEmpty()) {
      setUpdateTs(u);
      this.formulas.addAll(createTimeFormulas(u));
    }

    TimeColumn e = mapper.convertValue(json.findPath("event_ts"), TimeColumn.class);
    if (e != null && !e.getColumnName().trim().isEmpty()) {
      setEventTs(e);
      this.formulas.addAll(createTimeFormulas(e));
      if (e.getAggGrain() != null) {
        timeDimension = new TimeDimension(e.getColumnName(), e.getTruncTime(), e.getAggTimezone(), e.getAggGrain(), e.getShift());
      }
    }

    TimeColumn p = mapper.convertValue(json.findPath("time_partition"), TimeColumn.class);
    if (p != null && !p.getColumnName().trim().isEmpty()) {
      setTimePartition(p);
      timeDimension = new TimeDimension(p.getColumnName(), p.getConvertedTime(), p.getDataTimezone(), p.getDataGrain(), p.getShift());
    }

    if (!(json.findPath("frequency") instanceof MissingNode)) {
      setFrequency(json.findPath("frequency").textValue());
    }

    if (!(json.findPath("flow_frequency") instanceof MissingNode)) {
      setFlowFrequency(json.findPath("flow_frequency").textValue());
    }


    if (!(json.findPath("owner") instanceof MissingNode)) {
      String o = json.findPath("owner").textValue().trim();
      if (!o.isEmpty()) {
        setOwner(o);
      }
    }

    if (getOwner() == null && !json.findPath("user").findPath("username").isMissingNode()) {
      Logger.info("owner is not specified, set to default ldap login user : " + json.findPath("user"));
      setOwner(json.findPath("user").findPath("username").textValue());
    }

    if (!(json.findPath("tag") instanceof MissingNode)) {
      String t = json.findPath("tag").textValue().trim();
      if (!t.isEmpty()) {
        setTag(t);
      }
    }

    if (!(json.findPath("comment") instanceof MissingNode)) {
      String c = json.findPath("comment").textValue().trim();
      if (!c.isEmpty()) {
        setComment(c);
      }
    }

    if (!(json.findPath("start_time") instanceof MissingNode)) {
      String t = json.findPath("start_time").textValue().trim();
      if (!t.isEmpty()) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        try {
          Date date = format.parse(t);
          setStartTime(new Timestamp(date.getTime()));
        } catch (ParseException pe) {
          pe.printStackTrace();
          throw new IllegalArgumentException("Invalid start_time format, use 'yyyy-MM-dd HH:mm' format " + pe.getMessage());
        }
      }
    }

    if (this.dimensions != null && !this.dimensions.isEmpty()) {
      setIndDimensions(findIndDims(this.dimensions));
    }
  }


  public static Map<List<String>, List<List<String>>> findIndDims(List<List<String>> dimensions) {
    Map<List<String>, List<List<String>>> ret = new HashMap<>();

    Collections.sort(dimensions, (List<String> l1, List<String> l2) -> (l2.size() - l1.size()));

    for (List<String> dim : dimensions) {
      boolean found = false;
      // check if current dim is contained in previous dim
      for (List<String> prevAdded : ret.keySet()) {
        if (prevAdded.containsAll(dim)) {
          ret.get(prevAdded).add(dim);
          found = true;
          break;
        }
      }

      // check if it is an independent dimension
      if (!found) {
        ret.put(dim, new ArrayList<>());
      }
    }

    return ret;
  }

  @JsonIgnore
  public Map<List<String>, ObjectNode> getIndJsons() {
    Map<List<String>, ObjectNode> indJsons = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();
    if (indDimensions != null && !indDimensions.isEmpty()) {

      indJsons = new HashMap<>();
      ObjectNode tmp = mapper.convertValue(this.originalJson, ObjectNode.class);

      for (List<String> key : indDimensions.keySet()) {
        tmp.remove("dimensions");
        tmp.putArray("dimensions").addAll((ArrayNode) mapper.valueToTree(indDimensions.get(key)));
        indJsons.put(key, tmp.deepCopy());
      }
    }

    return indJsons;
  }

  @JsonIgnore
  public List<Formula> getAdtFormulas() {
    List<Formula> ret = new ArrayList<>();
    for (Formula f : this.formulas) {
      if (f.isAdditive()) {
        ret.add(f);
      }
    }

    return ret;
  }

  @JsonIgnore
  public List<Formula> getNonAdtFormulas() {
    List<Formula> ret = new ArrayList<>();
    for (Formula f : this.formulas) {
      if (!f.isAdditive()) {
        ret.add(f);
      }
    }
    return ret;
  }

  @JsonIgnore
  public List<Formula> getNonAdtOverallFormulas() {
    List<Formula> ret = new ArrayList<>();
    for (Formula f : this.formulas) {
      if (!f.isAdditive() && f.isOverall()) {
        ret.add(f);
      }
    }
    return ret;
  }

  public List<Formula> createTimeFormulas(TimeColumn tc) {
    List<Formula> ret = new ArrayList<>();

    Formula f1 = new Formula();
    f1.setExpr("max(" + tc.getColumnName() + ")");
    f1.setDataType(tc.getDataType());
    f1.setAlias(MAX_TIME_PREFIX + tc.getColumnName());
    f1.setOverall(true);
    ret.add(f1);
    Formula f2 = new Formula();
    f2.setExpr("min(" + tc.getColumnName() + ")");
    f2.setDataType(tc.getDataType());
    f2.setAlias(MIN_TIME_PREFIX + tc.getColumnName());
    f2.setOverall(true);
    ret.add(f2);

    return ret;
  }

  public DataSet getDataset() {
    return dataset;
  }

  public void setDataset(DataSet dataset) {
    this.dataset = dataset;
  }

  public List<Formula> getFormulas() {
    return formulas;
  }

  public void setFormulas(List<Formula> formulas) {
    this.formulas = formulas;
  }

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public String getView() {
    return view;
  }

  public void setView(String view) {
    this.view = view;
  }

  public List getDimensions() {
    return dimensions;
  }

  public void setDimensions(List dimensions) {
    this.dimensions = dimensions;
  }

  public Frequency getFrequency() {
    return frequency;
  }

  @JsonIgnore
  public void setFrequency(Frequency frequency) {
    this.frequency = frequency;
  }

  @JsonProperty
  public void setFrequency(String frequency) {
    String freqUpper = frequency.trim().toUpperCase();
    if (freqUpper.isEmpty()) {
      return;
    }
    setFrequency(Frequency.valueOf(freqUpper));
  }

  public Map<List<String>, List<List<String>>> getIndDimensions() {
    return indDimensions;
  }

  public void setIndDimensions(Map<List<String>, List<List<String>>> indDimensions) {
    this.indDimensions = indDimensions;
  }

  public TimeColumn getUpdateTs() {
    return updateTs;
  }

  public void setUpdateTs(TimeColumn updateTs) {
    this.updateTs = updateTs;
  }

  public TimeColumn getEventTs() {
    return eventTs;
  }

  public void setEventTs(TimeColumn eventTs) {
    this.eventTs = eventTs;
  }

  public TimeColumn getTimePartition() {
    return timePartition;
  }

  public void setTimePartition(TimeColumn timePartition) {
    this.timePartition = timePartition;
  }

  public JsonNode getOriginalJson() {
    return originalJson;
  }

  public void setOriginalJson(JsonNode originalJson) {
    this.originalJson = originalJson;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public Timestamp getStartTime() {
    return startTime;
  }

  public void setStartTime(Timestamp startTime) {
    this.startTime = startTime;
  }

  public TimeDimension getTimeDimension() {
    return timeDimension;
  }

  public void setTimeDimension(TimeDimension timeDimension) {
    this.timeDimension = timeDimension;
  }

  public Frequency getFlowFrequency() {
    return flowFrequency;
  }

  @JsonIgnore
  public void setFlowFrequency(Frequency flowFrequency) {
    this.flowFrequency = flowFrequency;
  }

  @JsonProperty
  public void setFlowFrequency(String flowFrequency) {
    String freqUpper = flowFrequency.trim().toUpperCase();
    if (freqUpper.isEmpty()) {
      return;
    }
    setFlowFrequency(Frequency.valueOf(freqUpper));
  }
}
