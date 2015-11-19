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
package metadata.etl.lineage;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import java.util.Arrays;
import org.codehaus.jettison.json.JSONException;
import wherehows.common.DatasetPath;
import wherehows.common.schemas.AzkabanJobExecRecord;
import wherehows.common.schemas.LineageRecord;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by zsun on 9/8/15.
 */
public class AzJsonAnalyzer {

  private static final String[] INPUT_KEYS =
    {"mapreduce.input.fileinputformat.inputdir", "pig.input.dirs", "mapred.input.dir"};
  private static final String[] OUTPUT_KEYS =
    {"mapreduce.output.fileoutputformat.outputdir", "pig.output.dirs", "mapred.output.dir"};

  Object document;
  AzkabanJobExecRecord aje;

  int appId;
  int defaultDatabaseId;

  public AzJsonAnalyzer(String jsonString, AzkabanJobExecRecord aje, int defaultDatabaseId) {
    document = Configuration.defaultConfiguration().jsonProvider().parse(jsonString);
    this.aje = aje;
    this.appId = aje.getAppId();
    this.defaultDatabaseId = defaultDatabaseId;
  }

  /**
   * Extract all related info from json
   * @return
   */
  public List<LineageRecord> extractFromJson()
    throws JSONException {

    List<LineageRecord> results = new ArrayList<>();
    List<String> inputs = parseInputs();
    // TODO get the count of insert, update, delete
    for (String s : inputs) {
      results.add(construct(s, "source", "read", null, null, null, null));
    }
    List<String> outputs = parseOutputs();
    for (String s : outputs) {
      results.add(construct(s, "target", "write",null, null, null, null));
    }
    return results;
  }

  private LineageRecord construct(String fullPath, String sourceTargetType, String operation, Long recordCount,
    Long insertCount, Long deleteCount, Long updateCount) {
    LineageRecord lineageRecord =
      new LineageRecord(this.appId, this.aje.getFlowExecId(), this.aje.getJobName(), this.aje.getJobExecId());

    lineageRecord.setDatasetInfo(this.defaultDatabaseId, fullPath, "HDFS");
    lineageRecord.setOperationInfo(sourceTargetType, operation, recordCount, insertCount, deleteCount, updateCount,
      this.aje.getStartTime(), this.aje.getEndTime(), this.aje.getFlowPath());

    return lineageRecord;
  }

  // The string could be a comma separated file path.
  public List<String> sepCommaString(List<String> originalStrings) {
    List<String> result = new ArrayList<>();
    for (String concatedString : originalStrings) {
      result.addAll(DatasetPath.separatedDataset(concatedString));
    }
    return result;
  }

  /**
   * Extract the input/output of a hadoop job from configure json file
   * @return a list of values
   */
  public List<String> parseInputs() {
    return sepCommaString(parseProperties(INPUT_KEYS));
  }

  public List<String> parseOutputs() {
    return sepCommaString(parseProperties(OUTPUT_KEYS));
  }

  private List<String> parseProperties(String[] propertyNames) {
    StringBuilder query = new StringBuilder();
    for (String s : propertyNames) {
      query.append("@.name==");
      query.append(s);
      query.append("||");
    }
    query.delete(query.length() - 2, query.length());
    List<String> result = JsonPath.read(document, "$.conf.property[?(" + query.toString() + ")].value");
    return result;
  }
}
