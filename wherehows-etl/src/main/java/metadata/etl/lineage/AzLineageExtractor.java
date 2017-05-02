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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;
import wherehows.common.LineageCombiner;
import wherehows.common.schemas.LineageRecord;


/**
 * Created by zsun on 9/23/15.
 */
public class AzLineageExtractor {

  private static final Logger logger = LoggerFactory.getLogger(AzLineageExtractor.class);
  /**
   * Get one azkaban job's lineage.
   * Process :
   * 1 get execution log from azkaban service
   * 2 get hadoop job id from execution log
   * 3 get input, output from execution log and hadoop conf, normalize the path
   * 4 construct the Lineage Record
   *
   * @return one azkaban job's lineage
   */
  public static List<LineageRecord> extractLineage(AzExecMessage message)
    throws Exception {

    List<LineageRecord> oneAzkabanJobLineage = new ArrayList<>();

    // azkaban job name should have subflow name append in front
    String[] flowSequence = message.azkabanJobExecution.getFlowPath().split(":")[1].split("/");
    String jobPrefix = "";
    for (int i = 1; i < flowSequence.length; i++) {
      jobPrefix += flowSequence[i] + ":";
    }
    //String log = asc.getExecLog(azJobExec.execId, azJobExec.jobName);
    String log =
      message.adc.getExecLog(message.azkabanJobExecution.getFlowExecId(), jobPrefix + message.azkabanJobExecution.getJobName());
    Set<String> hadoopJobIds = AzLogParser.getHadoopJobIdFromLog(log);

    for (String hadoopJobId : hadoopJobIds) {
      logger.debug("Get Hadoop job config: {} from Azkaban job: {}" + hadoopJobId, message.azkabanJobExecution.toString());
      // TODO persist this mapping?
      String confJson = message.hnne.getConfFromHadoop(hadoopJobId);
      AzJsonAnalyzer ja = new AzJsonAnalyzer(confJson, message.azkabanJobExecution,
        Integer.valueOf(message.prop.getProperty(Constant.AZ_DEFAULT_HADOOP_DATABASE_ID_KEY)));
      List<LineageRecord> oneHadoopJobLineage = ja.extractFromJson();
      oneAzkabanJobLineage.addAll(oneHadoopJobLineage);
    }

    // normalize and combine the path
    LineageCombiner lineageCombiner = new LineageCombiner(message.connection);
    lineageCombiner.addAll(oneAzkabanJobLineage);
    Integer defaultDatabaseId = Integer.valueOf(message.prop.getProperty(Constant.AZ_DEFAULT_HADOOP_DATABASE_ID_KEY));
    List<LineageRecord> lineageFromLog = AzLogParser.getLineageFromLog(log, message.azkabanJobExecution, defaultDatabaseId);
    lineageCombiner.addAll(lineageFromLog);

    return lineageCombiner.getCombinedLineage();
  }

  /**
   * Extract and write to database
   * @param message
   * @throws Exception
   */
  public static void extract(AzExecMessage message)
    throws Exception {
    try{
      List<LineageRecord> result = extractLineage(message);
      for (LineageRecord lr : result) {
        message.databaseWriter.append(lr);
      }
      logger.info(String.format("%03d lineage records extracted from [%s]", result.size(), message.toString()));
      message.databaseWriter.flush();
    } catch (Exception e) {
      logger.error(String.format("Failed to extract lineage info from [%s].\n%s", message.toString(), e.getMessage()));
    }
  }
}
