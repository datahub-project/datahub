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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import akka.routing.SmallestMailboxPool;
import akka.util.Timeout;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import wherehows.common.Constant;
import wherehows.common.PathAnalyzer;
import wherehows.common.schemas.AzkabanJobExecRecord;
import wherehows.common.writers.DatabaseWriter;


/**
 * Created by zsun on 8/29/15.
 */
@Slf4j
public class AzLineageExtractorMaster {

  Properties prop;

  public AzLineageExtractorMaster(Properties prop) throws Exception {
    this.prop = prop;
  }

  /**
   * Default 10 minutes
   * @throws Exception
   */
  public void run() throws Exception {
    run(10);
  }

  public void run(int timeFrame) throws Exception {
    run(timeFrame, System.currentTimeMillis());
  }

  /**
   * Entry point.
   * All recent finished azkaban jobs' lineage. Will write to database stagging table
   * @param timeFrame in minutes
   * @param endTimeStamp in millisecond
   * @throws Exception
   */
  public void run(int timeFrame, long endTimeStamp) throws Exception {
    // get recent finished job
    AzJobChecker azJobChecker = new AzJobChecker(prop);
    List<AzkabanJobExecRecord> jobExecList = azJobChecker.getRecentFinishedJobFromFlow(timeFrame, endTimeStamp);
    azJobChecker.close();
    log.info("Total number of azkaban jobs : {}", jobExecList.size());

    ActorSystem actorSystem = ActorSystem.create("LineageExtractor");
    int numOfActor = Integer.valueOf(prop.getProperty(Constant.LINEAGE_ACTOR_NUM, "50"));
    ActorRef lineageExtractorActor =
        actorSystem.actorOf(new SmallestMailboxPool(numOfActor).props(Props.create(AzLineageExtractorActor.class)),
            "lineageExtractorActor");

    // initialize
    //AzkabanServiceCommunicator asc = new AzkabanServiceCommunicator(prop);
    HadoopJobHistoryNodeExtractor hnne = new HadoopJobHistoryNodeExtractor(prop);
    AzDbCommunicator adc = new AzDbCommunicator(prop);

    String wherehowsUrl = prop.getProperty(Constant.WH_DB_URL_KEY);
    String wherehowsUserName = prop.getProperty(Constant.WH_DB_USERNAME_KEY);
    String wherehowsPassWord = prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
    Connection conn = DriverManager.getConnection(wherehowsUrl, wherehowsUserName, wherehowsPassWord);
    DatabaseWriter databaseWriter =
        new DatabaseWriter(wherehowsUrl, wherehowsUserName, wherehowsPassWord, "stg_job_execution_data_lineage");

    AzLogParser.initialize(conn);
    PathAnalyzer.initialize(conn);
    int timeout = 30; // default 30 minutes for one job
    if (prop.containsKey(Constant.LINEAGE_ACTOR_TIMEOUT_KEY)) {
      timeout = Integer.valueOf(prop.getProperty(Constant.LINEAGE_ACTOR_TIMEOUT_KEY));
    }
    List<Future<Object>> result = new ArrayList<>();
    for (AzkabanJobExecRecord aje : jobExecList) {
      AzExecMessage message = new AzExecMessage(aje, prop);
      message.asc = null;
      message.hnne = hnne;
      message.adc = adc;
      message.databaseWriter = databaseWriter;
      message.connection = conn;
      Timeout t = new Timeout(timeout, TimeUnit.SECONDS);
      Future<Object> fut = Patterns.ask(lineageExtractorActor, message, t);
      result.add(fut);
    }

    // join all threads
    Future<Iterable<Object>> seq = Futures.sequence(result, actorSystem.dispatcher());
    try {
      Await.result(seq, Duration.create(timeout + " seconds"));
    } catch (TimeoutException exception) {
      exception.printStackTrace();
    }

    adc.close();
    hnne.close();
    databaseWriter.close();
    log.info("All job finished lineage collecting!");
  }
}
