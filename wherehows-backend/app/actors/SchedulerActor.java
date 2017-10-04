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
package actors;

import akka.actor.UntypedActor;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import wherehows.common.jobs.JobStatus;
import models.daos.EtlJobDao;
import msgs.EtlJobMessage;
import play.Logger;
import play.Play;
import shared.Global;
import wherehows.common.utils.JobsUtil;

import static wherehows.common.Constant.*;


/**
 * Created by zechen on 9/3/15.
 */
public class SchedulerActor extends UntypedActor {

  public static final String ETL_JOBS_DIR = Play.application().configuration().getString(WH_ETL_JOBS_DIR);

  /**
   * Search for etl jobs that are ready to run and update the time for next run
   * @param message
   * @throws Exception
   */
  @Override
  public void onReceive(Object message) throws Exception {
    if (message.equals("checking")) {
      runDueJobs();
    }
  }

  private Map<String, Long> getScheduledJobs() throws Exception {
    Map<String, Long> map = new HashMap<>();
    for (Map<String, Object> job : EtlJobDao.getAllScheduledJobs()) {
      String jobName = (String) job.get("wh_etl_job_name");
      Boolean enabled = (Boolean) job.get("enabled");
      Long nextRun = (Long) job.get("next_run");
      // filter for only enabled jobs
      if (enabled != null && enabled) {
        map.put(jobName, nextRun);
      }
    }
    return map;
  }

  private void runDueJobs() throws Exception {
    Map<String, Properties> enabledJobs = JobsUtil.getScheduledJobs(ETL_JOBS_DIR);
    Logger.info("Enabled jobs: {}", enabledJobs.keySet());

    Map<String, Long> scheduledJobs = getScheduledJobs();
    Logger.info("Scheduled jobs: {}", scheduledJobs);

    long now = System.currentTimeMillis() / 1000;
    for (Map.Entry<String, Properties> entry : enabledJobs.entrySet()) {
      String etlJobName = entry.getKey();
      Properties properties = entry.getValue();
      EtlJobMessage etlMsg = new EtlJobMessage(etlJobName, properties);

      // Schedule next run if a cron expr is defined.
      String cronExpr = etlMsg.getCronExpr();
      if (cronExpr != null) {
        EtlJobDao.updateNextRun(etlJobName, cronExpr, new Date());
      }

      if (scheduledJobs.getOrDefault(etlJobName, Long.MAX_VALUE) > now) {
        continue;
      }

      Logger.info("Running job: {}", etlJobName);

      Long whExecId = EtlJobDao.insertNewRun(etlJobName);
      etlMsg.setWhEtlExecId(whExecId);

      StringBuilder s = new StringBuilder("Current running jobs : ");
      for (String j : Global.getCurrentRunningJob()) {
        s.append(j).append("\t");
      }
      Logger.info(s.toString());

      if (Global.getCurrentRunningJob().contains(etlJobName)) {
        Logger.error("The previous job is still running! Abort this job : " + etlMsg.toDebugString());
        EtlJobDao.endRun(etlMsg.getWhEtlExecId(), JobStatus.ERROR, "Previous is still running, Aborted!");
      } else {
        Global.getCurrentRunningJob().add(etlJobName);
        Logger.info("Send message : " + etlMsg.toDebugString());
        ActorRegistry.etlJobActor.tell(etlMsg, getSelf());
      }
    }
  }
}

