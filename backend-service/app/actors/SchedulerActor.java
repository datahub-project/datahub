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
import java.util.List;
import java.util.Map;
import models.EtlType;
import models.EtlJobName;
import models.RefIdType;
import msgs.EtlJobMessage;
import play.Logger;
import play.libs.Json;
import models.daos.EtlJobDao;


/**
 * Created by zechen on 9/3/15.
 */
public class SchedulerActor extends UntypedActor {
  public static final String MESSAGE = "checking-etl";

  /**
   * Search for etl jobs that are ready to run and update the time for next run
   * @param message
   * @throws Exception
   */
  @Override
  public void onReceive(Object message)
    throws Exception {
    if (MESSAGE.equals(message)) {
      List<Map<String, Object>> dueJobs = EtlJobDao.getDueJobs();
      Logger.info("running " + dueJobs.size() + " jobs");
      for (Map<String, Object> dueJob : dueJobs) {
        Integer whEtlJobId = ((Long) dueJob.get("wh_etl_job_id")).intValue();
        EtlJobName etlJobName = EtlJobName.valueOf((String) dueJob.get("wh_etl_job_name"));
        EtlType etlType = EtlType.valueOf((String) dueJob.get("wh_etl_type"));
        Integer refId = (Integer) dueJob.get("ref_id");
        RefIdType refIdType = RefIdType.valueOf((String) dueJob.get("ref_id_type"));
        EtlJobMessage etlMsg = new EtlJobMessage(etlJobName, etlType, whEtlJobId, refId, refIdType);
        if (dueJob.get("input_params") != null) {
          etlMsg.setInputParams(Json.parse((String) dueJob.get("input_params")));
        }

        EtlJobDao.updateNextRun(whEtlJobId, (String) dueJob.get("cron_expr"), new Date());
        Long whExecId = EtlJobDao.insertNewRun(whEtlJobId);
        etlMsg.setWhEtlExecId(whExecId);
        Logger.info("Send message : " + etlMsg.toDebugString());
        ActorRegistry.etlJobActor.tell(etlMsg, getSelf());
      }
    }
  }


}

