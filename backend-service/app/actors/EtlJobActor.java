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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;
import metadata.etl.EtlJob;
import models.EtlJobStatus;
import models.daos.EtlJobDao;
import models.daos.EtlJobPropertyDao;
import msgs.EtlJobMessage;
import play.Logger;



/**
 * Created by zechen on 9/4/15.
 */
public class EtlJobActor extends UntypedActor {

  @Override
  public void onReceive(Object message)
    throws Exception {

    if (message instanceof EtlJobMessage) {
      EtlJobMessage msg = (EtlJobMessage) message;
      try {
        Properties props = EtlJobPropertyDao.getJobProperties(msg.getEtlJobName(), msg.getRefId());
        Properties whProps = EtlJobPropertyDao.getWherehowsProperties();
        props.putAll(whProps);
        EtlJob etlJob = EtlJobFactory.getEtlJob(msg.getEtlJobName(), msg.getRefId(), msg.getWhEtlExecId(), props);
        EtlJobDao.startRun(msg.getWhEtlExecId(), "Job started!");
        etlJob.run();
        EtlJobDao.endRun(msg.getWhEtlExecId(), EtlJobStatus.SUCCEEDED, "Job succeed!");
        Logger.info("ETL job {} finished", msg.toDebugString());
        if (msg.getEtlJobName().affectDataset()) {
          ActorRegistry.treeBuilderActor.tell("dataset", getSelf());
        }

        if (msg.getEtlJobName().affectFlow()) {
          ActorRegistry.treeBuilderActor.tell("flow", getSelf());
        }
      } catch (Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        Logger.error(sw.toString());
        e.printStackTrace();
        EtlJobDao.endRun(msg.getWhEtlExecId(), EtlJobStatus.ERROR, e.getMessage());
      }
    }
  }
}
