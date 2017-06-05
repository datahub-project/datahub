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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import metadata.etl.Launcher;
import metadata.etl.models.EtlJobStatus;
import models.daos.EtlJobDao;
import models.daos.EtlJobPropertyDao;
import msgs.EtlJobMessage;
import play.Logger;
import play.Play;
import shared.Global;
import wherehows.common.Constant;


/**
 * Created by zechen on 9/4/15.
 */
public class EtlJobActor extends UntypedActor {

  private Process process;

  private static final String ETL_TEMP_DIR = Play.application().configuration().getString("etl.temp.dir");

  @Override
  public void onReceive(Object message) throws Exception {
    final String configDir = ETL_TEMP_DIR + "/exec";
    Properties props = null;
    if (message instanceof EtlJobMessage) {
      EtlJobMessage msg = (EtlJobMessage) message;
      try {
        props = msg.getEtlJobProperties();
        Properties whProps = EtlJobPropertyDao.getWherehowsProperties();
        props.putAll(whProps);
        props.setProperty(Constant.WH_APP_FOLDER_KEY, ETL_TEMP_DIR);
        props.setProperty(Launcher.WH_ETL_EXEC_ID_KEY, String.valueOf(msg.getWhEtlExecId()));

        EtlJobDao.startRun(msg.getWhEtlExecId(), "Job started!");

        // start a new process here
        final ProcessBuilder pb =
            ConfigUtil.buildProcess(msg.getEtlJobName(), msg.getWhEtlExecId(), msg.getCmdParam(), props);
        Logger.debug("run command : " + pb.command() + " ; timeout: " + msg.getTimeout());

        ConfigUtil.generateProperties(msg.getWhEtlExecId(), props, configDir);
        int retry = 0;
        int execResult = 0;
        Boolean execFinished = false;
        String line;

        while (retry < 3) {
          long startTime = System.currentTimeMillis();
          process = pb.start();

          // update process id and hostname for started job
          EtlJobDao.updateJobProcessInfo(msg.getWhEtlExecId(), getPid(process), getHostname());

          // wait until this process finished.
          execFinished = process.waitFor(msg.getTimeout(), TimeUnit.SECONDS);
          if (execFinished) {
            execResult = process.exitValue();
          }
          long elapsedTime = System.currentTimeMillis() - startTime;

          // For process error such as ImportError or ArgumentError happening shortly after starting, retry
          if (execResult == 2 && elapsedTime < 10000) {
            retry++;
            Logger.error("*** Process + " + getPid(process) + " failed, status: " + execResult + ". Retry " + retry);

            if (process.isAlive()) {
              process.destroy();
            }
            Thread.sleep(10000);
          } else {
            break;
          }
        }

        // if the process timeout and forcibly terminated, log the error and throw exception
        if (!execFinished) {
          Logger.error("*** Process + " + getPid(process) + " timeout");
          throw new Exception("Process + " + getPid(process) + " timeout");
        }

        // if the process failed, log the error and throw exception
        if (execResult > 0) {
          BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
          String errString = "Error Details:\n";
          while ((line = br.readLine()) != null) {
            errString = errString.concat(line).concat("\n");
          }
          Logger.error("*** Process + " + getPid(process) + " failed, status: " + execResult);
          Logger.error(errString);
          throw new Exception("Process + " + getPid(process) + " failed");
        }

        EtlJobDao.endRun(msg.getWhEtlExecId(), EtlJobStatus.SUCCEEDED, "Job succeed!");
        Logger.info("ETL job {} finished", msg.toDebugString());

        if (props.getProperty(Constant.REBUILD_TREE_DATASET) != null) {
          ActorRegistry.treeBuilderActor.tell("dataset", getSelf());
        }

        if (props.getProperty(Constant.REBUILD_TREE_FLOW) != null) {
          ActorRegistry.treeBuilderActor.tell("flow", getSelf());
        }
      } catch (Throwable e) { // catch all throwable at the highest level.
        e.printStackTrace();
        Logger.error("ETL job {} got a problem", msg.toDebugString());
        if (process.isAlive()) {
          process.destroy();
        }
        EtlJobDao.endRun(msg.getWhEtlExecId(), EtlJobStatus.ERROR, e.getMessage());
      } finally {
        Global.removeRunningJob(msg.getEtlJobName());
        if (!Logger.isDebugEnabled()) // if debug enable, won't delete the config files.
        {
          ConfigUtil.deletePropertiesFile(msg.getWhEtlExecId(), configDir);
        }
      }
    }
  }

  /**
   * Reflection to get the pid
   *
   * @param process {@code Process}
   * @return pid, -1 if not found
   */
  private static int getPid(Process process) {
    try {
      Class<?> cProcessImpl = process.getClass();
      Field fPid = cProcessImpl.getDeclaredField("pid");
      if (!fPid.isAccessible()) {
        fPid.setAccessible(true);
      }
      return fPid.getInt(process);
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * Return the hostname of the machine
   * @return hostname, null if unknown
   */
  private static String getHostname() {
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
      return null;
    }
  }
}
