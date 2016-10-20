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
package shared;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import play.Application;
import play.GlobalSettings;
import play.Logger;
import play.Play;
import utils.SchedulerUtil;


/**
 * Created by zechen on 9/3/15.
 */
public class Global extends GlobalSettings {

  // the jobs id that allowed to run on this instance
  private static Set<Integer> whiteList;
  private static Set<Integer> currentRunningJob;
  @Override
  public void onStart(Application arg0) {
    Logger.info("on start---===");

    List<Integer> whiteListList = Play.application().configuration().getIntList("scheduler.jobid.whitelist", null);
    if (whiteListList != null) {
      whiteList = new HashSet<>(whiteListList);
    } else {
      whiteList = null;
    }
    SchedulerUtil.start();

    currentRunningJob = new HashSet<>();
  }

  public static Set<Integer> getWhiteList() {
    return whiteList;
  }

  public static Set<Integer> getCurrentRunningJob() {
    return currentRunningJob;
  }

  public static void setCurrentRunningJob(Set<Integer> currentRunningJob) {
    Global.currentRunningJob = currentRunningJob;
  }

  public static void removeRunningJob(int jobId) {
    if (currentRunningJob.contains(jobId)) {
      currentRunningJob.remove(jobId);
    }
  }
}
