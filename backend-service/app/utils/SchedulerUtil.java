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
package utils;

import actors.ActorRegistry;
import actors.SchedulerActor;
import akka.actor.Cancellable;
import dataquality.actors.DqActorRegistry;
import dataquality.actors.DqSchedulerActor;
import java.util.concurrent.TimeUnit;
import play.Play;
import scala.concurrent.duration.Duration;


/**
 * Created by zechen on 9/3/15.
 */
public class SchedulerUtil {

  private static Cancellable etlSchedulerRef;
  private static Cancellable dqSchedulerRef;
  /**
   * Start Metadata Etl's scheduler
   */
  public static synchronized void startEtl() {
    startEtl(Play.application().configuration().getLong("etl.check.interval"));
  }

  /**
   * Start Metadata Etl's scheduler
   * @param mins
   */
  public static synchronized void startEtl(Long mins) {
    if (etlSchedulerRef != null) {
      etlSchedulerRef.cancel();
    }

    etlSchedulerRef = ActorRegistry.scheduler
      .schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(mins, TimeUnit.MINUTES),
        ActorRegistry.schedulerActor, SchedulerActor.MESSAGE, ActorRegistry.dispatcher, null);
  }

  /**
   * Cancel Metadata Etl's scheduler
   */
  public static synchronized void cancelEtl() {
    etlSchedulerRef.cancel();
  }

  /**
   * Start DQ system's scheduler
   */
  public static synchronized void startDq() {
    startDq(Play.application().configuration().getLong("dq.check.interval"));
  }

  /**
   * Start DQ system's scheduler
   * @param mins
   */
  public static synchronized void startDq(Long mins) {
    if (dqSchedulerRef != null) {
      dqSchedulerRef.cancel();
    }

    dqSchedulerRef = DqActorRegistry.scheduler
        .schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(mins, TimeUnit.MINUTES),
            DqActorRegistry.schedulerActor, DqSchedulerActor.MESSAGE, DqActorRegistry.dispatcher, null);
  }

  /**
   * Cancel DQ system's scheduler
   */
  public static synchronized void cancelDq() {
    dqSchedulerRef.cancel();
  }
}
