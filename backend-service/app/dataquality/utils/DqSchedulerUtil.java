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
package dataquality.utils;

import akka.actor.Cancellable;
import dataquality.actors.DqActorRegistry;
import java.util.concurrent.TimeUnit;
import play.Play;
import scala.concurrent.duration.Duration;


/**
 * Created by zechen on 4/24/15.
 */
public class DqSchedulerUtil {


    public static Cancellable schedulerRef;

    public static synchronized void start() {
      start(Play.application().configuration().getLong("dq.check.interval"));
    }

    /**
     * Start system's scheduler
     * @param mins
     */
    public static synchronized void start(Long mins) {
      if (schedulerRef != null) {
        schedulerRef.cancel();
      }

      schedulerRef = DqActorRegistry.scheduler
          .schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(mins, TimeUnit.MINUTES),
              DqActorRegistry.schedulerActor, "checking", DqActorRegistry.dispatcher, null);
    }

    /**
     * Cancel system's scheduler
     */
    public static synchronized void cancel() {
      schedulerRef.cancel();
    }
}
