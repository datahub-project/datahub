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
package dataquality.actors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Scheduler;
import akka.routing.SmallestMailboxRouter;
import scala.concurrent.ExecutionContext;


/**
 * Created by zechen on 12/17/15.
 */
public class DqActorRegistry {
  private static ActorSystem actorSystem = ActorSystem.create("DqService");

  public static ExecutionContext dispatcher = actorSystem.dispatcher();

  public static Scheduler scheduler = actorSystem.scheduler();

  public static ActorRef
      schedulerActor = actorSystem.actorOf(Props.create(DqSchedulerActor.class), "DqSchedulerActor");

  public static ActorRef aggActor =  actorSystem.actorOf(Props.create(AggActor.class).withRouter(
      new SmallestMailboxRouter(5)), "AggActor");

  public static ActorRef metricActor = actorSystem.actorOf(
      Props.create(MetricActor.class).withRouter(new SmallestMailboxRouter(10)), "MetricActor");
}
