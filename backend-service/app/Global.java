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
import play.Application;
import play.GlobalSettings;
import play.Logger;
import utils.SchedulerUtil;


/**
 * Created by zechen on 9/3/15.
 */
public class Global extends GlobalSettings {

  @Override
  public void onStart(Application arg0) {
    Logger.info("on start---===");
    SchedulerUtil.start();
  }
}
