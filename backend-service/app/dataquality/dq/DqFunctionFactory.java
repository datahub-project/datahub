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
package dataquality.dq;

import java.util.Map;


/**
 * Created by zechen on 8/6/15.
 */
public class DqFunctionFactory {

  public static DqFunction newFunction(DqFunctionType type, Map<String, Object> params) {
    DqFunction f = null;
    switch (type) {
      case SELF:
        f = new DqBaseFunction(params);
        break;
      case DIFF:
        f = new DqDiff(params);
        break;
      case DIFF_PCT:
        f = new DqDiffPct(params);
        break;
      case DEV:
        f = new DqDev(params);
        break;
    }

    return f;
  }
}
