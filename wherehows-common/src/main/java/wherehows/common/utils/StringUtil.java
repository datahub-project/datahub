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
package wherehows.common.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import wherehows.common.schemas.Record;


/**
 * Created by zechen on 10/21/15.
 */
public class StringUtil {

  public static String toDbString(Object object) {
    if (object != null) {
      return "'" + object.toString().replace("\\", "\\\\").replace("\'", "\\\'").replace("\"", "\\\"") + "'";
    } else {
      return "null";
    }
  }

  public static String toCsvString(Object object) {
    if (object != null) {
      return "\"" + object.toString().replace("\\", "\\\\").replace("\'", "\\\'").replace("\"", "\\\"") + "\"";
    } else {
      return "\\N";
    }
  }

  public static String replace(String s, String target, Object replacement) {
    if (replacement != null) {
      return s.replace(target, "'" + replacement.toString().replace("\'", "\\\'").replace("\"", "\\\"") + "'");
    } else {
      return s.replace(target, "null");
    }
  }

  public static Object objectToString(Object obj) {
    if (obj instanceof Collection || obj instanceof Map || obj instanceof Record) {
      return obj.toString();
    } else if (obj instanceof Object[]) {
      return Arrays.toString((Object[]) obj);
    }
    return obj;
  }

  public static String toStr(Object obj) {
    return obj != null ? obj.toString() : null;
  }
}
