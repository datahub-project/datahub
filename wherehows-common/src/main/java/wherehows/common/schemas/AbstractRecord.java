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
package wherehows.common.schemas;

import java.util.List;
import wherehows.common.utils.StringUtil;


/**
 * Created by zechen on 9/16/15.
 */
public abstract class AbstractRecord implements Record {
  char SEPR = 0x001A;

  @Override
  public String toCsvString() {
    List<Object> allFields = fillAllFields();
    StringBuilder sb = new StringBuilder();
    for (Object o : allFields) {
      sb.append(o);
      sb.append(SEPR);
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  @Override
  public String toDatabaseValue() {
    List<Object> allFields = fillAllFields();
    StringBuilder sb = new StringBuilder();
    for (Object o : allFields) {
      sb.append(StringUtil.toDbString(o));
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  public abstract List<Object> fillAllFields();
}
