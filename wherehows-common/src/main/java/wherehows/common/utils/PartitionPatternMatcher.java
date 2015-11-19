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

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import wherehows.common.schemas.PartitionLayout;


/**
 * Created by zechen on 10/16/15.
 */
public class PartitionPatternMatcher {
  List<PartitionLayout> layoutList;

  public PartitionPatternMatcher(List<PartitionLayout> layoutList) {
    Collections.sort(layoutList, (PartitionLayout o1, PartitionLayout o2) -> o1.getSortId().compareTo(o2.getSortId()));
    this.layoutList = layoutList;
  }

  /**
   * Analyze the partition full path and match with sorted layout list to get the first matching layout id
   * return null if no matches found
   * @param partitionFullPath
   * @return layout id
   */
  public Integer analyze(String partitionFullPath) {
    for (PartitionLayout pl : this.layoutList) {
      Pattern p = Pattern.compile(pl.getRegex());
      Matcher m = p.matcher(partitionFullPath);
      if (m.matches()) {
        return pl.getLayoutId();
      }
    }

    return null;
  }



}
