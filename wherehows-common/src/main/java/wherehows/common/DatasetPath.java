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
package wherehows.common;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by zsun on 9/11/15.
 */
public class DatasetPath {
  public String abstractPath;
  public String fullPath;
  public String partitionStart;
  public String partitionEnd;
  public String partitionType;
  public int layoutId;



  /**
   * Use pattern match to separate the comma string.
   * because we need to exclude edge case : /data/path/{2015/10/20,2015/10/21,2015/10/22}
   * @param originalString the full dataset string (from log)
   * @return list of separated dataset string
   */
  public static List<String> separatedDataset(String originalString) {
    Pattern commaPattern = Pattern.compile(",(?!([^\\{]*\\}.*)).*");
    ArrayList<String> datasets = new ArrayList<>();
    Matcher datasetMatcher = commaPattern.matcher(originalString);
    int datasetStartIndex = 0;
    while (datasetMatcher.find(datasetStartIndex)) {
      String oneDataset = originalString.substring(datasetStartIndex, datasetMatcher.start());
      datasets.add(oneDataset.trim());
      datasetStartIndex = datasetMatcher.start() + 1;
    }
    datasets.add(originalString.substring(datasetStartIndex, originalString.length()).trim()); // add the last one
    return datasets;
  }
}
