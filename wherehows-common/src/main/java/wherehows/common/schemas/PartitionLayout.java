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

/**
 * Created by zechen on 10/19/15.
 */
public class PartitionLayout {
  Integer layoutId;
  String regex;
  String mask;
  Integer leadingPathIndex;
  Integer partitionIndex;
  Integer secondPartitionIndex;
  Integer sortId;
  String partitionPatternGroup;

  public PartitionLayout() {
  }

  public PartitionLayout(Integer layoutId, String regex, String mask, Integer leadingPathIndex,
    Integer partitionIndex, Integer secondPartitionIndex, Integer sortId, String partitionPatternGroup) {
    this.layoutId = layoutId;
    this.regex = regex;
    this.mask = mask;
    this.leadingPathIndex = leadingPathIndex;
    this.partitionIndex = partitionIndex;
    this.secondPartitionIndex = secondPartitionIndex;
    this.sortId = sortId;
    this.partitionPatternGroup = partitionPatternGroup;
  }

  public Integer getLayoutId() {
    return layoutId;
  }

  public void setLayoutId(Integer layoutId) {
    this.layoutId = layoutId;
  }

  public String getRegex() {
    return regex;
  }

  public void setRegex(String regex) {
    this.regex = regex;
  }

  public String getMask() {
    return mask;
  }

  public void setMask(String mask) {
    this.mask = mask;
  }

  public Integer getLeadingPathIndex() {
    return leadingPathIndex;
  }

  public void setLeadingPathIndex(Integer leadingPathIndex) {
    this.leadingPathIndex = leadingPathIndex;
  }

  public Integer getPartitionIndex() {
    return partitionIndex;
  }

  public void setPartitionIndex(Integer partitionIndex) {
    this.partitionIndex = partitionIndex;
  }

  public Integer getSecondPartitionIndex() {
    return secondPartitionIndex;
  }

  public void setSecondPartitionIndex(Integer secondPartitionIndex) {
    this.secondPartitionIndex = secondPartitionIndex;
  }

  public Integer getSortId() {
    return sortId;
  }

  public void setSortId(Integer sortId) {
    this.sortId = sortId;
  }

  public String getPartitionPatternGroup() {
    return partitionPatternGroup;
  }

  public void setPartitionPatternGroup(String partitionPatternGroup) {
    this.partitionPatternGroup = partitionPatternGroup;
  }


}
