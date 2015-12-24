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
package dataquality.models;

import java.sql.Timestamp;


/**
 * Created by zechen on 8/7/15.
 */
public class TimeRange {

  Timestamp start;
  Timestamp end;

  boolean exclusiveStart;
  boolean exclusiveEnd;

  public TimeRange() {
  }

  public TimeRange(Timestamp start, Timestamp end) {
    this.start = start;
    this.end = end;
  }

  public TimeRange(Timestamp start, Timestamp end, boolean exclusiveStart, boolean exclusiveEnd) {
    this.start = start;
    this.end = end;
    this.exclusiveStart = exclusiveStart;
    this.exclusiveEnd = exclusiveEnd;
  }

  public boolean contains(Timestamp ts) {
    return (ts.after(start) && ts.before(end)) || (ts.equals(start) && !exclusiveStart) || (ts.equals(end) && !exclusiveEnd);
  }
  public Timestamp getStart() {
    return start;
  }

  public void setStart(Timestamp start) {
    this.start = start;
  }

  public Timestamp getEnd() {
    return end;
  }

  public void setEnd(Timestamp end) {
    this.end = end;
  }

  public boolean isExclusiveStart() {
    return exclusiveStart;
  }

  public void setExclusiveStart(boolean exclusiveStart) {
    this.exclusiveStart = exclusiveStart;
  }

  public boolean isExclusiveEnd() {
    return exclusiveEnd;
  }

  public void setExclusiveEnd(boolean exclusiveEnd) {
    this.exclusiveEnd = exclusiveEnd;
  }
}
