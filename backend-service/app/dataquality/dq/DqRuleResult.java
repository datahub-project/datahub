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

import com.fasterxml.jackson.annotation.JsonInclude;
import dataquality.models.MetricValue;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;



/**
 * Created by zechen on 8/7/15.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqRuleResult {
  String description;
  DqStatus status;
  Map<MetricValue, String> failedValues = new HashMap<>();
  List<String> messages = new LinkedList();

  public DqRuleResult() {
  }

  public DqRuleResult(DqStatus status) {
    this.status = status;
  }

  public void addMessage(String message) {
    messages.add(message);
  }

  public void addFailedValue(MetricValue value, String errorMsg) {
    this.failedValues.put(value, errorMsg);
  }

  public Map<MetricValue, String> getFailedValues() {
    return failedValues;
  }

  public void setFailedValues(Map<MetricValue, String> failedValues) {
    this.failedValues = failedValues;
  }

  public DqStatus getStatus() {
    return status;
  }

  public void setStatus(DqStatus status) {
    this.status = status;
  }

  public List<String> getMessages() {
    return messages;
  }

  public void setMessages(List<String> messages) {
    this.messages = messages;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
