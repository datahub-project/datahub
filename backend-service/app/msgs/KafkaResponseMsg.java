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
package msgs;

import wherehows.common.schemas.AbstractRecord;


/**
 * Message wrapper for communication between KafkaConsumerMaster and KafkaConsumerWorker
 */
public class KafkaResponseMsg {
  private AbstractRecord record;
  private String topic;

  public KafkaResponseMsg(AbstractRecord record, String topic) {
    this.record = record;
    this.topic = topic;
  }

  public AbstractRecord getRecord() {
    return record;
  }

  public void setRecord(AbstractRecord record) {
    this.record = record;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  @Override
  public String toString() {
    return "KafkaResponseMsg [record=" + record + ", topic=" + topic + "]";
  }

}
