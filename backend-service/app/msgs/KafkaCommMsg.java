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

import java.util.HashMap;
import java.util.Map;


/**
 * Generic communication message between KafkaConsumerMaster and KafkaConsumerWorker
 */
public class KafkaCommMsg {

  // Message type: AUDIT, AUDIT_RESPONSE, FLUSH, FLUSH_RESPONSE, HEARTBEAT, etc
  private String msgType;
  // Kafka topic of the worker
  private String topic;
  // Kafka worker thread number
  private int thread;
  // Message content
  private Map<String, Object> content;

  public KafkaCommMsg(String msgType, String topic, int thread) {
    this.msgType = msgType;
    this.topic = topic;
    this.thread = thread;
    this.content = new HashMap<>();
  }

  public String getMsgType() {
    return msgType;
  }

  public void setMsgType(String msgType) {
    this.msgType = msgType;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getThread() {
    return thread;
  }

  public void setThread(int thread) {
    this.thread = thread;
  }

  public Map<String, Object> getContent() {
    return content;
  }

  public void setContent(Map<String, Object> content) {
    this.content = content;
  }

  public void putContent(String key, Object value) {
    this.content.put(key, value);
  }

  @Override
  public String toString() {
    return "KafkaCommMsg [type=" + msgType + ", topic=" + topic + ", thread=" + thread + ", content="
        + content.toString() + "]";
  }
}
