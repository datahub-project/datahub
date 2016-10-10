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
public class KafkaResponseMsg {

  private String msgType;
  private String topic;
  private Map<String, Object> content;

  public KafkaResponseMsg(String msgType, String topic) {
    this.msgType = msgType;
    this.topic = topic;
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

  public Map<String, Object> getContent() {
    return content;
  }

  public void setContent(Map<String, Object> content) {
    this.content = content;
  }

  @Override
  public String toString() {
    return "KafkaResponseMsg [type=" + msgType + ", topic=" + topic + ", " + content.toString() + "]";
  }
}
