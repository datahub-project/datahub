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

package wherehows.common.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Generic JSON error message.
 */
public class ErrorMessage {

  private int errorCode;
  private String message;

  public ErrorMessage(@JsonProperty("error_code") int errorCode,
                      @JsonProperty("message") String message) {
    this.errorCode = errorCode;
    this.message = message;
  }

  @JsonProperty("error_code")
  public int getErrorCode() {
    return errorCode;
  }

  @JsonProperty("error_code")
  public void setErrorCode(int error_code) {
    this.errorCode = error_code;
  }

  @JsonProperty
  public String getMessage() {
    return message;
  }

  @JsonProperty
  public void setMessage(String message) {
    this.message = message;
  }
}
