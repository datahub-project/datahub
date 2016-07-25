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

package wherehows.common.kafka.schemaregistry.client.rest.exceptions;

public class RestClientException extends Exception {
  private final int status;
  private final int errorCode;

  public RestClientException(final String message, final int status, final int errorCode) {
    super(message + "; error code: " + errorCode);
    this.status = status;
    this.errorCode = errorCode;
  }

  public int getStatus() {
    return status;
  }

  public int getErrorCode() {
    return errorCode;
  }
}