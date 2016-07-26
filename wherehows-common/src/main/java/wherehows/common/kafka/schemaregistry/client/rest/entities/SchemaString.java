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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class SchemaString {
  private String schemaString;

  public SchemaString() {

  }

  public SchemaString(String schemaString) {
    this.schemaString = schemaString;
  }

  public static SchemaString fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, SchemaString.class);
  }

  @JsonProperty("schema")
  public String getSchemaString() {
    return schemaString;
  }

  @JsonProperty("schema")
  public void setSchemaString(String schemaString) {
    this.schemaString = schemaString;
  }

  public String toJson() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
