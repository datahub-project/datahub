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

package wherehows.common.kafka.schemaregistry.client;

public class SchemaMetadata {
  private String id;
  private int version;
  private String schema;

  public SchemaMetadata(String id, int version, String schema) {
    this.id = id;
    this.version = version;
    this.schema = schema;

  }

  public String getId() {
    return id;
  }

  public int getVersion() {
    return version;
  }

  public String getSchema() {
    return schema;
  }
}
