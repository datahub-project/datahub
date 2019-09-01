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
package com.linkedin.datahub.models.table;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Date;
import java.util.List;


public class Dataset {

  public long id;
  public String name;
  public String source;
  public String urn;
  public Date created;
  public Date modified;
  public String formatedModified;
  public String schema;
  public String hdfsBrowserLink;
  public boolean isFavorite;
  public boolean isOwned;
  public long watchId;
  public boolean isWatched;
  public boolean hasSchemaHistory;
  public JsonNode properties;
  public List<User> owners;
  public List<String> umpFrequency;
}
