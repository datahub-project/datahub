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
package models;

public class Metric {

    public Integer id;
    public String name;
    public String description;
    public String dashboardName;
    public String group;
    public String category;
    public String subCategory;
    public String level;
    public String sourceType;
    public String source;
    public Long sourceDatasetId;
    public String refIDType;
    public String refID;
    public String type;
    public String grain;
    public String displayFactor;
    public String displayFactorSym;
    public String goodDirection;
    public String formula;
    public String dimensions;
    public String owners;
    public String tags;
    public String urn;
    public String metricUrl;
    public String wikiUrl;
    public String scmUrl;
    public String schema;
    public Long watchId;
    public String sourceDatasetLink;
}
