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
    public String refID;
    public String name;
    public String urn;
    public String refIDType;
    public String description;
    public String dashboardName;
    public String category;
    public String subCategory;
    public String group;
    public String sourceType;
    public String source;
    public String schema;
    public String grain;
    public String formula;
    public String displayFactor;
    public String displayFactorSym;
    public Long watchId;
}
