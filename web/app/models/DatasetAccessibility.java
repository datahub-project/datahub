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

import java.util.List;

public class DatasetAccessibility {

    public Long datasetId;
    public Integer dbId;
    public String dbName;
    public String datasetType;
    public String partitionGain;
    public String partitionExpr;
    public String dataTimeExpr;
    public Integer dataTimeEpoch;
    public Long recordCount;
    public Long sizeInByte;
    public Integer logTimeEpoch;
    public String logTimeEpochStr;
    public List<DatasetAccessItem> itemList;
}