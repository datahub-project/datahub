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

public class DatasetColumn {
    public Long id;
    public int sortID;
    public int parentSortID;
    public String fieldName;
    public String dataType;
    public String comment;
    public boolean partitioned;
    public boolean nullable;
    public boolean distributed;
    public boolean indexed;
    public Long commentCount;
    public String treeGridClass;
}
