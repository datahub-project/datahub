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
package wherehows.common.schemas;

import java.util.ArrayList;
import java.util.List;


public class DatasetDependencyRecord extends AbstractRecord {
  public Long dataset_id;
  public String database_name;
  public String type;
  public String table_name;
  public Integer level_from_root;
  public Integer next_level_dependency_count;
  public String topology_sort_id;
  public String ref_obj_type;
  public String ref_obj_location;
  public String high_watermark;

  public DatasetDependencyRecord() {
  }

  public DatasetDependencyRecord(Long dataset_id, String database_name, String type, String table_name,
                               Integer level_from_root, Integer next_level_dependency_count, String topology_sort_id,
                               String ref_obj_type, String ref_obj_location, String high_watermark) {
    this.dataset_id = dataset_id;
    this.database_name = database_name;
    this.type = type;
    this.table_name = table_name;
    this.level_from_root = level_from_root;
    this.next_level_dependency_count = next_level_dependency_count;
    this.topology_sort_id = topology_sort_id;
    this.ref_obj_type = ref_obj_type;
    this.ref_obj_location = ref_obj_location;
    this.high_watermark = high_watermark;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(dataset_id);
    allFields.add(database_name);
    allFields.add(type);
    allFields.add(table_name);
    allFields.add(level_from_root);
    allFields.add(next_level_dependency_count);
    allFields.add(topology_sort_id);
    allFields.add(ref_obj_type);
    allFields.add(ref_obj_location);
    allFields.add(high_watermark);
    return allFields;
  }


}
