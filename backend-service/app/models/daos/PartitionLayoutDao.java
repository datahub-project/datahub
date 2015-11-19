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
package models.daos;

import java.util.List;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import utils.JdbcUtil;
import wherehows.common.schemas.PartitionLayout;


/**
 * Created by zechen on 10/16/15.
 */
public class PartitionLayoutDao {
  public static final String GET_LAYOUTS =
    " SELECT layout_id, regex, mask, leading_path_index, partition_index, second_partition_index, sort_id, partition_pattern_group "
      + " FROM dataset_partition_layout_pattern ";

  public static List<PartitionLayout> getPartitionLayouts() {
    return JdbcUtil.wherehowsJdbcTemplate.query(GET_LAYOUTS, new BeanPropertyRowMapper<>(PartitionLayout.class));
  }

}
