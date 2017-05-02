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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import utils.JdbcUtil;


/**
 * Created by zechen on 10/15/15.
 */
public class FlowDao {

  public static final String FIND_FLOW_OWNERS =
    " SELECT DISTINCT ca.short_connection_string as instance, fop.owner_id, fop.permissions, fop.owner_type FROM flow_owner_permission fop "
      + " JOIN flow f ON fop.flow_id = f.flow_id and fop.app_id = f.app_id "
      + " JOIN cfg_application ca ON fop.app_id = ca.app_id "
      + " WHERE f.flow_path = :flow_path "
      + " AND ca.short_connection_string LIKE :instance ";

  public static final String FIND_FLOW_SCHEDULES =
    " SELECT DISTINCT ca.short_connection_string as instance, fs.frequency, fs.unit, fs.effective_start_time, fs.effective_end_time FROM flow_schedule fs "
      + " JOIN flow f ON fs.flow_id = f.flow_id and fs.app_id = f.app_id "
      + " JOIN cfg_application ca ON fs.app_id = ca.app_id "
      + " WHERE f.flow_path = :flow_path "
      + " AND ca.short_connection_string LIKE :instance "
      + " AND fs.is_active = 'Y'";

  public static List<Map<String, Object>> getFlowOwner(String flowPath, String instance) {
    Map<String, Object> params = new HashMap<>();
    params.put("flow_path", flowPath);
    if (instance == null || instance.isEmpty()) {
      params.put("instance", "%");
    } else {
      params.put("instance", instance);
    }
    List<Map<String, Object>> owners = JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_FLOW_OWNERS, params);

    return owners;
  }

  public static List<Map<String, Object>> getFlowSchedules(String flowPath, String instance) {
    Map<String, Object> params = new HashMap<>();
    params.put("flow_path", flowPath);
    if (instance == null || instance.isEmpty()) {
      params.put("instance", "%");
    } else {
      params.put("instance", instance);
    }
    List<Map<String, Object>> schedules = JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_FLOW_SCHEDULES, params);
    return schedules;
  }

}
