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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import utils.JdbcUtil;
import wherehows.common.schemas.ClusterInfo;


public class ClusterDao {

  private static final String GET_CLUSTER_INFO = "SELECT * FROM cfg_cluster";

  public static List<ClusterInfo> getClusterInfo() throws Exception {

    List<Map<String, Object>> rows = JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_CLUSTER_INFO);

    List<ClusterInfo> clusters = new ArrayList<>();
    for (Map<String, Object> row : rows) {
      String clusterCode = row.get("cluster_code").toString();
      // skip the default placeholder row
      if (clusterCode.equals("[all]")) {
        continue;
      }

      int clusterId = Integer.parseInt(row.get("cluster_id").toString());
      String clusterShortName = row.get("cluster_short_name").toString();
      String datacenterCode = row.get("data_center_code").toString();
      String clusterType = row.get("cluster_type").toString();
      String deploymentTierCode = row.get("deployment_tier_code").toString();

      clusters.add(
          new ClusterInfo(clusterId, clusterCode, clusterShortName, datacenterCode, clusterType, deploymentTierCode));
    }

    return clusters;
  }
}
