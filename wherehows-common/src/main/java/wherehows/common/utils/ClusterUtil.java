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
package wherehows.common.utils;

import java.util.ArrayList;
import java.util.List;
import wherehows.common.schemas.ClusterInfo;


public class ClusterUtil {

  private static final List<ClusterInfo> CLUSTER_LIST = new ArrayList<>();

  public static void updateClusterInfo(List<ClusterInfo> clusterList) {
    if (clusterList != null && clusterList.size() > 0) {
      CLUSTER_LIST.clear();
      CLUSTER_LIST.addAll(clusterList);
    }
  }

  /**
   * match the clusterName to the list of cluster info, return the matching clusterCode
   * @param clusterName String
   * @return clusterCode
   */
  public static String matchClusterCode(String clusterName) {
    if (clusterName == null) {
      return null;
    }

    // first use cluster code to find match
    for (ClusterInfo cluster : CLUSTER_LIST) {
      if (clusterName.contains(cluster.getClusterCode())) {
        return cluster.getClusterCode();
      }
    }

    // second round use cluster short name to find match
    for (ClusterInfo cluster : CLUSTER_LIST) {
      if (clusterName.contains(cluster.getClusterShortName())) {
        return cluster.getClusterCode();
      }
    }

    // no match found, return original string
    return clusterName;
  }

  public static List<ClusterInfo> getClusterList() {
    return CLUSTER_LIST;
  }
}
