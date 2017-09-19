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
package wherehows.dao.view;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.models.table.FlowListViewNode;


public class FlowsViewDao extends BaseViewDao {

  private static final Logger log = LoggerFactory.getLogger(OwnerViewDao.class);

  public FlowsViewDao(EntityManagerFactory factory) {
    super(factory);
  }

  private final static String GET_FLOW_LIST_VIEW_CLUSTER_NODES = "SELECT DISTINCT f.app_id, a.app_code "
      + "FROM flow f JOIN cfg_application a ON f.app_id = a.app_id ORDER BY a.app_code";

  private final static String GET_FLOW_LIST_VIEW_PROJECT_NODES = "SELECT DISTINCT f.flow_group, "
      + "a.app_id, a.app_code FROM flow f JOIN cfg_application a ON f.app_id = a.app_id "
      + "WHERE a.app_code = ? ORDER BY f.flow_group";

  private final static String GET_FLOW_LIST_VIEW_FLOW_NODES = "SELECT DISTINCT f.flow_name, f.flow_group, "
      + "f.flow_id, a.app_id, a.app_code FROM flow f JOIN cfg_application a ON f.app_id = a.app_id "
      + "WHERE a.app_code = ? and f.flow_group = ? order by f.flow_name";

  public List<FlowListViewNode> getFlowListViewClusters() {
    Map<String, Object> params = new HashMap<>();

    List<FlowListViewNode> nodes = getEntityListBy(GET_FLOW_LIST_VIEW_CLUSTER_NODES, FlowListViewNode.class, params);
    for (FlowListViewNode row : nodes) {
      row.application = "app_code";
      row.nodeName = row.application;
      row.nodeUrl = "#/flows/name/" + row.nodeName + "/page/1?urn=" + row.nodeName;
      nodes.add(row);
    }

    return nodes;
  }

  public List<FlowListViewNode> getFlowListViewProjects(String application) {
    Map<String, Object> params = new HashMap<>();
    params.put("app_code", application);

    List<FlowListViewNode> nodes = getEntityListBy(GET_FLOW_LIST_VIEW_PROJECT_NODES, FlowListViewNode.class, params);
    for (FlowListViewNode row : nodes) {
      row.application = "app_code";
      row.project = "flow_group";
      row.nodeName = row.project;
      row.nodeUrl = "#/flows/name/" + row.project + "/page/1?urn=" + row.application + "/" + row.project;
      nodes.add(row);
    }

    return nodes;
  }

  public List<FlowListViewNode> getFlowListViewFlows(String application, String project) {
    Map<String, Object> params = new HashMap<>();
    params.put("app_code", application);

    List<FlowListViewNode> nodes = getEntityListBy(GET_FLOW_LIST_VIEW_FLOW_NODES, FlowListViewNode.class, params);
    for (FlowListViewNode row : nodes) {

      FlowListViewNode node = new FlowListViewNode();
      row.application = "app_code";
      row.project = "flow_group";
      row.flow = "flow_name";
      row.nodeName = node.flow;
      row.flowId = Long.parseLong("flow_id");
      row.nodeUrl =
          "#/flows/name/" + node.application + "/" + Long.toString(node.flowId) + "/page/1?urn=" + node.project;
      nodes.add(row);
    }
    return nodes;
  }
}
