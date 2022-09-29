package com.datahub.authorization.ranger;

import com.datahub.authorization.ranger.response.UserById;
import com.datahub.authorization.ranger.response.UserByName;
import com.sun.jersey.api.client.ClientResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.util.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.util.RangerRESTClient;


/**
 * Class to wrap Apache Ranger Rest Client
 */

public class RangerRestClientWrapper {
  private static final String URI_BASE = "/service";
  private static final String USER_ROLES = URI_BASE + "/roles/roles/user/%s";
  private static final String USER_BY_NAME = URI_BASE + "/xusers/users/userName/%s";
  private static final String USER_BY_ID = URI_BASE + "/xusers/users/%d";

  private final RangerRESTClient rangerRESTClient;

  public RangerRestClientWrapper(String rangerUrl, String rangerSslConfig, String userName, String password,
      RangerPluginConfig pluginConfig) {
    this.rangerRESTClient = new RangerRESTClient(rangerUrl, rangerSslConfig, pluginConfig);
    this.rangerRESTClient.setBasicAuthInfo(userName, password);
  }

  public UserByName getUserByName(String userName) throws Exception {
    ClientResponse clientResponse = this.rangerRESTClient.get(StringUtils.format(USER_BY_NAME, userName), null);
    Map<String, Object> userByNameMap = clientResponse.getEntity(new HashMap<String, Object>().getClass());
    UserByName userByNameResponse = new UserByName(userByNameMap);
    return userByNameResponse;
  }

  public UserById getUserById(Integer id) throws Exception {
    ClientResponse clientResponse = this.rangerRESTClient.get(StringUtils.format(USER_BY_ID, id), null);
    Map<String, Object> userByIdMap = clientResponse.getEntity(new HashMap<String, Object>().getClass());
    UserById userByIdResponse = new UserById(userByIdMap);
    return userByIdResponse;
  }

  public List<String> getUserRole(String username) throws Exception {
    ClientResponse clientResponse =
        this.rangerRESTClient.get(StringUtils.format(USER_ROLES, username), null);
    return clientResponse.getEntity((new ArrayList<String>()).getClass());
  }
}