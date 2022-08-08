package com.datahub.authorization.ranger.response;

import java.util.List;
import java.util.Map;


public class UserById {
  public static final String GROUP_NAME_LIST = "groupNameList";
  private final List<String> groupNameList;

  public UserById(Map<String, Object> userPropertyMap) throws Exception {
    if (!userPropertyMap.containsKey(GROUP_NAME_LIST)) {
      throw new Exception(String.format("Property \"%s\" is not found", GROUP_NAME_LIST));
    }

    this.groupNameList = (List<String>) userPropertyMap.get(GROUP_NAME_LIST);
  }

  public List<String> getGroupNameList() {
    return this.groupNameList;
  }
}


