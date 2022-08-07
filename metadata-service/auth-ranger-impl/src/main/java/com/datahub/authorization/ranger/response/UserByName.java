package com.datahub.authorization.ranger.response;

import java.util.Map;
import org.apache.hadoop.util.StringUtils;


public class UserByName {
  public static final String ID = "id";
  private final Integer id;

  public UserByName(Map<String, Object> userPropertyMap) throws Exception {
    if (!userPropertyMap.containsKey(ID)) {
      throw new Exception(StringUtils.format("Property \"%s\" is not found", ID));
    }

    this.id = (Integer) userPropertyMap.get(ID);
  }

  public Integer getId() {
    return this.id;
  }
}
