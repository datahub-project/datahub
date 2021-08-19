package com.linkedin.metadata.filter.auth;

import java.util.Map;


public interface Principal {

  /**
   * The name of the principal. This will be equivalent to the DataHub username.
   */
  String name();

  /**
   * Attributes about the principal, such as the groups they are in.
   */
  Map<String, Object> attributes();

}
