package com.linkedin.metadata.filter.auth;

import java.util.Map;


public interface AuthenticationContext {
  Map<String, String> requestHeaders();
}
