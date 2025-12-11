package com.datahub.authorization;

import java.util.Map;
import lombok.Value;

@Value
public class BatchAuthorizationResult {
  /** The original authorization request */
  BatchAuthorizationRequest request;

  /** Results per individual privilege */
  Map<String, AuthorizationResult> results;
}
