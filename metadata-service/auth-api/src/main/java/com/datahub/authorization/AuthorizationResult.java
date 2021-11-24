package com.datahub.authorization;

import com.linkedin.policy.DataHubPolicyInfo;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class AuthorizationResult {
  AuthorizationRequest request;

  Optional<DataHubPolicyInfo> policy;

  Type type;

  public enum Type {
    ALLOW,
    DENY
  }
}
