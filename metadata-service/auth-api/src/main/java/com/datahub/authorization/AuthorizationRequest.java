package com.datahub.authorization;

import java.util.Optional;
import lombok.Value;


@Value
public class AuthorizationRequest {
  String actorUrn;
  String privilege;
  Optional<ResourceSpec> resourceSpec;
}
