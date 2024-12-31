package com.datahub.authorization;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.List;
import lombok.Data;

/** POJO representing the "authentication" configuration block in application.yaml. */
@Data
public class AuthorizationConfiguration {
  /** Configuration for the default DataHub Policies-based authorizer. */
  private DefaultAuthorizerConfiguration defaultAuthorizer;

  /** List of configurations for {@link Authorizer}s to be registered */
  private List<AuthorizerConfiguration> authorizers;

  private ViewAuthorizationConfiguration view;
}
