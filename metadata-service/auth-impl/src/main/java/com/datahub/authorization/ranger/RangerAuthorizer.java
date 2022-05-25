package com.datahub.authorization.ranger;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.Authorizer;
import com.datahub.authorization.AuthorizerContext;
import com.datahub.authorization.ResourceSpec;
import com.linkedin.common.urn.Urn;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;


@Slf4j
public class RangerAuthorizer implements Authorizer {
  private AuthorizerConfig authorizerConfig;
  //private RangerAuthenticator rangerAuthenticator;
  private DataHubRangerClient dataHubRangerClient;

  public RangerAuthorizer() {
    this(null, null);
  }

  /**
   * This constructor to test RangerAuthorizer from integration test
   * @param authorizerConfig
   * @param dataHubRangerClient
   */
  public RangerAuthorizer(AuthorizerConfig authorizerConfig, DataHubRangerClient dataHubRangerClient) {
    this.authorizerConfig = authorizerConfig;
    this.dataHubRangerClient = dataHubRangerClient;
  }

  @Override
  public void init(@Nonnull Map<String, Object> authorizerConfigMap, @Nonnull final AuthorizerContext ctx) {
    this.authorizerConfig = new AuthorizerConfig(authorizerConfigMap);
    this.dataHubRangerClient = new DataHubRangerClientImpl(this.authorizerConfig);
    this.dataHubRangerClient.init();
  }

  @Override
  public AuthorizationResult authorize(AuthorizationRequest request) {
    String[] actorUrnPart = request.getActorUrn().split(":");
    String userIdentifier = actorUrnPart[actorUrnPart.length - 1];

    Set<String> roles = this.dataHubRangerClient.getUserRoles(userIdentifier);
    Set<String> groups = this.dataHubRangerClient.getUserGroups(userIdentifier);

    RangerAccessResourceImpl rangerAccessResource = new RangerAccessResourceImpl();
    // set ResourceSpec default to "platform"
    ResourceSpec resourceSpec = request.getResourceSpec().orElse(new ResourceSpec("platform", "platform"));

    // user has requested access to specific resource
    log.debug(String.format("User \"%s\" requested access", userIdentifier));
    log.debug(String.format("Access is requested for resource type: %s", resourceSpec.getType()));
    log.debug(String.format("Access is requested for resource : %s", resourceSpec.getResource()));
    log.debug(String.format("Requested privilege : %s", request.getPrivilege()));
    // Convert resource type to lowercase as ranger doesn't support capital letter in resource type
    rangerAccessResource.setValue(resourceSpec.getType().toLowerCase(), resourceSpec.getResource());
    RangerAccessRequest rangerAccessRequest =
        new RangerAccessRequestImpl(rangerAccessResource, request.getPrivilege(), userIdentifier, groups, roles);

    // Check with Apache Ranger if access is allowed to the user
    RangerAccessResult accessResult = this.dataHubRangerClient.isAccessAllowed(rangerAccessRequest);
    AuthorizationResult.Type result = AuthorizationResult.Type.DENY;

    if (accessResult != null && accessResult.getIsAllowed()) {
      result = AuthorizationResult.Type.ALLOW;
    }

    String message = String.format("Access to resource \"%s\" for privilege \"%s\" is \"%s\" for user \"%s\"",
        resourceSpec.getResource(), request.getPrivilege(), result.toString(), userIdentifier);
    log.info(message);
    return new AuthorizationResult(request, result, message);
  }

  @Override
  public AuthorizedActors authorizedActors(String privilege, Optional<ResourceSpec> resourceSpec) {
    log.info("Apache Ranger authorizer authorizedActors");
    return new AuthorizedActors(privilege, new ArrayList<Urn>(), new ArrayList<Urn>(), true, true);
  }
}
