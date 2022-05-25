package com.datahub.authorization.ranger;

import lombok.extern.slf4j.Slf4j;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

import java.util.HashSet;
import java.util.Set;


@Slf4j
public class FakeDataHubRangerClient implements DataHubRangerClient {
  private AuthorizerConfig authorizerConfig;

  /**
   * Initiate Apache Ranger policy-engine thread
   */
  public static void initRangerBasePlugin() {
  }

  public FakeDataHubRangerClient(AuthorizerConfig authorizerConfig) {
    this.authorizerConfig = authorizerConfig;
  }

  @Override
  public void init() {
    log.info("Fake Ranger PolicyEngine is initialized");
  }

  @Override
  public Set<String> getUserGroups(String userIdentifier) {
    Set<String> groups = new HashSet<String>();
    groups.add("admin");
    return groups;
  }

  @Override
  public Set<String> getUserRoles(String userIdentifier) {
    Set<String> roles = new HashSet<String>();
    roles.add("admin");
    return roles;
  }

  @Override
  public RangerAccessResult isAccessAllowed(RangerAccessRequest rangerAccessRequest) {
    RangerAccessResult rangerAccessResult = new RangerAccessResult(1, "datahub", null, null);
    rangerAccessResult.setIsAllowed(false);
    return rangerAccessResult;
  }
}