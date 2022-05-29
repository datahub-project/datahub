package com.datahub.authorization.ranger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.StringUtils;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;


@Slf4j
public class DataHubRangerClientImpl implements DataHubRangerClient {
  public static final String SERVICE_NAME = "datahub";
  public static final String PROP_RANGER_URL = StringUtils.format("ranger.plugin.%s.policy.rest.url", SERVICE_NAME);
  private static RangerBasePlugin rangerBasePlugin;
  private final AuthorizerConfig authorizerConfig;
  private RangerClient rangerClient;

  public DataHubRangerClientImpl(AuthorizerConfig authorizerConfig) {
    this.authorizerConfig = authorizerConfig;
  }

  @Override
  public void init() {

    // Don't need any synchronization as single Authorizer object is getting created on server startup
    if (rangerBasePlugin == null) {
      // rangerBasePlugin is static
      // Make sure classpath should have ranger-datahub-security.xml file
      rangerBasePlugin = this.newRangerBasePlugin();
      rangerBasePlugin.setResultProcessor(new RangerDefaultAuditHandler());

      rangerBasePlugin.init();
      log.info("Ranger policy engine is initialized");
    }

    rangerClient = this.newRangerClient();
  }

  public RangerBasePlugin newRangerBasePlugin() {
    return new RangerBasePlugin(SERVICE_NAME, SERVICE_NAME);
  }

  public RangerClient newRangerClient() {
    String rangerURL = rangerBasePlugin.getConfig().get(PROP_RANGER_URL);
    return new RangerClient(rangerURL, this.authorizerConfig.getAuthType(), this.authorizerConfig.getUsername(),
        this.authorizerConfig.getPassword(), this.authorizerConfig.getSslConfig().get());
  }

  @Override
  public Set<String> getUserGroups(String userIdentifier) {
    return null;
  }

  @Override
  public Set<String> getUserRoles(String userIdentifier) {
    List<String> roles = new ArrayList<String>();
    try {
      roles = this.rangerClient.getUserRoles(userIdentifier);
      log.debug(StringUtils.format("User %s roles %s", userIdentifier, roles.toString()));
    } catch (RangerServiceException e) {
      log.error("Fail to fetch user roles from ranger", e);
      log.info("Returning empty roles list");
    }

    return roles.stream().collect(Collectors.toSet());
  }

  @Override
  public RangerAccessResult isAccessAllowed(RangerAccessRequest rangerAccessRequest) {
    log.info("Evaluating the access policy for incoming request");
    return rangerBasePlugin.isAccessAllowed(rangerAccessRequest);
  }
}
