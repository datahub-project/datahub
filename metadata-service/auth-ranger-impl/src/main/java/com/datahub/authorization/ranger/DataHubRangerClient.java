package com.datahub.authorization.ranger;

import com.datahub.authorization.ranger.response.UserByName;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.StringUtils;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;


@Slf4j
public class DataHubRangerClient {
  private static final String SERVICE_NAME = "datahub";
  private static final String PROP_RANGER_URL = StringUtils.format("ranger.plugin.%s.policy.rest.url", SERVICE_NAME);
  private static final String PROP_RANGER_SSL =
      StringUtils.format("ranger.plugin.%s.policy.rest.ssl.config.file", SERVICE_NAME);
  // Apache Ranger base url

  private static RangerBasePlugin rangerBasePlugin;
  private final AuthorizerConfig authorizerConfig;
  private RangerRestClientWrapper rangerRestClientWrapper;

  public DataHubRangerClient(AuthorizerConfig authorizerConfig) {
    this.authorizerConfig = authorizerConfig;
  }

  public void init() {

    // No need of synchronization as single Authorizer object is getting created on server startup
    if (rangerBasePlugin == null) {
      // rangerBasePlugin is static
      // Make sure classpath should have ranger-datahub-security.xml file
      rangerBasePlugin = this.newRangerBasePlugin();
      rangerBasePlugin.setResultProcessor(new RangerDefaultAuditHandler());

      rangerBasePlugin.init();
      log.info("Ranger policy engine is initialized");
    }

    rangerRestClientWrapper = this.newRangerRestClientWrapper();
  }

  public RangerBasePlugin newRangerBasePlugin() {
    return new RangerBasePlugin(SERVICE_NAME, SERVICE_NAME);
  }

  public RangerRestClientWrapper newRangerRestClientWrapper() {
    String rangerURL = rangerBasePlugin.getConfig().get(PROP_RANGER_URL);
    String rangerSslConfig = rangerBasePlugin.getConfig().get(PROP_RANGER_SSL, null);
    RangerRestClientWrapper rangerRestClientWrapper =
        new RangerRestClientWrapper(rangerURL, rangerSslConfig, this.authorizerConfig.getUsername(),
            this.authorizerConfig.getPassword(), rangerBasePlugin.getConfig());
    return rangerRestClientWrapper;
  }

  public Set<String> getUserGroups(String userIdentifier) {
    List<String> groups = new ArrayList<String>();

    try {

      UserByName userByName = this.rangerRestClientWrapper.getUserByName(userIdentifier);
      // userByName.id is (integer) apache ranger user identifier
      groups = this.rangerRestClientWrapper.getUserById(userByName.getId()).getGroupNameList();

      log.debug(StringUtils.format("User %s groups %s", userIdentifier, groups.toString()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return groups.stream().collect(Collectors.toSet());
  }

  public Set<String> getUserRoles(String userIdentifier) {
    List<String> roles = new ArrayList<String>();
    try {
      roles = this.rangerRestClientWrapper.getUserRole(userIdentifier);
      log.debug(StringUtils.format("User %s roles %s", userIdentifier, roles.toString()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return roles.stream().collect(Collectors.toSet());
  }

  public RangerAccessResult isAccessAllowed(RangerAccessRequest rangerAccessRequest) {
    return rangerBasePlugin.isAccessAllowed(rangerAccessRequest);
  }
}
