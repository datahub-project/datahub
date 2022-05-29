package com.datahub.authorization.ranger;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.ranger.RangerClient;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/***
 * RangerAccessRequest class doesn't have the equal method implementation and hence need to provide mock matcher
 */
class RangerAccessRequestMatcher implements ArgumentMatcher<RangerAccessRequest> {
  private final RangerAccessRequest expected;

  public RangerAccessRequestMatcher(RangerAccessRequest expected) {
    this.expected = expected;
  }

  @Override
  public boolean matches(RangerAccessRequest argument) {
    return argument.getUserGroups().equals(expected.getUserGroups()) && argument.getUserRoles()
        .equals(expected.getUserRoles()) && argument.getUser().equals(expected.getUser());
  }
}

public class RangerAuthorizerTest {
  private RangerBasePlugin rangerBasePlugin;
  private RangerClient rangerClient;

  private DataHubRangerClientImpl dataHubRangerClientImpl;

  private RangerAuthorizer rangerAuthorizer;
  private List<String> roles;

  RangerAccessResourceImpl rangerAccessResource;
  private Map<String, Object> authorizerConfigMap;

  @BeforeMethod
  public void setupTest() throws Exception {
    // Mock Apache Ranger library classes
    rangerBasePlugin = mock(RangerBasePlugin.class);
    rangerClient = mock(RangerClient.class);

    // Spy our class method to inject Mock objects of Apache Ranger library classes
    rangerAuthorizer = spy(RangerAuthorizer.class);

    authorizerConfigMap = new HashMap<>();
    authorizerConfigMap.put("username", "foo");
    authorizerConfigMap.put("password", "bar");
    authorizerConfigMap.put("sslConfig", "SSLCONFIG");

    dataHubRangerClientImpl = spy(new DataHubRangerClientImpl(new AuthorizerConfig(authorizerConfigMap)));

    roles = new ArrayList<>();
    roles.add("admin");

    rangerAccessResource = new RangerAccessResourceImpl();
    rangerAccessResource.setValue("platform", "platform");

    // Mock
    doNothing().when(rangerBasePlugin).setResultProcessor(null);
    doNothing().when(rangerBasePlugin).init();
    doReturn(rangerBasePlugin).when(dataHubRangerClientImpl).newRangerBasePlugin();
    doReturn(rangerClient).when(dataHubRangerClientImpl).newRangerClient();
    doReturn(dataHubRangerClientImpl).when(rangerAuthorizer).newDataHubRangerClient();

    when(rangerClient.getUserRoles("datahub")).thenReturn(roles);

    when(dataHubRangerClientImpl.newRangerBasePlugin()).thenReturn(rangerBasePlugin);

    rangerAuthorizer.init(authorizerConfigMap, new AuthorizerContext(null));
  }

  @Test
  public void testAuthorizationAllow() throws Exception {

    RangerAccessRequest rangerAccessRequest =
        new RangerAccessRequestImpl(rangerAccessResource, PoliciesConfig.VIEW_ANALYTICS_PRIVILEGE.getType(),
            "datahub", null, this.roles.stream().collect(Collectors.toSet()));

    RangerAccessResult rangerAccessResult = new RangerAccessResult(1, "datahub", null, null);
    // For rangerAccessRequest the access should be allowed
    rangerAccessResult.setIsAllowed(true);

    when(rangerBasePlugin.isAccessAllowed(argThat(new RangerAccessRequestMatcher(rangerAccessRequest)))).thenReturn(
        rangerAccessResult);

    assert this.callAuthorizer("urn:li:corpuser:datahub").getType() == AuthorizationResult.Type.ALLOW;
  }

  @Test
  public void testAuthorizationDeny() throws Exception {

    RangerAccessRequest rangerAccessRequest =
        new RangerAccessRequestImpl(rangerAccessResource, PoliciesConfig.VIEW_ANALYTICS_PRIVILEGE.getType(),
            "X", null, this.roles.stream().collect(Collectors.toSet()));

    RangerAccessResult rangerAccessResult = new RangerAccessResult(1, "datahub", null, null);
    // For rangerAccessRequest the access should be denied
    rangerAccessResult.setIsAllowed(false);

    when(rangerBasePlugin.isAccessAllowed(argThat(new RangerAccessRequestMatcher(rangerAccessRequest)))).thenReturn(
        rangerAccessResult);

    assert this.callAuthorizer("urn:li:corpuser:X").getType() == AuthorizationResult.Type.DENY;
  }

  private AuthorizationResult callAuthorizer(String urn) {
    AuthorizationRequest authorizationRequest =
        new AuthorizationRequest(urn, PoliciesConfig.VIEW_ANALYTICS_PRIVILEGE.getType(), Optional.empty());
    return rangerAuthorizer.authorize(authorizationRequest);
  }
}