package com.datahub.authorization.ranger;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerContext;
import com.datahub.authorization.ranger.response.UserById;
import com.datahub.authorization.ranger.response.UserByName;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
  private RangerRestClientWrapper rangerRestClientWrapper;

  private DataHubRangerClient _dataHubRangerClient;

  private RangerAuthorizer rangerAuthorizer;
  private List<String> roles;
  private List<String> groups;

  RangerAccessResourceImpl rangerAccessResource;
  private Map<String, Object> authorizerConfigMap;

  @BeforeMethod
  public void setupTest() throws Exception {
    authorizerConfigMap = new HashMap<>();
    authorizerConfigMap.put(AuthorizerConfig.CONFIG_USERNAME, "foo");
    authorizerConfigMap.put(AuthorizerConfig.CONFIG_PASSWORD, "bar");

    // Mock Apache Ranger library classes
    rangerBasePlugin = mock(RangerBasePlugin.class);
    rangerRestClientWrapper = mock(RangerRestClientWrapper.class);

    // Spy our class method to inject Mock objects of Apache Ranger library classes
    rangerAuthorizer = spy(RangerAuthorizer.class);

    AuthorizerConfig authorizerConfig = AuthorizerConfig.builder()
        .username((String) authorizerConfigMap.get(AuthorizerConfig.CONFIG_USERNAME))
        .password((String) authorizerConfigMap.get(AuthorizerConfig.CONFIG_PASSWORD))
        .build();

    _dataHubRangerClient = spy(new DataHubRangerClient(authorizerConfig));

    rangerAccessResource = new RangerAccessResourceImpl();
    rangerAccessResource.setValue("platform", "platform");

    // Mock
    doNothing().when(rangerBasePlugin).setResultProcessor(null);
    doNothing().when(rangerBasePlugin).init();
    doReturn(rangerBasePlugin).when(_dataHubRangerClient).newRangerBasePlugin();
    doReturn(rangerRestClientWrapper).when(_dataHubRangerClient).newRangerRestClientWrapper();
    doReturn(_dataHubRangerClient).when(rangerAuthorizer).newDataHubRangerClient();

    roles = new ArrayList<>();
    roles.add("admin");
    when(rangerRestClientWrapper.getUserRole("datahub")).thenReturn(roles);

    Map<String, Object> userByIdResponse = new HashMap<>();
    groups = new ArrayList<>();
    groups.add("public");
    userByIdResponse.put(UserById.GROUP_NAME_LIST, groups);
    when(rangerRestClientWrapper.getUserById(1)).thenReturn(new UserById(userByIdResponse));

    when(_dataHubRangerClient.newRangerBasePlugin()).thenReturn(rangerBasePlugin);

    rangerAuthorizer.init(authorizerConfigMap, new AuthorizerContext(null));
  }

  @Test
  public void testAuthorizationAllow() throws Exception {

    RangerAccessRequest rangerAccessRequest =
        new RangerAccessRequestImpl(rangerAccessResource, PoliciesConfig.VIEW_ANALYTICS_PRIVILEGE.getType(), "datahub",
            this.groups.stream().collect(Collectors.toSet()), this.roles.stream().collect(Collectors.toSet()));

    RangerAccessResult rangerAccessResult = new RangerAccessResult(1, "datahub", null, null);
    // For rangerAccessRequest the access should be allowed
    rangerAccessResult.setIsAllowed(true);

    when(rangerBasePlugin.isAccessAllowed(argThat(new RangerAccessRequestMatcher(rangerAccessRequest)))).thenReturn(
        rangerAccessResult);
    // mock Apache Ranger API response as per username "github"
    Map<String, Object> userByNameResponse = new HashMap<>();
    userByNameResponse.put(UserByName.ID, 1);
    when(rangerRestClientWrapper.getUserByName("datahub")).thenReturn(new UserByName(userByNameResponse));

    assert this.callAuthorizer("urn:li:corpuser:datahub").getType() == AuthorizationResult.Type.ALLOW;
  }

  @Test
  public void testAuthorizationDeny() throws Exception {

    RangerAccessRequest rangerAccessRequest =
        new RangerAccessRequestImpl(rangerAccessResource, PoliciesConfig.VIEW_ANALYTICS_PRIVILEGE.getType(), "X",
            this.groups.stream().collect(Collectors.toSet()), this.roles.stream().collect(Collectors.toSet()));

    RangerAccessResult rangerAccessResult = new RangerAccessResult(1, "datahub", null, null);
    // For rangerAccessRequest the access should be denied
    rangerAccessResult.setIsAllowed(false);

    // mock Apache Ranger API response as per username "X"
    Map<String, Object> userByNameResponse = new HashMap<>();
    userByNameResponse.put(UserByName.ID, 1);
    when(rangerRestClientWrapper.getUserByName("X")).thenReturn(new UserByName(userByNameResponse));

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