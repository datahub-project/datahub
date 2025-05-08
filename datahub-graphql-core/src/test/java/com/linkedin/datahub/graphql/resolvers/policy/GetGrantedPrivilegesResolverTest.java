package com.linkedin.datahub.graphql.resolvers.policy;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.PolicyEngine;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GetGrantedPrivilegesInput;
import com.linkedin.datahub.graphql.generated.PolicyEvaluationDetail;
import com.linkedin.datahub.graphql.generated.Privileges;
import com.linkedin.datahub.graphql.generated.ResourceSpec;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetGrantedPrivilegesResolverTest {

  private GetGrantedPrivilegesResolver resolver;
  private DataFetchingEnvironment environment;
  private QueryContext context;
  private AuthorizerChain authorizerChain;
  private DataHubAuthorizer dataHubAuthorizer;
  private MockedStatic<ResolverUtils> resolverUtils;
  private MockedStatic<PolicyAuthUtils> policyAuthUtils;
  private MockedStatic<GraphQLConcurrencyUtils> concurrencyUtils;
  private static final String ACTOR_URN = "urn:li:corpuser:test";
  private static final String RESOURCE_URN = "urn:li:dataset:test";

  @BeforeMethod
  public void setup() {
    resolver = new GetGrantedPrivilegesResolver();
    environment = mock(DataFetchingEnvironment.class);
    context = mock(QueryContext.class);
    authorizerChain = mock(AuthorizerChain.class);
    dataHubAuthorizer = mock(DataHubAuthorizer.class);
    resolverUtils = mockStatic(ResolverUtils.class);
    policyAuthUtils = mockStatic(PolicyAuthUtils.class);
    concurrencyUtils = mockStatic(GraphQLConcurrencyUtils.class);

    when(environment.getContext()).thenReturn(context);
    when(context.getAuthorizer()).thenReturn(authorizerChain);
    when(authorizerChain.getDefaultAuthorizer()).thenReturn(dataHubAuthorizer);
  }

  @AfterMethod
  public void tearDown() {
    resolverUtils.close();
    policyAuthUtils.close();
    concurrencyUtils.close();
  }

  @Test
  public void testGetGrantedPrivilegesWithAuthorizedUser() throws Exception {
    // Setup
    GetGrantedPrivilegesInput input = new GetGrantedPrivilegesInput();
    input.setActorUrn(ACTOR_URN);
    input.setIncludeEvaluationDetails(false);

    when(environment.getArgument("input")).thenReturn(input);
    when(context.getActorUrn()).thenReturn(ACTOR_URN);
    policyAuthUtils.when(() -> PolicyAuthUtils.canManagePolicies(context)).thenReturn(false);
    resolverUtils
        .when(() -> bindArgument(eq(input), eq(GetGrantedPrivilegesInput.class)))
        .thenReturn(input);

    List<String> privileges = List.of("READ");
    Map<String, String> denyReasons = new HashMap<>();
    PolicyEngine.PolicyGrantedPrivileges mockResult =
        mock(PolicyEngine.PolicyGrantedPrivileges.class);
    when(mockResult.getPrivileges()).thenReturn(privileges);
    when(mockResult.getReasonOfDeny()).thenReturn(denyReasons);
    when(dataHubAuthorizer.getGrantedPrivileges(any(), any())).thenReturn(mockResult);

    Privileges expectedPrivileges =
        Privileges.builder().setPrivileges(privileges).setEvaluationDetails(null).build();

    concurrencyUtils
        .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(expectedPrivileges));

    // Execute
    CompletableFuture<Privileges> result = resolver.get(environment);

    // Verify
    Privileges returnedPrivileges = result.get();
    assertEquals(returnedPrivileges.getPrivileges(), List.of("READ"));
    assertEquals(returnedPrivileges.getEvaluationDetails(), null);
  }

  @Test
  public void testGetGrantedPrivilegesWithResourceSpec() throws Exception {
    // Setup
    GetGrantedPrivilegesInput input = new GetGrantedPrivilegesInput();
    input.setActorUrn(ACTOR_URN);
    input.setIncludeEvaluationDetails(false);

    ResourceSpec resourceSpec = new ResourceSpec();
    resourceSpec.setResourceType(EntityType.DATASET);
    resourceSpec.setResourceUrn(RESOURCE_URN);
    input.setResourceSpec(resourceSpec);

    when(environment.getArgument("input")).thenReturn(input);
    when(context.getActorUrn()).thenReturn(ACTOR_URN);
    policyAuthUtils.when(() -> PolicyAuthUtils.canManagePolicies(context)).thenReturn(false);
    resolverUtils
        .when(() -> bindArgument(eq(input), eq(GetGrantedPrivilegesInput.class)))
        .thenReturn(input);

    List<String> privileges = List.of("READ");
    Map<String, String> denyReasons = new HashMap<>();
    PolicyEngine.PolicyGrantedPrivileges mockResult =
        mock(PolicyEngine.PolicyGrantedPrivileges.class);
    when(mockResult.getPrivileges()).thenReturn(privileges);
    when(mockResult.getReasonOfDeny()).thenReturn(denyReasons);
    when(dataHubAuthorizer.getGrantedPrivileges(any(), any())).thenReturn(mockResult);

    Privileges expectedPrivileges =
        Privileges.builder().setPrivileges(privileges).setEvaluationDetails(null).build();

    concurrencyUtils
        .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(expectedPrivileges));

    // Execute
    CompletableFuture<Privileges> result = resolver.get(environment);

    // Verify
    Privileges returnedPrivileges = result.get();
    assertEquals(returnedPrivileges.getPrivileges(), List.of("READ"));
  }

  @Test
  public void testGetGrantedPrivilegesWithEvaluationDetails() throws Exception {
    // Setup
    GetGrantedPrivilegesInput input = new GetGrantedPrivilegesInput();
    input.setActorUrn(ACTOR_URN);
    input.setIncludeEvaluationDetails(true);

    when(environment.getArgument("input")).thenReturn(input);
    when(context.getActorUrn()).thenReturn(ACTOR_URN);
    policyAuthUtils.when(() -> PolicyAuthUtils.canManagePolicies(context)).thenReturn(true);
    resolverUtils
        .when(() -> bindArgument(eq(input), eq(GetGrantedPrivilegesInput.class)))
        .thenReturn(input);

    List<String> privileges = List.of("READ");
    Map<String, String> denyReasons = new HashMap<>();
    denyReasons.put("policy1", "Access denied by policy1");
    PolicyEngine.PolicyGrantedPrivileges mockResult =
        mock(PolicyEngine.PolicyGrantedPrivileges.class);
    when(mockResult.getPrivileges()).thenReturn(privileges);
    when(mockResult.getReasonOfDeny()).thenReturn(denyReasons);
    when(dataHubAuthorizer.getGrantedPrivileges(any(), any())).thenReturn(mockResult);

    List<PolicyEvaluationDetail> evaluationDetails = new ArrayList<>();
    evaluationDetails.add(new PolicyEvaluationDetail("policy1", "Access denied by policy1"));

    Privileges expectedPrivileges =
        Privileges.builder()
            .setPrivileges(privileges)
            .setEvaluationDetails(evaluationDetails)
            .build();

    concurrencyUtils
        .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(expectedPrivileges));

    // Execute
    CompletableFuture<Privileges> result = resolver.get(environment);

    // Verify
    Privileges returnedPrivileges = result.get();
    assertEquals(returnedPrivileges.getPrivileges(), List.of("READ"));
    assertEquals(returnedPrivileges.getEvaluationDetails().size(), 1);
    PolicyEvaluationDetail detail = returnedPrivileges.getEvaluationDetails().get(0);
    assertEquals(detail.getPolicyName(), "policy1");
    assertEquals(detail.getReason(), "Access denied by policy1");
  }

  @Test
  public void testGetGrantedPrivilegesUnauthorized() {
    // Setup
    GetGrantedPrivilegesInput input = new GetGrantedPrivilegesInput();
    input.setActorUrn("urn:li:corpuser:different");
    when(environment.getArgument("input")).thenReturn(input);
    when(context.getActorUrn()).thenReturn(ACTOR_URN);
    policyAuthUtils.when(() -> PolicyAuthUtils.canManagePolicies(context)).thenReturn(false);
    resolverUtils
        .when(() -> bindArgument(eq(input), eq(GetGrantedPrivilegesInput.class)))
        .thenReturn(input);

    // Execute and Verify
    assertThrows(AuthorizationException.class, () -> resolver.get(environment));
  }

  @Test
  public void testGetGrantedPrivilegesUnsupportedAuthorizer() {
    // Setup
    GetGrantedPrivilegesInput input = new GetGrantedPrivilegesInput();
    input.setActorUrn(ACTOR_URN);
    when(environment.getArgument("input")).thenReturn(input);
    when(context.getActorUrn()).thenReturn(ACTOR_URN);
    when(context.getAuthorizer()).thenReturn(mock(DataHubAuthorizer.class));
    policyAuthUtils.when(() -> PolicyAuthUtils.canManagePolicies(context)).thenReturn(false);
    resolverUtils
        .when(() -> bindArgument(eq(input), eq(GetGrantedPrivilegesInput.class)))
        .thenReturn(input);

    // Execute and Verify
    assertThrows(UnsupportedOperationException.class, () -> resolver.get(environment));
  }
}
