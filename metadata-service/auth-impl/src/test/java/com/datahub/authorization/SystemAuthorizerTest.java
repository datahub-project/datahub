package com.datahub.authorization;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SystemAuthorizerTest {

  @Test
  public void testAuthorizeWithSystemActor() {
    // Given
    AuthorizationRequest request =
        new AuthorizationRequest(
            Constants.SYSTEM_ACTOR, "EDIT_ENTITY", Optional.empty(), Collections.emptySet());

    // When
    AuthorizationResult result = Authorizer.SYSTEM.authorize(request);

    // Then
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getType(), AuthorizationResult.Type.ALLOW);
    Assert.assertEquals(result.getRequest(), request);
    Assert.assertEquals(result.getMessage(), "Granted by system actor " + Constants.SYSTEM_ACTOR);
  }

  @Test
  public void testAuthorizeWithNonSystemActor() {
    // Given
    String nonSystemActorUrn = "urn:li:corpuser:testuser";
    AuthorizationRequest request =
        new AuthorizationRequest(
            nonSystemActorUrn, "EDIT_ENTITY", Optional.empty(), Collections.emptySet());

    // When
    AuthorizationResult result = Authorizer.SYSTEM.authorize(request);

    // Then
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getType(), AuthorizationResult.Type.DENY);
    Assert.assertEquals(result.getRequest(), request);
    Assert.assertEquals(result.getMessage(), "Only system user is allowed.");
  }

  @Test
  public void testAuthorizeWithNullActorUrn() {
    // Given
    AuthorizationRequest request =
        new AuthorizationRequest(null, "EDIT_ENTITY", Optional.empty(), Collections.emptySet());

    // When
    AuthorizationResult result = Authorizer.SYSTEM.authorize(request);

    // Then
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getType(), AuthorizationResult.Type.DENY);
    Assert.assertEquals(result.getRequest(), request);
    Assert.assertEquals(result.getMessage(), "Only system user is allowed.");
  }

  @Test
  public void testAuthorizeWithEmptyActorUrn() {
    // Given
    AuthorizationRequest request =
        new AuthorizationRequest("", "EDIT_ENTITY", Optional.empty(), Collections.emptySet());

    // When
    AuthorizationResult result = Authorizer.SYSTEM.authorize(request);

    // Then
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getType(), AuthorizationResult.Type.DENY);
    Assert.assertEquals(result.getRequest(), request);
    Assert.assertEquals(result.getMessage(), "Only system user is allowed.");
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testAuthorizeWithNullRequest() {
    // When
    Authorizer.SYSTEM.authorize(null);

    // Then - expect NullPointerException due to @Nonnull annotation
  }

  @Test
  public void testMultipleAuthorizationCalls() {
    // Test that the authorizer can handle multiple calls correctly

    // First call with system actor
    AuthorizationRequest systemRequest =
        new AuthorizationRequest(
            Constants.SYSTEM_ACTOR, "DELETE_ENTITY", Optional.empty(), Collections.emptySet());
    AuthorizationResult systemResult = Authorizer.SYSTEM.authorize(systemRequest);
    Assert.assertEquals(systemResult.getType(), AuthorizationResult.Type.ALLOW);

    // Second call with non-system actor
    AuthorizationRequest userRequest =
        new AuthorizationRequest(
            "urn:li:corpuser:user1", "DELETE_ENTITY", Optional.empty(), Collections.emptySet());
    AuthorizationResult userResult = Authorizer.SYSTEM.authorize(userRequest);
    Assert.assertEquals(userResult.getType(), AuthorizationResult.Type.DENY);

    // Third call again with system actor to ensure consistency
    AuthorizationResult systemResult2 = Authorizer.SYSTEM.authorize(systemRequest);
    Assert.assertEquals(systemResult2.getType(), AuthorizationResult.Type.ALLOW);
  }

  @Test
  public void testAuthorizeWithDifferentPrivileges() {
    // Test that authorization is based on actor URN, not privilege

    // System actor with different privileges
    AuthorizationRequest editRequest =
        new AuthorizationRequest(
            Constants.SYSTEM_ACTOR, "EDIT_ENTITY", Optional.empty(), Collections.emptySet());
    AuthorizationRequest deleteRequest =
        new AuthorizationRequest(
            Constants.SYSTEM_ACTOR, "DELETE_ENTITY", Optional.empty(), Collections.emptySet());
    AuthorizationRequest manageRequest =
        new AuthorizationRequest(
            Constants.SYSTEM_ACTOR, "MANAGE_POLICIES", Optional.empty(), Collections.emptySet());

    // All should be allowed
    Assert.assertEquals(
        Authorizer.SYSTEM.authorize(editRequest).getType(), AuthorizationResult.Type.ALLOW);
    Assert.assertEquals(
        Authorizer.SYSTEM.authorize(deleteRequest).getType(), AuthorizationResult.Type.ALLOW);
    Assert.assertEquals(
        Authorizer.SYSTEM.authorize(manageRequest).getType(), AuthorizationResult.Type.ALLOW);

    // Non-system actor with same privileges
    AuthorizationRequest userEditRequest =
        new AuthorizationRequest(
            "urn:li:corpuser:user1", "EDIT_ENTITY", Optional.empty(), Collections.emptySet());

    // Should be denied
    Assert.assertEquals(
        Authorizer.SYSTEM.authorize(userEditRequest).getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void testAuthorizeWithResourceSpec() {
    // Test that authorization works regardless of resource spec

    // Create a mock EntitySpec (you may need to adjust based on actual EntitySpec implementation)
    EntitySpec entitySpec = new EntitySpec("dataset", "urn:li:dataset:test");

    // System actor with resource spec
    AuthorizationRequest withResource =
        new AuthorizationRequest(
            Constants.SYSTEM_ACTOR, "EDIT_ENTITY", Optional.of(entitySpec), Collections.emptySet());

    // System actor without resource spec
    AuthorizationRequest withoutResource =
        new AuthorizationRequest(
            Constants.SYSTEM_ACTOR, "EDIT_ENTITY", Optional.empty(), Collections.emptySet());

    // Both should be allowed
    Assert.assertEquals(
        Authorizer.SYSTEM.authorize(withResource).getType(), AuthorizationResult.Type.ALLOW);
    Assert.assertEquals(
        Authorizer.SYSTEM.authorize(withoutResource).getType(), AuthorizationResult.Type.ALLOW);

    // Non-system actor with resource spec should be denied
    AuthorizationRequest userWithResource =
        new AuthorizationRequest(
            "urn:li:corpuser:user1",
            "EDIT_ENTITY",
            Optional.of(entitySpec),
            Collections.emptySet());
    Assert.assertEquals(
        Authorizer.SYSTEM.authorize(userWithResource).getType(), AuthorizationResult.Type.DENY);
  }
}
