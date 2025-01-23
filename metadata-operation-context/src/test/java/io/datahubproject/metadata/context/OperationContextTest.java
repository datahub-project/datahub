package io.datahubproject.metadata.context;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class OperationContextTest {

  @Test
  public void testSystemPrivilegeEscalation() {
    Authentication systemAuth = new Authentication(new Actor(ActorType.USER, "SYSTEM"), "");
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "USER"), "");

    // Allows system authentication
    OperationContext systemOpContext =
        OperationContext.asSystem(
            OperationContextConfig.builder().build(),
            systemAuth,
            mock(EntityRegistry.class),
            mock(ServicesRegistryContext.class),
            null,
            TestOperationContexts.emptyActiveUsersRetrieverContext(null),
            mock(ValidationContext.class),
            true);

    OperationContext opContext =
        systemOpContext.asSession(RequestContext.TEST, Authorizer.EMPTY, userAuth);

    assertEquals(
        opContext.getAuthentication(), systemAuth, "Expected system authentication when allowed");
    assertEquals(
        opContext.getAuditStamp().getActor().getId(),
        "USER",
        "Audit stamp expected to match the user's identity");
    assertEquals(opContext.getSessionAuthentication(), userAuth);
    assertEquals(opContext.getSessionActorContext().getAuthentication(), userAuth);
    assertEquals(opContext.getActorContext().getAuthentication(), systemAuth);
    assertEquals(opContext.getSystemActorContext().getAuthentication(), systemAuth);
    assertEquals(opContext.getSystemAuthentication().get(), systemAuth);

    // Do not allow system auth
    OperationContext opContextNoSystem =
        systemOpContext.toBuilder()
            .operationContextConfig(
                systemOpContext.getOperationContextConfig().toBuilder()
                    .allowSystemAuthentication(false)
                    .build())
            .build(userAuth, true);

    assertEquals(
        opContextNoSystem.getAuthentication(),
        userAuth,
        "Expect user authentication when system authentication is not allowed");
    assertEquals(
        opContextNoSystem.getAuditStamp().getActor().getId(),
        "USER",
        "Audit stamp expected to match the user's identity");
    assertEquals(opContextNoSystem.getSessionActorContext().getAuthentication(), userAuth);
    assertEquals(opContextNoSystem.getActorContext().getAuthentication(), userAuth);
    assertEquals(opContextNoSystem.getSystemActorContext().getAuthentication(), systemAuth);
    assertEquals(opContextNoSystem.getSystemAuthentication().get(), systemAuth);
    assertEquals(opContextNoSystem.getSystemActorContext().getAuthentication(), systemAuth);
    assertEquals(opContextNoSystem.getSessionAuthentication(), userAuth);
  }
}
