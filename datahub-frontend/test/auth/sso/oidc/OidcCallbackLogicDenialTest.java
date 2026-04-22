package auth.sso.oidc;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import auth.CookieConfigs;
import auth.sso.SsoManager;
import client.AuthServiceClient;
import com.datahub.authentication.LoginDenialReason;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

public class OidcCallbackLogicDenialTest {

  @Test
  public void verifyPreProvisionedUserRejectsStubWithAtMostOneAspect() throws Exception {
    SystemEntityClient systemEntityClient = mock(SystemEntityClient.class);
    Entity corpUser = mock(Entity.class, Answers.RETURNS_DEEP_STUBS);
    when(corpUser.getValue().getCorpUserSnapshot().getAspects().size()).thenReturn(1);
    when(systemEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(corpUser);

    OidcCallbackLogic logic = newOidcCallbackLogic(systemEntityClient, false);
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                logic.verifyPreProvisionedUser(
                    mock(OperationContext.class), new CorpuserUrn("stubuser")));
    assertTrue(ex.getMessage().contains("has not yet been provisioned"));
  }

  @Test
  public void verifyPreProvisionedUserWrapsRemoteInvocationException() throws Exception {
    SystemEntityClient systemEntityClient = mock(SystemEntityClient.class);
    when(systemEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenThrow(new RemoteInvocationException("GMS error"));

    OidcCallbackLogic logic = newOidcCallbackLogic(systemEntityClient, false);
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () ->
                logic.verifyPreProvisionedUser(
                    mock(OperationContext.class), new CorpuserUrn("anyone")));
    assertTrue(ex.getMessage().contains("Failed to validate user"));
  }

  @Test
  public void emitOidcLoginDeniedLogRoutineReasonWithVerboseSecondLine() {
    OidcCallbackLogic logic = newOidcCallbackLogic(mock(SystemEntityClient.class), true);
    logic.emitOidcLoginDeniedLog("verboseUser", LoginDenialReason.NOT_PROVISIONED);
  }

  @Test
  public void emitOidcLoginDeniedLogWarnReasonWithoutVerbose() {
    OidcCallbackLogic logic = newOidcCallbackLogic(mock(SystemEntityClient.class), false);
    logic.emitOidcLoginDeniedLog("warnUser", LoginDenialReason.SESSION_TOKEN_DENIED);
  }

  @Test
  public void emitOidcLoginDeniedLogWarnReasonWithVerboseSecondLine() {
    OidcCallbackLogic logic = newOidcCallbackLogic(mock(SystemEntityClient.class), true);
    logic.emitOidcLoginDeniedLog("warnUserVerbose", LoginDenialReason.UNKNOWN);
  }

  private static OidcCallbackLogic newOidcCallbackLogic(
      SystemEntityClient systemEntityClient, boolean authVerboseLogging) {
    return new OidcCallbackLogic(
        mock(SsoManager.class),
        mock(OperationContext.class),
        systemEntityClient,
        mock(AuthServiceClient.class),
        mock(CookieConfigs.class),
        "",
        authVerboseLogging);
  }
}
