package com.datahub.authentication.session;

import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_STATUS_SUSPENDED;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.LoginDenialReason;
import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.CorpUserKey;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UserSessionEligibilityCheckerTest {

  private static final String USER = "urn:li:corpuser:eligibilitytest";
  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization(
          Mockito.mock(com.linkedin.metadata.aspect.AspectRetriever.class));

  @Test
  public void testMissingKeyWhenEnforced() {
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_KEY_ASPECT_NAME)))
        .thenReturn(null);

    UserSessionEligibilityChecker checker = new UserSessionEligibilityChecker(entityService);
    Optional<LoginDenialReason> r = checker.checkEligibility(opContext, USER, true);
    assertEquals(r, Optional.of(LoginDenialReason.HARD_DELETED));
  }

  @Test
  public void testMissingKeyWhenNotEnforcedStillAllowsWithoutProfile() {
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_KEY_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class), eq(UrnUtils.getUrn(USER)), eq(STATUS_ASPECT_NAME)))
        .thenReturn(null);

    UserSessionEligibilityChecker checker = new UserSessionEligibilityChecker(entityService);
    assertTrue(checker.checkEligibility(opContext, USER, false).isEmpty());
  }

  @Test
  public void testKeyOnlyIsNotProvisioned() {
    CorpUserKey key = new CorpUserKey().setUsername("eligibilitytest");
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_KEY_ASPECT_NAME)))
        .thenReturn(key);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class), eq(UrnUtils.getUrn(USER)), eq(STATUS_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_STATUS_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_INFO_ASPECT_NAME)))
        .thenReturn(null);

    UserSessionEligibilityChecker checker = new UserSessionEligibilityChecker(entityService);
    assertEquals(
        checker.checkEligibility(opContext, USER, true),
        Optional.of(LoginDenialReason.NOT_PROVISIONED));
  }

  @Test
  public void testSoftDeletedPrecedesNotProvisioned() {
    CorpUserKey key = new CorpUserKey().setUsername("eligibilitytest");
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_KEY_ASPECT_NAME)))
        .thenReturn(key);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class), eq(UrnUtils.getUrn(USER)), eq(STATUS_ASPECT_NAME)))
        .thenReturn(new Status().setRemoved(true));

    UserSessionEligibilityChecker checker = new UserSessionEligibilityChecker(entityService);
    assertEquals(
        checker.checkEligibility(opContext, USER, true),
        Optional.of(LoginDenialReason.SOFT_DELETED));
  }

  @Test
  public void testSuspendedPrecedesNotProvisioned() {
    CorpUserKey key = new CorpUserKey().setUsername("eligibilitytest");
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_KEY_ASPECT_NAME)))
        .thenReturn(key);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class), eq(UrnUtils.getUrn(USER)), eq(STATUS_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_STATUS_ASPECT_NAME)))
        .thenReturn(new CorpUserStatus().setStatus(CORP_USER_STATUS_SUSPENDED));

    UserSessionEligibilityChecker checker = new UserSessionEligibilityChecker(entityService);
    assertEquals(
        checker.checkEligibility(opContext, USER, true), Optional.of(LoginDenialReason.SUSPENDED));
  }

  @Test
  public void testInactiveCorpUserInfo() {
    CorpUserKey key = new CorpUserKey().setUsername("eligibilitytest");
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_KEY_ASPECT_NAME)))
        .thenReturn(key);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class), eq(UrnUtils.getUrn(USER)), eq(STATUS_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_STATUS_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_INFO_ASPECT_NAME)))
        .thenReturn(new CorpUserInfo().setActive(false));

    UserSessionEligibilityChecker checker = new UserSessionEligibilityChecker(entityService);
    assertEquals(
        checker.checkEligibility(opContext, USER, true), Optional.of(LoginDenialReason.INACTIVE));
  }

  @Test
  public void testResolvesBareUsernameToCorpuserUrn() {
    CorpUserKey key = new CorpUserKey().setUsername("jdoe");
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn("urn:li:corpuser:jdoe")),
                eq(CORP_USER_KEY_ASPECT_NAME)))
        .thenReturn(key);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn("urn:li:corpuser:jdoe")),
                eq(STATUS_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn("urn:li:corpuser:jdoe")),
                eq(CORP_USER_STATUS_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn("urn:li:corpuser:jdoe")),
                eq(CORP_USER_INFO_ASPECT_NAME)))
        .thenReturn(new CorpUserInfo().setActive(true));

    UserSessionEligibilityChecker checker = new UserSessionEligibilityChecker(entityService);
    assertTrue(checker.checkEligibility(opContext, "jdoe", true).isEmpty());
  }

  @Test
  public void testCorpUserInfoPresentPasses() {
    CorpUserKey key = new CorpUserKey().setUsername("eligibilitytest");
    EntityService<?> entityService = Mockito.mock(EntityService.class);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_KEY_ASPECT_NAME)))
        .thenReturn(key);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class), eq(UrnUtils.getUrn(USER)), eq(STATUS_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_STATUS_ASPECT_NAME)))
        .thenReturn(null);
    Mockito.when(
            entityService.getLatestAspect(
                any(OperationContext.class),
                eq(UrnUtils.getUrn(USER)),
                eq(CORP_USER_INFO_ASPECT_NAME)))
        .thenReturn(new CorpUserInfo().setActive(true));

    UserSessionEligibilityChecker checker = new UserSessionEligibilityChecker(entityService);
    assertTrue(checker.checkEligibility(opContext, USER, true).isEmpty());
  }
}
