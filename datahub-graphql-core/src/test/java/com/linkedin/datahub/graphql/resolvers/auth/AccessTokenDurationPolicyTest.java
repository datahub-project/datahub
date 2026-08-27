package com.linkedin.datahub.graphql.resolvers.auth;

import static org.testng.Assert.*;

import com.datahub.authentication.AccessTokenConfiguration;
import com.linkedin.datahub.graphql.generated.AccessTokenDuration;
import java.util.Optional;
import org.testng.annotations.Test;

public class AccessTokenDurationPolicyTest {

  @Test
  public void testResolveIsoAllowed() {
    AccessTokenConfiguration policy = AccessTokenConfiguration.defaults();
    Optional<Long> ms = AccessTokenDurationPolicy.resolveExpiresInMs(policy, null, "P30D");
    assertTrue(ms.isPresent());
    assertEquals(ms.get().longValue(), 2_592_000_000L);
  }

  @Test
  public void testResolveEnumAllowed() {
    AccessTokenConfiguration policy = AccessTokenConfiguration.defaults();
    Optional<Long> ms =
        AccessTokenDurationPolicy.resolveExpiresInMs(policy, AccessTokenDuration.ONE_HOUR, null);
    assertTrue(ms.isPresent());
    assertEquals(ms.get().longValue(), 3_600_000L);
  }

  @Test
  public void testRejectNoExpiryWhenDisabled() {
    AccessTokenConfiguration policy = AccessTokenConfiguration.defaults();
    IllegalArgumentException thrown =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                AccessTokenDurationPolicy.resolveExpiresInMs(
                    policy, AccessTokenDuration.NO_EXPIRY, null));
    assertTrue(thrown.getMessage().contains("ACCESS_TOKEN_ALLOW_NO_EXPIRY"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("ACCESS_TOKEN_ALLOWED_DURATIONS"), thrown.getMessage());
  }

  @Test
  public void testAllowNoExpiryWhenEnabled() {
    AccessTokenConfiguration policy = AccessTokenConfiguration.defaults();
    policy.setAllowNoExpiry(true);
    Optional<Long> ms =
        AccessTokenDurationPolicy.resolveExpiresInMs(policy, AccessTokenDuration.NO_EXPIRY, null);
    assertFalse(ms.isPresent());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectBothDurationAndIso() {
    AccessTokenConfiguration policy = AccessTokenConfiguration.defaults();
    AccessTokenDurationPolicy.resolveExpiresInMs(policy, AccessTokenDuration.ONE_DAY, "P1D");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectEnumWithBlankDurationIso() {
    AccessTokenConfiguration policy = AccessTokenConfiguration.defaults();
    AccessTokenDurationPolicy.resolveExpiresInMs(policy, AccessTokenDuration.ONE_DAY, "  ");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectBlankDurationIsoAlone() {
    AccessTokenConfiguration policy = AccessTokenConfiguration.defaults();
    AccessTokenDurationPolicy.resolveExpiresInMs(policy, null, "  ");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectNeitherDurationNorIso() {
    AccessTokenConfiguration policy = AccessTokenConfiguration.defaults();
    AccessTokenDurationPolicy.resolveExpiresInMs(policy, null, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectDisallowedIso() {
    AccessTokenConfiguration policy = new AccessTokenConfiguration();
    policy.setAllowedDurations("PT1H");
    AccessTokenDurationPolicy.resolveExpiresInMs(policy, null, "P3Y");
  }

  @Test
  public void testCustomIsoWhenAllowlisted() {
    AccessTokenConfiguration policy = new AccessTokenConfiguration();
    policy.setAllowedDurations("PT1H,P3Y");
    Optional<Long> ms = AccessTokenDurationPolicy.resolveExpiresInMs(policy, null, "P3Y");
    assertTrue(ms.isPresent());
    assertEquals(ms.get().longValue(), 94_608_000_000L);
  }
}
