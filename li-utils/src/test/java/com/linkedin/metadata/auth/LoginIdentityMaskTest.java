package com.linkedin.metadata.auth;

import static org.assertj.core.api.Assertions.assertThat;

import org.testng.annotations.Test;

public class LoginIdentityMaskTest {

  @Test
  public void maskNullOrEmpty() {
    assertThat(LoginIdentityMask.mask(null)).isEmpty();
    assertThat(LoginIdentityMask.mask("")).isEmpty();
  }

  @Test
  public void maskStableForSameNormalizedInput() {
    String a = LoginIdentityMask.mask("  DataHub@Example.COM  ");
    String b = LoginIdentityMask.mask("datahub@example.com");
    assertThat(a).isEqualTo(b);
    assertThat(a).startsWith("sha256:");
    assertThat(a.length()).isGreaterThan("sha256:".length());
  }

  @Test
  public void maskDiffersForDifferentInputs() {
    assertThat(LoginIdentityMask.mask("user-a")).isNotEqualTo(LoginIdentityMask.mask("user-b"));
  }
}
