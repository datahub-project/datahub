package io.datahubproject.metadata.services;

import static com.github.stefanbirkner.systemlambda.SystemLambda.*;
import static org.testng.Assert.*;

import java.util.Set;
import org.testng.annotations.Test;

/** Tests for {@link SecretMasker}. Uses system-lambda for environment variable mocking. */
public class SecretMaskerTest {

  @Test
  public void testMasksSingleSecret() throws Exception {
    withEnvironmentVariable("TEST_PASSWORD", "secret123")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("TEST_PASSWORD"));

              String input = "Connecting with password: secret123";
              String result = masker.mask(input);

              assertEquals(result, "Connecting with password: ${TEST_PASSWORD}");
              assertFalse(result.contains("secret123"));
            });
  }

  @Test
  public void testMasksMultipleSecrets() throws Exception {
    withEnvironmentVariable("DB_PASSWORD", "pass12345")
        .and("API_KEY", "key45678")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("DB_PASSWORD", "API_KEY"));

              String input = "DB: pass12345, API: key45678";
              String result = masker.mask(input);

              assertTrue(result.contains("${DB_PASSWORD}"));
              assertTrue(result.contains("${API_KEY}"));
              assertFalse(result.contains("pass12345"));
              assertFalse(result.contains("key45678"));
            });
  }

  @Test
  public void testNoMaskingWhenNoSecrets() {
    SecretMasker masker = new SecretMasker(Set.of());

    String input = "Normal log message";
    String result = masker.mask(input);

    assertEquals(result, input);
    assertFalse(masker.isEnabled());
  }

  @Test
  public void testExtractsEnvVarReferences() {
    String recipe =
        """
            source:
              type: mysql
              config:
                host: ${DB_HOST}
                password: ${DB_PASSWORD}
            """;

    Set<String> vars = SecretMasker.extractEnvVarReferences(recipe);

    assertEquals(vars.size(), 2);
    assertTrue(vars.contains("DB_HOST"));
    assertTrue(vars.contains("DB_PASSWORD"));
  }

  @Test
  public void testExtractsNoVarsFromPlainRecipe() {
    String recipe =
        """
            source:
              type: mysql
              config:
                host: localhost
                password: hardcoded
            """;

    Set<String> vars = SecretMasker.extractEnvVarReferences(recipe);

    assertEquals(vars.size(), 0);
  }

  @Test
  public void testHandlesNullAndEmpty() {
    SecretMasker masker = new SecretMasker(Set.of("TEST"));

    assertEquals(masker.mask(null), "");
    assertEquals(masker.mask(""), "");
  }

  @Test
  public void testExcludesSystemVars() {
    // System vars like PATH should be excluded
    SecretMasker masker = new SecretMasker(Set.of("PATH", "HOME", "USER"));

    // Masker should not be enabled for system vars
    assertFalse(masker.isEnabled());
  }

  @Test
  public void testMixedSystemAndSecretVars() throws Exception {
    withEnvironmentVariable("MY_SECRET", "secret123")
        .execute(
            () -> {
              // Mix system vars with actual secret var
              SecretMasker masker = new SecretMasker(Set.of("PATH", "MY_SECRET"));

              // Should be enabled (has non-system var)
              assertTrue(masker.isEnabled());

              String input = "Using secret: secret123";
              String result = masker.mask(input);

              assertTrue(result.contains("${MY_SECRET}"));
              assertFalse(result.contains("secret123"));
            });
  }

  @Test
  public void testLongestMatchFirst() throws Exception {
    withEnvironmentVariable("SHORT_SECRET", "abcdef12")
        .and("LONG_SECRET", "abcdef1234567890")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("SHORT_SECRET", "LONG_SECRET"));

              // Test that longer secret matches first
              String input = "Value is abcdef1234567890 here";
              String result = masker.mask(input);

              // Should match LONG_SECRET, not SHORT_SECRET
              assertTrue(result.contains("${LONG_SECRET}"));
              assertFalse(result.contains("${SHORT_SECRET}"));
              assertFalse(result.contains("abcdef1234567890"));
            });
  }

  @Test
  public void testSpecialRegexCharactersEscaped() throws Exception {
    withEnvironmentVariable("SPECIAL_SECRET", "secret$with.special*chars")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("SPECIAL_SECRET"));

              String input = "Password: secret$with.special*chars";
              String result = masker.mask(input);

              assertTrue(result.contains("${SPECIAL_SECRET}"));
              assertFalse(result.contains("secret$with.special*chars"));
            });
  }
}
