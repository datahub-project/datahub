package com.linkedin.gms.factory.auth;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.TokenServiceConfiguration;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubTokenServiceFactoryTest {

  private DataHubTokenServiceFactory factory;
  private ConfigurationProvider configurationProvider;
  private AuthenticationConfiguration authConfig;
  private TokenServiceConfiguration tokenServiceConfig;

  @BeforeMethod
  public void setUp() throws Exception {
    factory = new DataHubTokenServiceFactory();

    configurationProvider = mock(ConfigurationProvider.class);
    authConfig = mock(AuthenticationConfiguration.class);
    tokenServiceConfig = mock(TokenServiceConfiguration.class);

    when(configurationProvider.getAuthentication()).thenReturn(authConfig);
    when(authConfig.getTokenService()).thenReturn(tokenServiceConfig);

    var field = DataHubTokenServiceFactory.class.getDeclaredField("configurationProvider");
    field.setAccessible(true);
    field.set(factory, configurationProvider);
  }

  @Test
  public void testValidate_authDisabled_skipsKeyChecks() {
    when(authConfig.isEnabled()).thenReturn(false);
    // Should not throw even with no signing key or salt configured
    factory.validate();
  }

  @Test
  public void testValidate_authEnabled_missingSigningKey_throws() {
    when(authConfig.isEnabled()).thenReturn(true);
    when(tokenServiceConfig.getSigningKey()).thenReturn(null);
    when(tokenServiceConfig.getSalt()).thenReturn("some-salt");

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> factory.validate());
    assertEquals(
        e.getMessage(), "authentication.tokenService.signingKey must be set and not be empty");
  }

  @Test
  public void testValidate_authEnabled_emptySigningKey_throws() {
    when(authConfig.isEnabled()).thenReturn(true);
    when(tokenServiceConfig.getSigningKey()).thenReturn("");
    when(tokenServiceConfig.getSalt()).thenReturn("some-salt");

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> factory.validate());
    assertEquals(
        e.getMessage(), "authentication.tokenService.signingKey must be set and not be empty");
  }

  @Test
  public void testValidate_authEnabled_missingSalt_throws() {
    when(authConfig.isEnabled()).thenReturn(true);
    when(tokenServiceConfig.getSigningKey()).thenReturn("some-key");
    when(tokenServiceConfig.getSalt()).thenReturn(null);

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> factory.validate());
    assertEquals(e.getMessage(), "authentication.tokenService.salt must be set and not be empty");
  }

  @Test
  public void testValidate_authEnabled_emptySalt_throws() {
    when(authConfig.isEnabled()).thenReturn(true);
    when(tokenServiceConfig.getSigningKey()).thenReturn("some-key");
    when(tokenServiceConfig.getSalt()).thenReturn("");

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> factory.validate());
    assertEquals(e.getMessage(), "authentication.tokenService.salt must be set and not be empty");
  }

  @Test
  public void testValidate_authEnabled_keysPresent_doesNotThrow() {
    when(authConfig.isEnabled()).thenReturn(true);
    when(tokenServiceConfig.getSigningKey()).thenReturn("some-signing-key");
    when(tokenServiceConfig.getSalt()).thenReturn("some-salt");

    factory.validate();
  }
}
