package com.linkedin.metadata.datahubusage.event;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

public class LoginSourceTest {

  @Test
  public void testGetSourceAcceptsCamelCaseWireValue() {
    assertEquals(LoginSource.getSource("passwordLogin"), LoginSource.PASSWORD_LOGIN);
    assertEquals(LoginSource.getSource("ssoLogin"), LoginSource.SSO_LOGIN);
  }

  @Test
  public void testGetSourceAcceptsEnumConstantName() {
    // Clients and older tests sometimes send the enum name instead of the camelCase source.
    assertEquals(LoginSource.getSource("PASSWORD_LOGIN"), LoginSource.PASSWORD_LOGIN);
    assertEquals(LoginSource.getSource("SSO_LOGIN"), LoginSource.SSO_LOGIN);
    assertEquals(LoginSource.getSource("password_login"), LoginSource.PASSWORD_LOGIN);
  }

  @Test
  public void testGetSourceCaseInsensitive() {
    assertEquals(LoginSource.getSource("PasswordLogin"), LoginSource.PASSWORD_LOGIN);
    assertEquals(LoginSource.getSource("Password_Login"), LoginSource.PASSWORD_LOGIN);
  }

  @Test
  public void testGetSourceUnknownOrBlankReturnsNull() {
    assertNull(LoginSource.getSource("not-a-real-source"));
    assertNull(LoginSource.getSource(""));
    assertNull(LoginSource.getSource("   "));
    assertNull(LoginSource.getSource(null));
  }

  @Test
  public void testGetSourceRoundTripForAllValues() {
    for (LoginSource loginSource : LoginSource.values()) {
      assertEquals(LoginSource.getSource(loginSource.getSource()), loginSource);
      assertEquals(LoginSource.getSource(loginSource.name()), loginSource);
    }
  }
}
