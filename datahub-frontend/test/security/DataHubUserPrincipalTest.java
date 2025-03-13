package security;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class DataHubUserPrincipalTest {

  @Test
  public void testGetName() {
    DataHubUserPrincipal principal = new DataHubUserPrincipal("testuser");
    assertEquals("testuser", principal.getName(), "Principal name should match constructor value");
  }

  @Test
  public void testEquals() {
    DataHubUserPrincipal principal1 = new DataHubUserPrincipal("testuser");
    DataHubUserPrincipal principal2 = new DataHubUserPrincipal("testuser");
    DataHubUserPrincipal principal3 = new DataHubUserPrincipal("otheruser");

    // Test equality with same name
    assertTrue(principal1.equals(principal2), "Principals with same name should be equal");
    assertTrue(principal2.equals(principal1), "Equals should be symmetric");

    // Test inequality with different name
    assertFalse(
        principal1.equals(principal3), "Principals with different names should not be equal");

    // Test with null and different object type
    assertFalse(principal1.equals(null), "Principal should not equal null");
    assertFalse(principal1.equals("testuser"), "Principal should not equal string with same name");

    // Test reflexivity
    assertTrue(principal1.equals(principal1), "Principal should equal itself");
  }

  @Test
  public void testHashCode() {
    DataHubUserPrincipal principal1 = new DataHubUserPrincipal("testuser");
    DataHubUserPrincipal principal2 = new DataHubUserPrincipal("testuser");

    // Test hash code consistency
    assertEquals(
        principal1.hashCode(),
        principal2.hashCode(),
        "Equal principals should have same hash code");

    // Test hash code is based on name
    assertEquals(
        "testuser".hashCode(),
        principal1.hashCode(),
        "Principal hash code should be based on name");
  }

  @Test
  public void testToString() {
    DataHubUserPrincipal principal = new DataHubUserPrincipal("testuser");
    String expectedString = "DataHubUserPrincipal[testuser]";

    assertEquals(
        expectedString, principal.toString(), "toString should return formatted principal name");
  }
}
