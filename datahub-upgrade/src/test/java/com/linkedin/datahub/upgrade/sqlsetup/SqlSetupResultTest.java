package com.linkedin.datahub.upgrade.sqlsetup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class SqlSetupResultTest {

  @Test
  public void testDefaultValues() {
    SqlSetupResult result = new SqlSetupResult();

    assertEquals(result.tablesCreated, 0);
    assertEquals(result.usersCreated, 0);
    assertEquals(result.cdcUserCreated, false);
    assertEquals(result.executionTimeMs, 0);
    assertEquals(result.errorMessage, null);
  }

  @Test
  public void testSettersAndGetters() {
    SqlSetupResult result = new SqlSetupResult();

    result.tablesCreated = 5;
    result.usersCreated = 2;
    result.cdcUserCreated = true;
    result.executionTimeMs = 1500L;
    result.errorMessage = "Test error";

    assertEquals(result.tablesCreated, 5);
    assertEquals(result.usersCreated, 2);
    assertEquals(result.cdcUserCreated, true);
    assertEquals(result.executionTimeMs, 1500L);
    assertEquals(result.errorMessage, "Test error");
  }

  @Test
  public void testToString() {
    SqlSetupResult result = new SqlSetupResult();
    result.tablesCreated = 3;
    result.usersCreated = 1;
    result.cdcUserCreated = true;
    result.executionTimeMs = 2000L;

    String toString = result.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("tablesCreated=3"));
    assertTrue(toString.contains("usersCreated=1"));
    assertTrue(toString.contains("cdcUserCreated=true"));
    assertTrue(toString.contains("executionTimeMs=2000"));
  }

  @Test
  public void testToStringWithZeroValues() {
    SqlSetupResult result = new SqlSetupResult();
    // All values are default (0/false)

    String toString = result.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("tablesCreated=0"));
    assertTrue(toString.contains("usersCreated=0"));
    assertTrue(toString.contains("cdcUserCreated=false"));
    assertTrue(toString.contains("executionTimeMs=0"));
  }

  @Test
  public void testToStringWithLargeValues() {
    SqlSetupResult result = new SqlSetupResult();
    result.tablesCreated = 1000;
    result.usersCreated = 500;
    result.cdcUserCreated = false;
    result.executionTimeMs = 999999L;

    String toString = result.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("tablesCreated=1000"));
    assertTrue(toString.contains("usersCreated=500"));
    assertTrue(toString.contains("cdcUserCreated=false"));
    assertTrue(toString.contains("executionTimeMs=999999"));
  }
}
