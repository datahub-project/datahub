package com.linkedin.datahub.graphql.resolvers.auth;

import static org.testng.Assert.*;

import com.datahub.authentication.AccessTokenConfiguration;
import com.datahub.authentication.token.IsoDurationParser;
import org.testng.annotations.Test;

public class IsoDurationParserTest {

  @Test
  public void testParseHour() {
    assertEquals(IsoDurationParser.parseToMillis("PT1H"), 3_600_000L);
  }

  @Test
  public void testParseDay() {
    assertEquals(IsoDurationParser.parseToMillis("P1D"), 86_400_000L);
  }

  @Test
  public void testParseWeek() {
    assertEquals(IsoDurationParser.parseToMillis("P7D"), 604_800_000L);
    assertEquals(IsoDurationParser.parseToMillis("P1W"), 604_800_000L);
  }

  @Test
  public void testParseMonthApproximation() {
    assertEquals(IsoDurationParser.parseToMillis("P1M"), 2_592_000_000L);
    assertEquals(IsoDurationParser.parseToMillis("P30D"), 2_592_000_000L);
  }

  @Test
  public void testParseYearApproximation() {
    assertEquals(IsoDurationParser.parseToMillis("P1Y"), 31_536_000_000L);
    assertEquals(IsoDurationParser.parseToMillis("P365D"), 31_536_000_000L);
    assertEquals(IsoDurationParser.parseToMillis("P3Y"), 94_608_000_000L);
  }

  @Test
  public void testCaseInsensitive() {
    assertEquals(IsoDurationParser.parseToMillis("pt1h"), 3_600_000L);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectBlank() {
    IsoDurationParser.parseToMillis("  ");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectZero() {
    IsoDurationParser.parseToMillis("PT0S");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectInvalid() {
    IsoDurationParser.parseToMillis("THREE_YEARS");
  }

  @Test
  public void testAccessTokenConfigurationDefaults() {
    AccessTokenConfiguration config = AccessTokenConfiguration.defaults();
    assertFalse(config.isAllowNoExpiry());
    assertTrue(config.isDurationMillisAllowed(2_592_000_000L));
  }

  @Test
  public void testAccessTokenConfigurationMillisEquivalence() {
    AccessTokenConfiguration config = new AccessTokenConfiguration();
    config.setAllowedDurations("P30D");
    assertTrue(config.isDurationMillisAllowed(IsoDurationParser.parseToMillis("P1M")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAccessTokenConfigurationRejectsEmpty() {
    AccessTokenConfiguration config = new AccessTokenConfiguration();
    config.setAllowedDurations("");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAccessTokenConfigurationRejectsEmptyCsvItem() {
    AccessTokenConfiguration config = new AccessTokenConfiguration();
    config.setAllowedDurations("P30D,,P1D");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAccessTokenConfigurationRejectsTrailingComma() {
    AccessTokenConfiguration config = new AccessTokenConfiguration();
    config.setAllowedDurations("P30D,");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testAllowedDurationsIsUnmodifiable() {
    AccessTokenConfiguration.defaults().getAllowedDurations().add("P1D");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testAllowedDurationMillisIsUnmodifiable() {
    AccessTokenConfiguration.defaults().getAllowedDurationMillis().add(1L);
  }
}
