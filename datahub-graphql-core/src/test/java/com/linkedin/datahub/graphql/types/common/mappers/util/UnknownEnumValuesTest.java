package com.linkedin.datahub.graphql.types.common.mappers.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.datahub.graphql.generated.IncidentState;
import com.linkedin.datahub.graphql.generated.PolicyType;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UnknownEnumValuesTest {

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlings.reset();
  }

  @Test
  public void testTolerantFallsBackForUnknownValues() {
    assertEquals(
        UnknownEnumValues.map(
            IncidentState.class,
            com.linkedin.incident.IncidentState.$UNKNOWN,
            IncidentState.ACTIVE),
        IncidentState.ACTIVE);
    assertNull(
        UnknownEnumValues.mapOrNull(
            IncidentState.class, com.linkedin.incident.IncidentState.$UNKNOWN));
    assertEquals(
        UnknownEnumValues.fromString(
            PolicyType.class, "TYPE_FROM_NEWER_BUILD", PolicyType.METADATA),
        PolicyType.METADATA);
  }

  @Test
  public void testStrictThrowsForUnknownValues() {
    UnknownDataHandlings.use(StrictHandling.INSTANCE);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            UnknownEnumValues.map(
                IncidentState.class,
                com.linkedin.incident.IncidentState.$UNKNOWN,
                IncidentState.ACTIVE));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            UnknownEnumValues.mapOrNull(
                IncidentState.class, com.linkedin.incident.IncidentState.$UNKNOWN));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            UnknownEnumValues.fromString(
                PolicyType.class, "TYPE_FROM_NEWER_BUILD", PolicyType.METADATA));
    assertThrows(
        NullPointerException.class,
        () -> UnknownEnumValues.fromString(PolicyType.class, null, PolicyType.METADATA));
  }

  @Test
  public void testStrictMapsKnownValues() {
    UnknownDataHandlings.use(StrictHandling.INSTANCE);

    assertEquals(
        UnknownEnumValues.map(
            IncidentState.class,
            com.linkedin.incident.IncidentState.RESOLVED,
            IncidentState.ACTIVE),
        IncidentState.RESOLVED);
    assertEquals(
        UnknownEnumValues.fromString(PolicyType.class, "PLATFORM", PolicyType.METADATA),
        PolicyType.PLATFORM);
  }
}
