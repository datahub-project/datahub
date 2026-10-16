package com.linkedin.metadata.utils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TolerantHandlingTest {

  private final UnknownDataHandling handling = TolerantHandling.INSTANCE;
  private final UnknownDataGuard site =
      UnknownDataGuard.forSite(TolerantHandlingTest.class, "test row");
  private EntityRegistry registry;
  private AspectSpec status;

  @BeforeMethod
  public void setUp() {
    UnknownDataGuard.resetLogWindows();
    status = mock(AspectSpec.class);
    EntitySpec dataset = mock(EntitySpec.class);
    when(dataset.getAspectSpec("status")).thenReturn(status);
    registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec(anyString())).thenReturn(Optional.empty());
    when(registry.findEntitySpec("dataset")).thenReturn(Optional.of(dataset));
    when(registry.findAspectSpec(anyString(), anyString())).thenCallRealMethod();
  }

  @AfterMethod
  public void tearDown() {
    UnknownDataHandlings.reset();
  }

  @Test
  public void testAdmitsKnownDataAndSkipsUnknown() {
    assertTrue(handling.admit(site, registry, Optional.empty(), "dataset", "status", "urn"));
    assertFalse(
        handling.admit(site, registry, Optional.empty(), "dataset", "aspectFromNewerBuild", "u"));
    assertFalse(
        handling.admitUrn(site, registry, Optional.empty(), "urn:li:entityFromNewerBuild:x", null));
    assertFalse(
        handling.admitEvent(
            site,
            registry,
            Optional.empty(),
            UrnUtils.getUrn("urn:li:dataset:(urn:li:entityFromNewerBuild:x,col)"),
            "dataset",
            "status"));
  }

  @Test
  public void testLookupsAreEmptyForUnknownData() {
    assertTrue(handling.entitySpecForData(registry, "dataset").isPresent());
    assertTrue(handling.entitySpecForData(registry, "entityFromNewerBuild").isEmpty());
    assertSame(handling.aspectSpecForData(registry, "dataset", "status").orElseThrow(), status);
    assertTrue(handling.aspectSpecForData(registry, "dataset", "aspectFromNewerBuild").isEmpty());
  }

  @Test
  public void testClassifiesUnknownData() {
    assertTrue(handling.isUnknown(registry, "entityFromNewerBuild", null));
    assertFalse(handling.isUnknown(registry, "dataset", "status"));
    assertTrue(handling.isUnknownUrn(registry, "urn:li:entityFromNewerBuild:x", null));
    assertFalse(handling.isUnknownUrn(registry, "not-a-urn", null));
    assertFalse(handling.isUnknownUrn(registry, "urn:li:dataset:x", "status"));
    assertTrue(
        handling.referencesUnknownEntityType(
            registry, UrnUtils.getUrn("urn:li:dataset:(urn:li:entityFromNewerBuild:x,col)")));
    assertTrue(handling.isTolerant());
  }

  @Test
  public void testUrnCountsAsUnknownExactlyWhenItsRowIsSkipped() {
    // Rollback counts the rows it leaves in place with isUnknownUrn and skips them with admitUrn.
    for (String urn :
        new String[] {
          "urn:li:entityFromNewerBuild:x",
          "urn:li:dataset:(urn:li:entityFromNewerBuild:x,col)",
          "urn:li:dataset:x"
        }) {
      assertEquals(
          handling.isUnknownUrn(registry, urn, "status"),
          !handling.admitUrn(site, registry, Optional.empty(), urn, "status"),
          urn);
    }
  }

  @Test
  public void testProcessDefaultsToTolerantAndCanBeSwitched() {
    assertSame(UnknownDataHandlings.handling(), TolerantHandling.INSTANCE);
    UnknownDataHandling other = mock(UnknownDataHandling.class);

    UnknownDataHandlings.use(other);
    assertSame(UnknownDataHandlings.handling(), other);

    UnknownDataHandlings.reset();
    assertEquals(UnknownDataHandlings.handling(), TolerantHandling.INSTANCE);
  }

  @Test
  public void testUnknownEntityTypeIsToleratedOnlyWhenANewerVersionRegisteredIt() {
    doThrow(new IllegalArgumentException("Failed to find entity"))
        .when(registry)
        .getEntitySpec("corpusr");
    UnknownDataHandling rows =
        TolerantHandling.forNextVersionEntityTypes("entityFromNewerBuild"::equals);

    assertFalse(
        rows.admitUrn(site, registry, Optional.empty(), "urn:li:entityFromNewerBuild:x", null));
    assertTrue(rows.isUnknown(registry, "entityFromNewerBuild", null));
    assertTrue(rows.entitySpecForData(registry, "entityFromNewerBuild").isEmpty());

    // No entityType row (e.g. a typo): handled as before unknown data was tolerated.
    assertTrue(rows.admitUrn(site, registry, Optional.empty(), "urn:li:corpusr:bob", null));
    assertTrue(rows.admit(site, registry, Optional.empty(), "corpusr", "status", "urn"));
    assertFalse(rows.isUnknown(registry, "corpusr", null));
    assertFalse(rows.referencesUnknownEntityType(registry, UrnUtils.getUrn("urn:li:corpusr:bob")));
    expectThrows(IllegalArgumentException.class, () -> rows.entitySpecForData(registry, "corpusr"));

    // Unknown aspects of known types are still tolerated.
    assertFalse(
        rows.admit(site, registry, Optional.empty(), "dataset", "aspectFromNewerBuild", "urn"));
  }
}
