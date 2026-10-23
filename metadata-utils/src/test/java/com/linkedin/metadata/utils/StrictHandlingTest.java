package com.linkedin.metadata.utils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Optional;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Strict handling behaves as before unknown data was tolerated. */
public class StrictHandlingTest {

  private final UnknownDataHandling handling = StrictHandling.INSTANCE;
  private EntityRegistry registry;
  private AspectSpec status;

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlings.reset();
  }

  @BeforeMethod
  public void setUp() {
    status = mock(AspectSpec.class);
    EntitySpec dataset = mock(EntitySpec.class);
    when(dataset.getAspectSpec("status")).thenReturn(status);
    registry = mock(EntityRegistry.class);
    doThrow(new IllegalArgumentException("Failed to find entity"))
        .when(registry)
        .getEntitySpec(anyString());
    doReturn(dataset).when(registry).getEntitySpec("dataset");
  }

  @Test
  public void testAdmitsEverythingWithoutLookingAnythingUp() {
    EntityRegistry untouched = mock(EntityRegistry.class);
    UnknownDataGuard site = UnknownDataGuard.forSite(StrictHandlingTest.class, "test row");

    assertTrue(
        handling.admit(
            site, untouched, Optional.empty(), "entityFromNewerBuild", "aspect", "subject"));
    assertTrue(
        handling.admitUrn(
            site, untouched, Optional.empty(), "urn:li:entityFromNewerBuild:x", null));
    assertTrue(
        handling.admitEvent(
            site,
            untouched,
            Optional.empty(),
            UrnUtils.getUrn("urn:li:entityFromNewerBuild:x"),
            "entityFromNewerBuild",
            null));
    assertFalse(handling.isUnknown(untouched, "entityFromNewerBuild", null));
    assertFalse(handling.isUnknownUrn(untouched, "urn:li:entityFromNewerBuild:x", null));
    assertFalse(
        handling.referencesUnknownEntityType(
            untouched, UrnUtils.getUrn("urn:li:entityFromNewerBuild:x")));
    assertFalse(handling.isFromNewerVersion(new Status(), null, null, untouched));
    assertFalse(handling.toleratesUnknownEntityAspect());
    assertFalse(handling.toleratesUnknownEnum());
    assertFalse(handling.toleratesUnknownData());
    verifyNoInteractions(untouched);
  }

  @Test
  public void testLookupsThrowForUnknownData() {
    assertTrue(handling.entitySpecForData(registry, "dataset").isPresent());
    expectThrows(
        IllegalArgumentException.class,
        () -> handling.entitySpecForData(registry, "entityFromNewerBuild"));

    assertSame(handling.aspectSpecForData(registry, "dataset", "status").orElseThrow(), status);
    expectThrows(
        IllegalArgumentException.class,
        () -> handling.aspectSpecForData(registry, "dataset", "aspectFromNewerBuild"));
    expectThrows(
        IllegalArgumentException.class,
        () -> handling.aspectSpecForData(registry, "entityFromNewerBuild", "status"));
  }

  @Test
  public void testLeavesAspectsUntouched() {
    Status aspect = new Status().setRemoved(false);
    EntityRegistry untouched = mock(EntityRegistry.class);

    assertEquals(handling.stripUnknownReferences(aspect, untouched), 0);
    assertFalse(handling.aspectReferencesUnknownEntityType(aspect, untouched));
    assertEquals(aspect, new Status().setRemoved(false));
    verifyNoInteractions(untouched);
  }
}
