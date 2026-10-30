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

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.SchemaFieldKey;
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
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
  }

  @BeforeMethod
  public void setUp() {
    UnknownDataGuard.resetLogWindows();
    status = mock(AspectSpec.class);
    EntitySpec dataset = withKey(mock(EntitySpec.class), new DatasetKey().schema());
    when(dataset.getAspectSpec("status")).thenReturn(status);
    EntitySpec schemaField = withKey(mock(EntitySpec.class), new SchemaFieldKey().schema());
    when(schemaField.getAspectSpec("status")).thenReturn(status);
    registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec(anyString())).thenReturn(Optional.empty());
    when(registry.findEntitySpec("dataset")).thenReturn(Optional.of(dataset));
    when(registry.findEntitySpec("schemaField")).thenReturn(Optional.of(schemaField));
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
            UrnUtils.getUrn("urn:li:schemaField:(urn:li:entityFromNewerBuild:x,col)"),
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
    assertFalse(handling.isUnknownUrn(registry, null, null));
    assertFalse(handling.isUnknownUrn(registry, "urn:li:dataset:x", "status"));
    assertTrue(
        handling.referencesUnknownEntityType(
            registry, UrnUtils.getUrn("urn:li:schemaField:(urn:li:entityFromNewerBuild:x,col)")));
    assertTrue(handling.toleratesUnknownEntityAspect());
    assertTrue(handling.toleratesUnknownEnum());
  }

  @Test
  public void testUrnCountsAsUnknownExactlyWhenItsRowIsSkipped() {
    // Rollback counts the rows it leaves in place with isUnknownUrn and skips them with admitUrn.
    for (String urn :
        new String[] {
          "urn:li:entityFromNewerBuild:x",
          "urn:li:schemaField:(urn:li:entityFromNewerBuild:x,col)",
          "urn:li:dataset:x"
        }) {
      assertEquals(
          handling.isUnknownUrn(registry, urn, "status"),
          !handling.admitUrn(site, registry, Optional.empty(), urn, "status"),
          urn);
    }
  }

  @Test
  public void testProcessDefaultsToStrictAndCanBeSwitched() {
    // Strict until the flags are applied, the same as their default, so a process that never
    // applies them behaves as before this tolerance existed.
    UnknownDataHandlings.reset();
    assertSame(UnknownDataHandlings.handling(), StrictHandling.INSTANCE);
    UnknownDataHandling other = mock(UnknownDataHandling.class);

    UnknownDataHandlings.use(other);
    assertSame(UnknownDataHandlings.handling(), other);

    UnknownDataHandlings.reset();
    assertSame(UnknownDataHandlings.handling(), StrictHandling.INSTANCE);
  }

  @Test
  public void testWithOnlyEnumValuesToleratedUnknownEntitiesAndAspectsAreHandledStrictly() {
    // Entity types, aspects and references are their own flag: with it off, nothing is skipped,
    // stripped or kept, and the stored aspect isn't read for the merge.
    final UnknownDataHandling enumsOnly = TolerantHandling.of(false, true, entityType -> true);
    final Ownership aspect = ownership("urn:li:entityFromNewerBuild:x");

    assertTrue(
        enumsOnly.admitUrn(
            site, registry, Optional.empty(), "urn:li:entityFromNewerBuild:x", null));
    assertTrue(
        enumsOnly.admit(site, registry, Optional.empty(), "dataset", "aspectFromNewerBuild", "u"));
    assertFalse(enumsOnly.isUnknown(registry, "entityFromNewerBuild", null));
    assertEquals(enumsOnly.stripUnknownReferences(aspect, registry), 0);
    assertEquals(aspect.getOwners().size(), 1);
    assertFalse(enumsOnly.aspectReferencesUnknownEntityType(aspect, registry));
    assertFalse(enumsOnly.isFromNewerVersion(aspect, null, null, registry));
    assertEquals(
        enumsOnly.mergeStoredReferences(
            aspect,
            () -> {
              throw new AssertionError(
                  "stored aspect read with unknown entities and aspects not tolerated");
            },
            registry),
        UnknownReferenceMerger.MergeResult.NONE);
    assertFalse(enumsOnly.toleratesUnknownEntityAspect());
    assertTrue(enumsOnly.toleratesUnknownEnum());
    assertTrue(enumsOnly.toleratesUnknownData());
  }

  @Test
  public void testToleratingNeitherKindIsRejected() {
    expectThrows(
        IllegalArgumentException.class,
        () -> TolerantHandling.of(false, false, entityType -> true));
  }

  @Test
  public void testWithOnlyUnknownEntitiesAndAspectsToleratedReferencesAreHandled() {
    final UnknownDataHandling entityAspectOnly =
        TolerantHandling.of(true, false, entityType -> true);
    final Ownership aspect = ownership("urn:li:entityFromNewerBuild:x");

    assertTrue(entityAspectOnly.isFromNewerVersion(aspect, null, null, registry));
    assertEquals(entityAspectOnly.stripUnknownReferences(aspect, registry), 1);
    assertTrue(entityAspectOnly.toleratesUnknownEntityAspect());
    assertFalse(entityAspectOnly.toleratesUnknownEnum());
    assertTrue(entityAspectOnly.toleratesUnknownData());
  }

  @Test
  public void testNestedKeyReferencesFollowTheKeySchemaAndTheNewerVersionsTypes() {
    UnknownDataHandling rows = TolerantHandling.of(true, true, "entityFromNewerBuild"::equals);

    // A newer version's type in a urn-typed key field (schemaField.parent): its row is skipped.
    String newerParent = "urn:li:schemaField:(urn:li:entityFromNewerBuild:x,col)";
    assertTrue(rows.isUnknownUrn(registry, newerParent, "status"));
    assertFalse(rows.admitUrn(site, registry, Optional.empty(), newerParent, "status"));

    // Urn-shaped text in a string key field (datasetKey.name) is not a reference.
    String urnShapedName =
        "urn:li:dataset:(urn:li:dataPlatform:kafka,urn:li:entityFromNewerBuild:x,PROD)";
    assertFalse(rows.isUnknownUrn(registry, urnShapedName, "status"));
    assertTrue(rows.admitUrn(site, registry, Optional.empty(), urnShapedName, "status"));

    // A nested type no newer version registered (e.g. a typo) is left to validation.
    String typoParent = "urn:li:schemaField:(urn:li:datsaet:x,col)";
    assertFalse(rows.isUnknownUrn(registry, typoParent, "status"));
    assertTrue(rows.admitUrn(site, registry, Optional.empty(), typoParent, "status"));
  }

  private Ownership ownership(final String owner) {
    when(registry.findEntitySpec("corpuser")).thenReturn(Optional.of(mock(EntitySpec.class)));
    return new Ownership()
        .setOwners(
            new OwnerArray(
                new Owner().setOwner(UrnUtils.getUrn(owner)).setType(OwnershipType.DATAOWNER)))
        .setLastModified(
            new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn("urn:li:corpuser:alice")));
  }

  @Test
  public void testUnknownEntityTypeIsToleratedOnlyWhenANewerVersionRegisteredIt() {
    doThrow(new IllegalArgumentException("Failed to find entity"))
        .when(registry)
        .getEntitySpec("corpusr");
    UnknownDataHandling rows = TolerantHandling.of(true, true, "entityFromNewerBuild"::equals);

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

  private static EntitySpec withKey(EntitySpec entitySpec, RecordDataSchema keySchema) {
    AspectSpec key = mock(AspectSpec.class);
    when(key.getPegasusSchema()).thenReturn(keySchema);
    when(entitySpec.getKeyAspectSpec()).thenReturn(key);
    return entitySpec;
  }
}
