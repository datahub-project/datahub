package com.linkedin.metadata.models.registry;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.test.TestEntityProfile;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlings;
import com.linkedin.mxe.SystemMetadata;
import java.util.Optional;
import java.util.function.Predicate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RegistryKnowledgeTest {

  private static final Predicate<String> NEWER_VERSION_TYPE = "entityFromNewerBuild"::equals;

  private ConfigEntityRegistry registry;

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlings.reset();
  }

  @BeforeClass
  public void setUp() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    registry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yml"));
  }

  @Test
  public void testClassify() {
    assertEquals(RegistryKnowledge.classify(registry, "dataset", "status"), RegistryFit.KNOWN);
    assertEquals(RegistryKnowledge.classify(registry, "dataset", null), RegistryFit.KNOWN);
    assertEquals(
        RegistryKnowledge.classify(registry, "dataset", "aspectFromNewerBuild"),
        RegistryFit.UNKNOWN_ASPECT);
    assertEquals(
        RegistryKnowledge.classify(registry, "entityFromNewerBuild", null),
        RegistryFit.UNKNOWN_ENTITY_TYPE);
    assertEquals(RegistryKnowledge.classify(registry, null, "status"), RegistryFit.MALFORMED);
  }

  @Test
  public void testClassifyUrnTreatsUnparseableUrnsAsMalformed() {
    assertEquals(
        RegistryKnowledge.classifyUrn(registry, "urn:li:entityFromNewerBuild:x", "status"),
        RegistryFit.UNKNOWN_ENTITY_TYPE);
    assertEquals(
        RegistryKnowledge.classifyUrn(registry, "not-a-urn", "status"), RegistryFit.MALFORMED);
    assertEquals(RegistryKnowledge.classifyUrn(registry, null, "status"), RegistryFit.MALFORMED);
    assertFalse(RegistryFit.MALFORMED.isUnknown());
  }

  @Test
  public void testReferencesUnknownEntityTypeLooksInsideUrnTypedKeyFields() {
    // datasetKey.platform is a urn field; a nested type counts when a newer version registered it.
    assertTrue(
        RegistryKnowledge.referencesUnknownEntityType(
            registry,
            UrnUtils.getUrn("urn:li:dataset:(urn:li:entityFromNewerBuild:x,name,PROD)"),
            NEWER_VERSION_TYPE));
    assertTrue(
        RegistryKnowledge.referencesUnknownEntityType(
            registry, UrnUtils.getUrn("urn:li:entityFromNewerBuild:x"), NEWER_VERSION_TYPE));
  }

  @Test
  public void testReferencesUnknownEntityTypeIgnoresUrnShapedTextInStringKeyFields() {
    // datasetKey.name and both chartKey fields are strings: urn-shaped text there is no reference.
    assertFalse(
        RegistryKnowledge.referencesUnknownEntityType(
            registry,
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataset:p,urn:li:entityFromNewerBuild:x,PROD)"),
            NEWER_VERSION_TYPE));
    assertFalse(
        RegistryKnowledge.referencesUnknownEntityType(
            registry,
            UrnUtils.getUrn("urn:li:chart:(urn:li:entityFromNewerBuild:x,c)"),
            NEWER_VERSION_TYPE));
  }

  @Test
  public void testReferencesUnknownEntityTypeLeavesNestedTypesNoNewerVersionRegistered() {
    // A nested type no newer version registered (e.g. a typo) is left to validation.
    assertFalse(
        RegistryKnowledge.referencesUnknownEntityType(
            registry,
            UrnUtils.getUrn("urn:li:dataset:(urn:li:datsaet:x,name,PROD)"),
            NEWER_VERSION_TYPE));
  }

  @Test
  public void testReferencesUnknownEntityTypeLeavesMalformedKeysToValidation() {
    // datasetKey has three fields; a two-part key doesn't map onto them.
    assertFalse(
        RegistryKnowledge.referencesUnknownEntityType(
            registry,
            UrnUtils.getUrn("urn:li:dataset:(urn:li:entityFromNewerBuild:x,name)"),
            NEWER_VERSION_TYPE));
  }

  @Test
  public void testReferencesUnknownEntityTypeWithoutAKeyAspect() {
    // A spec without a key aspect has no urn-typed key fields, so nothing nested is a reference.
    EntityRegistry noKeyRegistry = mock(EntityRegistry.class);
    when(noKeyRegistry.findEntitySpec("dataset")).thenReturn(Optional.of(mock(EntitySpec.class)));

    assertFalse(
        RegistryKnowledge.referencesUnknownEntityType(
            noKeyRegistry,
            UrnUtils.getUrn("urn:li:dataset:(urn:li:entityFromNewerBuild:x,name,PROD)"),
            NEWER_VERSION_TYPE));
  }

  @Test
  public void testEntityTypeOfLeavesMalformedUrnsToValidation() {
    assertEquals(RegistryKnowledge.entityTypeOf("urn:li:dataset:x"), "dataset");
    // An empty type or key isn't a urn of an unknown type; validation rejects it instead.
    assertNull(RegistryKnowledge.entityTypeOf("urn:li:entityFromNewerBuild:"));
    assertNull(RegistryKnowledge.entityTypeOf("urn:li::x"));
    assertNull(RegistryKnowledge.entityTypeOf("not-a-urn"));
  }

  @Test
  public void testIsWrittenByNewerSchema() {
    AspectSpec status = registry.findAspectSpec("dataset", "status").orElseThrow();
    long current = status.getSchemaVersion();

    assertTrue(
        RegistryKnowledge.isWrittenByNewerSchema(
            new SystemMetadata().setSchemaVersion(current + 1), status));
    assertFalse(
        RegistryKnowledge.isWrittenByNewerSchema(
            new SystemMetadata().setSchemaVersion(current), status));
    assertFalse(RegistryKnowledge.isWrittenByNewerSchema(new SystemMetadata(), status));
  }
}
