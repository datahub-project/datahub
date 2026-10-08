package com.linkedin.metadata.models.registry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.test.TestEntityProfile;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.SystemMetadata;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RegistryKnowledgeTest {

  private ConfigEntityRegistry registry;

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
  public void testReferencesUnknownEntityTypeLooksInsideKeys() {
    assertFalse(
        RegistryKnowledge.referencesUnknownEntityType(
            registry, UrnUtils.getUrn("urn:li:chart:(urn:li:dataset:x,c)")));
    assertTrue(
        RegistryKnowledge.referencesUnknownEntityType(
            registry, UrnUtils.getUrn("urn:li:chart:(urn:li:entityFromNewerBuild:x,c)")));
    assertTrue(
        RegistryKnowledge.referencesUnknownEntityType(
            registry, UrnUtils.getUrn("urn:li:entityFromNewerBuild:x")));
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
