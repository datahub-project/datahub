package com.linkedin.metadata.aspect;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.datahub.test.TestEntityProfile;
import com.linkedin.common.Status;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.sql.Timestamp;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class EntityAspectTest {

  private static final String TEST_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_ASPECT = "status";
  private static final String TEST_METADATA = "{\"removes\":false}";
  private static final String TEST_SYSTEM_METADATA = "{\"lastModified\":1234567890}";
  private static final String TEST_CREATED_BY = "urn:li:corpuser:testUser";
  private static final String TEST_CREATED_FOR = "urn:li:corpuser:testImpersonator";

  private EntityRegistry entityRegistry;
  private EntityAspect testEntityAspect;
  private Timestamp testTimestamp;

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @BeforeMethod
  public void setup() {
    entityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yml"));

    testTimestamp = new Timestamp(System.currentTimeMillis());

    // Initialize test entity aspect
    testEntityAspect =
        EntityAspect.builder()
            .urn(TEST_URN)
            .aspect(TEST_ASPECT)
            .version(1L)
            .metadata(TEST_METADATA)
            .systemMetadata(TEST_SYSTEM_METADATA)
            .createdOn(testTimestamp)
            .createdBy(TEST_CREATED_BY)
            .createdFor(TEST_CREATED_FOR)
            .build();
  }

  @Test
  public void testEntityAspectBuilder() {
    assertNotNull(testEntityAspect);
    assertEquals(testEntityAspect.getUrn(), TEST_URN);
    assertEquals(testEntityAspect.getAspect(), TEST_ASPECT);
    assertEquals(testEntityAspect.getVersion(), 1L);
    assertEquals(testEntityAspect.getMetadata(), TEST_METADATA);
    assertEquals(testEntityAspect.getSystemMetadata(), TEST_SYSTEM_METADATA);
    assertEquals(testEntityAspect.getCreatedOn(), testTimestamp);
    assertEquals(testEntityAspect.getCreatedBy(), TEST_CREATED_BY);
    assertEquals(testEntityAspect.getCreatedFor(), TEST_CREATED_FOR);
  }

  @Test
  public void testEntitySystemAspectBuilder() {
    EntityAspect.EntitySystemAspect systemAspect =
        EntityAspect.EntitySystemAspect.builder().forInsert(testEntityAspect, entityRegistry);

    assertNotNull(systemAspect);
    assertEquals(systemAspect.getUrnRaw(), TEST_URN);
    assertEquals(systemAspect.getAspectName(), TEST_ASPECT);
    assertEquals(systemAspect.getVersion(), 0L);
  }

  @Test
  public void testToEnvelopedAspects() {
    EntityAspect.EntitySystemAspect systemAspect =
        EntityAspect.EntitySystemAspect.builder()
            .recordTemplate(new Status().setRemoved(false))
            .forInsert(testEntityAspect, entityRegistry);

    // Execute
    EnvelopedAspect envelopedAspect = systemAspect.toEnvelopedAspects();

    // Verify
    assertNotNull(envelopedAspect);
    assertEquals(envelopedAspect.getName(), TEST_ASPECT);
    assertEquals(envelopedAspect.getVersion(), 0L);
    assertEquals(envelopedAspect.getType(), AspectType.VERSIONED);
  }

  @Test
  public void testCopy() {
    EntityAspect.EntitySystemAspect original =
        EntityAspect.EntitySystemAspect.builder().forInsert(testEntityAspect, entityRegistry);

    EntityAspect.EntitySystemAspect copied = (EntityAspect.EntitySystemAspect) original.copy();

    assertNotNull(copied);
    assertEquals(copied.getUrnRaw(), original.getUrnRaw());
    assertEquals(copied.getAspectName(), original.getAspectName());
    assertNull(copied.getEntityAspect()); // EntityAspect should be null in copy
  }

  @Test
  public void testWithVersion() {
    EntityAspect.EntitySystemAspect systemAspect =
        EntityAspect.EntitySystemAspect.builder().forInsert(testEntityAspect, entityRegistry);

    long newVersion = 2L;
    EntityAspect updatedAspect = systemAspect.withVersion(newVersion);

    assertNotNull(updatedAspect);
    assertEquals(updatedAspect.getVersion(), newVersion);
    assertEquals(updatedAspect.getUrn(), TEST_URN);
    assertEquals(updatedAspect.getAspect(), TEST_ASPECT);
  }

  @Test
  public void testToString() {
    String expectedString =
        "EntityAspect{"
            + "urn='"
            + TEST_URN
            + '\''
            + ", aspect='"
            + TEST_ASPECT
            + '\''
            + ", version="
            + 1L
            + ", metadata='"
            + TEST_METADATA
            + '\''
            + ", systemMetadata='"
            + TEST_SYSTEM_METADATA
            + '\''
            + '}';

    assertEquals(testEntityAspect.toString(), expectedString);
  }

  @Test
  public void testEntityAspectBuilderWithNullValues() {
    EntityAspect aspect =
        EntityAspect.builder().urn(TEST_URN).aspect(TEST_ASPECT).version(1L).build();

    assertNotNull(aspect);
    assertEquals(aspect.getUrn(), TEST_URN);
    assertEquals(aspect.getAspect(), TEST_ASPECT);
    assertEquals(aspect.getVersion(), 1L);
    assertNull(aspect.getMetadata());
    assertNull(aspect.getSystemMetadata());
    assertNull(aspect.getCreatedOn());
    assertNull(aspect.getCreatedBy());
    assertNull(aspect.getCreatedFor());
  }

  @Test
  public void testEntityAspectToBuilder() {
    EntityAspect modifiedAspect =
        testEntityAspect.toBuilder().metadata("{\"newKey\":\"newValue\"}").version(2L).build();

    assertNotNull(modifiedAspect);
    assertEquals(modifiedAspect.getUrn(), TEST_URN);
    assertEquals(modifiedAspect.getAspect(), TEST_ASPECT);
    assertEquals(modifiedAspect.getVersion(), 2L);
    assertEquals(modifiedAspect.getMetadata(), "{\"newKey\":\"newValue\"}");
    assertEquals(modifiedAspect.getSystemMetadata(), TEST_SYSTEM_METADATA);
    assertEquals(modifiedAspect.getCreatedOn(), testTimestamp);
    assertEquals(modifiedAspect.getCreatedBy(), TEST_CREATED_BY);
    assertEquals(modifiedAspect.getCreatedFor(), TEST_CREATED_FOR);
  }

  @Test
  public void testEntityAspectEqualsAndHashCode() {
    EntityAspect duplicateAspect =
        EntityAspect.builder()
            .urn(TEST_URN)
            .aspect(TEST_ASPECT)
            .version(1L)
            .metadata(TEST_METADATA)
            .systemMetadata(TEST_SYSTEM_METADATA)
            .createdOn(testTimestamp)
            .createdBy(TEST_CREATED_BY)
            .createdFor(TEST_CREATED_FOR)
            .build();

    assertEquals(testEntityAspect, duplicateAspect);
    assertEquals(testEntityAspect.hashCode(), duplicateAspect.hashCode());
  }

  @Test
  public void testEntityAspectNotEquals() {
    EntityAspect differentAspect =
        EntityAspect.builder()
            .urn(TEST_URN)
            .aspect(TEST_ASPECT)
            .version(2L) // Different version
            .metadata(TEST_METADATA)
            .systemMetadata(TEST_SYSTEM_METADATA)
            .createdOn(testTimestamp)
            .createdBy(TEST_CREATED_BY)
            .createdFor(TEST_CREATED_FOR)
            .build();

    assertNotEquals(testEntityAspect, differentAspect);
  }

  @Test
  public void testEntityAspectBuilderWithEmptyStrings() {
    EntityAspect aspect =
        EntityAspect.builder()
            .urn(TEST_URN)
            .aspect(TEST_ASPECT)
            .version(1L)
            .metadata("")
            .systemMetadata("")
            .createdBy("")
            .createdFor("")
            .build();

    assertNotNull(aspect);
    assertEquals(aspect.getMetadata(), "");
    assertEquals(aspect.getSystemMetadata(), "");
    assertEquals(aspect.getCreatedBy(), "");
    assertEquals(aspect.getCreatedFor(), "");
  }
}
