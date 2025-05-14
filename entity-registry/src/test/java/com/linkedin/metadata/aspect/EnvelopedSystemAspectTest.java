package com.linkedin.metadata.aspect;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.test.TestEntityProfile;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.commons.lang3.NotImplementedException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class EnvelopedSystemAspectTest {

  private static final String TEST_URN_STR =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_ASPECT_NAME = "status";
  private static final long TEST_VERSION = 1L;
  private static final String TEST_ACTOR = "urn:li:corpuser:testUser";

  private Urn testUrn;
  private EnvelopedAspect testEnvelopedAspect;
  private EnvelopedSystemAspect testSystemAspect;
  private AuditStamp testAuditStamp;
  private Status testStatus;
  private EntityRegistry entityRegistry;

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @BeforeMethod
  public void setup() throws Exception {
    entityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yml"));

    // Setup test URN
    testUrn = Urn.createFromString(TEST_URN_STR);

    // Setup test AuditStamp
    testAuditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString(TEST_ACTOR));

    // Setup test Status
    testStatus = new Status().setRemoved(false);

    // Setup test EnvelopedAspect
    testEnvelopedAspect =
        new EnvelopedAspect()
            .setName(TEST_ASPECT_NAME)
            .setVersion(TEST_VERSION)
            .setValue(new Aspect(testStatus.data()))
            .setCreated(testAuditStamp);

    // Create test subject
    testSystemAspect =
        new EnvelopedSystemAspect(
            testUrn, testEnvelopedAspect, entityRegistry.getEntitySpec(testUrn.getEntityType()));
  }

  @Test
  public void testConstructor() {
    assertNotNull(testSystemAspect);
    assertEquals(testSystemAspect.getUrn(), testUrn);
    assertEquals(
        testSystemAspect.getEntitySpec(), entityRegistry.getEntitySpec(testUrn.getEntityType()));
    assertEquals(
        testSystemAspect.getAspectSpec(),
        entityRegistry.getEntitySpec(testUrn.getEntityType()).getAspectSpec(TEST_ASPECT_NAME));
  }

  @Test
  public void testStaticFactoryMethod() {
    SystemAspect systemAspect =
        EnvelopedSystemAspect.of(
            testUrn, testEnvelopedAspect, entityRegistry.getEntitySpec(testUrn.getEntityType()));
    assertNotNull(systemAspect);
    assertTrue(systemAspect instanceof EnvelopedSystemAspect);
  }

  @Test
  public void testGetRecordTemplate() {
    RecordTemplate template = testSystemAspect.getRecordTemplate();
    assertNotNull(template);
    assertTrue(template.data().equals(testStatus.data()));
  }

  @Test
  public void testGetVersion() {
    assertEquals(testSystemAspect.getVersion(), TEST_VERSION);
  }

  @Test
  public void testGetCreatedOn() {
    Timestamp timestamp = testSystemAspect.getCreatedOn();
    assertNotNull(timestamp);
    assertEquals(timestamp, Timestamp.from(Instant.ofEpochMilli(testAuditStamp.getTime())));
  }

  @Test
  public void testGetCreatedBy() {
    String createdBy = testSystemAspect.getCreatedBy();
    assertEquals(createdBy, TEST_ACTOR);
  }

  @Test
  public void testGetDatabaseAspect() {
    assertFalse(testSystemAspect.getDatabaseAspect().isPresent());
  }

  @Test
  public void testCopy() {
    SystemAspect copied = testSystemAspect.copy();
    assertNotNull(copied);
    assertEquals(copied.getUrn(), testSystemAspect.getUrn());
    assertEquals(copied.getVersion(), testSystemAspect.getVersion());
  }

  @Test(expectedExceptions = NotImplementedException.class)
  public void testAsLatest() {
    testSystemAspect.asLatest();
  }

  @Test(expectedExceptions = NotImplementedException.class)
  public void testWithVersion() {
    testSystemAspect.withVersion(2L);
  }

  @Test(expectedExceptions = NotImplementedException.class)
  public void testSetSystemMetadata() {
    testSystemAspect.setSystemMetadata(new SystemMetadata());
  }

  @Test(expectedExceptions = NotImplementedException.class)
  public void testSetRecordTemplate() {
    testSystemAspect.setRecordTemplate(new Status());
  }

  @Test(expectedExceptions = NotImplementedException.class)
  public void testSetAuditStamp() {
    testSystemAspect.setAuditStamp(new AuditStamp());
  }

  @Test(expectedExceptions = NotImplementedException.class)
  public void testSetDatabaseAspect() {
    testSystemAspect.setDatabaseAspect(testSystemAspect);
  }
}
