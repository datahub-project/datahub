package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanSystemAspectTest {

  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();
  private final EntityRegistry entityRegistry = opContext.getEntityRegistry();
  private final EntitySpec entitySpec = entityRegistry.getEntitySpec(DATASET_ENTITY_NAME);
  private final AspectSpec aspectSpec = entitySpec.getAspectSpec(STATUS_ASPECT_NAME);
  private final SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();
  private final RecordTemplate recordTemplate = new Status().setRemoved(false);

  @Mock private EbeanAspectV2 ebeanAspectV2;

  private Urn urn;
  private AuditStamp auditStamp;
  private static final String URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)";
  private static final String CREATED_BY = "urn:li:corpuser:tester";
  private static final long CREATED_TIME = System.currentTimeMillis();

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    urn = Urn.createFromString(URN_STRING);
    auditStamp = new AuditStamp().setActor(Urn.createFromString(CREATED_BY)).setTime(CREATED_TIME);

    // Setup common mock behaviors
    when(ebeanAspectV2.getUrn()).thenReturn(URN_STRING);
    when(ebeanAspectV2.getAspect()).thenReturn(STATUS_ASPECT_NAME);
    when(ebeanAspectV2.getCreatedBy()).thenReturn(CREATED_BY);
    when(ebeanAspectV2.getCreatedOn()).thenReturn(new Timestamp(CREATED_TIME));
    when(ebeanAspectV2.toEntityAspect())
        .thenReturn(
            new EntityAspect(
                URN_STRING,
                STATUS_ASPECT_NAME,
                0,
                RecordUtils.toJsonString(recordTemplate),
                RecordUtils.toJsonString(systemMetadata),
                new Timestamp(CREATED_TIME),
                CREATED_BY,
                null));
  }

  @Test
  public void testBuilderForInsert() {
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .forInsert(
                urn,
                STATUS_ASPECT_NAME,
                entitySpec,
                aspectSpec,
                recordTemplate,
                systemMetadata,
                auditStamp);

    assertNotNull(aspect);
    assertEquals(aspect.getUrn(), urn);
    assertEquals(aspect.getAspectName(), STATUS_ASPECT_NAME);
    assertEquals(aspect.getEntitySpec(), entitySpec);
    assertEquals(aspect.getAspectSpec(), aspectSpec);
    assertEquals(aspect.getRecordTemplate(), recordTemplate);
    assertEquals(aspect.getSystemMetadata(), systemMetadata);
    assertEquals(aspect.getVersion(), 0);
  }

  @Test
  public void testBuilderForUpdate() {
    EbeanSystemAspect aspect = EbeanSystemAspect.builder().forUpdate(ebeanAspectV2, entityRegistry);

    assertNotNull(aspect);
    assertEquals(aspect.getUrn().toString(), URN_STRING);
    assertEquals(aspect.getAspectName(), STATUS_ASPECT_NAME);
    assertNotNull(aspect.getEntitySpec());
    assertNotNull(aspect.getAspectSpec());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testWithVersionWithoutSystemMetadata() {
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .forInsert(
                urn, STATUS_ASPECT_NAME, entitySpec, aspectSpec, recordTemplate, null, auditStamp);

    aspect.withVersion(0);
  }

  @Test
  public void testWithVersion() {
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .forInsert(
                urn,
                STATUS_ASPECT_NAME,
                entitySpec,
                aspectSpec,
                recordTemplate,
                systemMetadata,
                auditStamp);

    assertNotNull(aspect.withVersion(0));
    assertNotNull(aspect.withVersion(1));
  }

  @Test
  public void testGetDatabaseAspect() {
    // Test with null ebeanAspectV2
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .forInsert(
                urn,
                STATUS_ASPECT_NAME,
                entitySpec,
                aspectSpec,
                recordTemplate,
                systemMetadata,
                auditStamp);
    Optional<SystemAspect> result = aspect.getDatabaseAspect();
    assertFalse(result.isPresent());

    // Test with non-null ebeanAspectV2
    aspect = EbeanSystemAspect.builder().forUpdate(ebeanAspectV2, entityRegistry);
    result = aspect.getDatabaseAspect();
    assertTrue(result.isPresent());
  }

  @Test
  public void testCopy() {
    EbeanSystemAspect original =
        EbeanSystemAspect.builder()
            .forInsert(
                urn,
                STATUS_ASPECT_NAME,
                entitySpec,
                aspectSpec,
                recordTemplate,
                systemMetadata,
                auditStamp);

    EbeanSystemAspect copied = (EbeanSystemAspect) original.copy();

    assertNotNull(copied);
    assertEquals(copied.getUrn(), original.getUrn());
    assertEquals(copied.getAspectName(), original.getAspectName());
    assertEquals(copied.getEntitySpec(), original.getEntitySpec());
    assertEquals(copied.getAspectSpec(), original.getAspectSpec());
    assertNotSame(copied, original);
  }

  @Test
  public void testNullAuditStampHandling() {
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .forInsert(
                urn,
                STATUS_ASPECT_NAME,
                entitySpec,
                aspectSpec,
                recordTemplate,
                systemMetadata,
                null);

    assertNull(aspect.getCreatedOn());
    assertNull(aspect.getCreatedBy());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullUrnInForInsert() {
    EbeanSystemAspect.builder()
        .forInsert(
            null,
            STATUS_ASPECT_NAME,
            entitySpec,
            aspectSpec,
            recordTemplate,
            systemMetadata,
            auditStamp);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullAspectNameInForInsert() {
    EbeanSystemAspect.builder()
        .forInsert(urn, null, entitySpec, aspectSpec, recordTemplate, systemMetadata, auditStamp);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullEntitySpecInForInsert() {
    EbeanSystemAspect.builder()
        .forInsert(
            urn, STATUS_ASPECT_NAME, null, aspectSpec, recordTemplate, systemMetadata, auditStamp);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullAspectSpecInForInsert() {
    EbeanSystemAspect.builder()
        .forInsert(
            urn, STATUS_ASPECT_NAME, entitySpec, null, recordTemplate, systemMetadata, auditStamp);
  }

  @Test
  public void testWithVersionNullRecordTemplate() {
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .forInsert(
                urn, STATUS_ASPECT_NAME, entitySpec, aspectSpec, null, systemMetadata, auditStamp);

    assertThrows(NullPointerException.class, () -> aspect.withVersion(0));
  }
}
