package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.entity.AspectDao;
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

  @Test
  public void testGetSystemMetadata() {
    // Case 1: When systemMetadata is null and ebeanAspectV2 has system metadata
    when(ebeanAspectV2.getSystemMetadata()).thenReturn(RecordUtils.toJsonString(systemMetadata));
    EbeanSystemAspect aspect =
        new EbeanSystemAspect(
            ebeanAspectV2,
            UrnUtils.getUrn(ebeanAspectV2.getUrn()),
            ebeanAspectV2.getAspect(),
            entitySpec,
            aspectSpec,
            recordTemplate,
            null, // null so that we get it from ebeanAspectV2
            auditStamp,
            null, // serializationHooks
            null, // prePatchValidationConfig
            null); // aspectDao

    // First call should parse from ebeanAspectV2's system metadata
    SystemMetadata metadata = aspect.getSystemMetadata();
    assertNotNull(metadata);
    assertEquals(metadata.getRunId(), systemMetadata.getRunId());

    // Case 2: When systemMetadata is already set (cached)
    // Call getSystemMetadata again to test caching behavior
    SystemMetadata cachedMetadata = aspect.getSystemMetadata();
    assertSame(
        cachedMetadata, metadata, "Should return the same cached object on subsequent calls");

    // Case 3: When systemMetadata is explicitly set
    SystemMetadata customMetadata = new SystemMetadata();
    customMetadata.setLastObserved(9876543210L);
    customMetadata.setRunId("custom-run-id");

    EbeanSystemAspect aspectWithCustomMetadata =
        EbeanSystemAspect.builder()
            .forUpdate(ebeanAspectV2, entityRegistry)
            .setSystemMetadata(customMetadata);

    // Should return the custom metadata
    assertEquals(aspectWithCustomMetadata.getSystemMetadata(), customMetadata);
    assertEquals(aspectWithCustomMetadata.getSystemMetadata().getRunId(), "custom-run-id");
    assertEquals(
        aspectWithCustomMetadata.getSystemMetadata().getLastObserved(), Long.valueOf(9876543210L));

    // Case 4: When ebeanAspectV2's systemMetadata is null
    when(ebeanAspectV2.getSystemMetadata()).thenReturn(null);
    EbeanSystemAspect aspectWithNullMetadata =
        EbeanSystemAspect.builder().forUpdate(ebeanAspectV2, entityRegistry);

    // Should create default system metadata
    SystemMetadata defaultMetadata = aspectWithNullMetadata.getSystemMetadata();
    assertNotNull(defaultMetadata);
    assertEquals(defaultMetadata.getRunId(), "no-run-id-provided");

    // Case 5: When ebeanAspectV2 is null
    EbeanSystemAspect aspectWithNullEbeanAspect =
        EbeanSystemAspect.builder()
            .forInsert(
                urn, STATUS_ASPECT_NAME, entitySpec, aspectSpec, recordTemplate, null, auditStamp);

    // Should create default system metadata
    SystemMetadata defaultMetadataWithNullEbeanAspect =
        aspectWithNullEbeanAspect.getSystemMetadata();
    assertNotNull(defaultMetadataWithNullEbeanAspect);
    assertEquals(defaultMetadataWithNullEbeanAspect.getRunId(), "no-run-id-provided");
  }

  @Test
  public void testPrePatchValidationDisabled() throws Exception {
    // Pre-patch validation is null/disabled - no exception should be thrown
    when(ebeanAspectV2.getMetadata()).thenReturn(generateLargeMetadata(20000000)); // 20MB
    EbeanSystemAspect aspect = EbeanSystemAspect.builder().forUpdate(ebeanAspectV2, entityRegistry);
    assertNotNull(aspect);
  }

  @Test
  public void testPrePatchValidationNotEnabledExplicitly() throws Exception {
    com.linkedin.metadata.config.AspectSizeValidationConfig config =
        new com.linkedin.metadata.config.AspectSizeValidationConfig();
    com.linkedin.metadata.config.AspectSizeValidationConfig.AspectCheckpointConfig prePatchConfig =
        new com.linkedin.metadata.config.AspectSizeValidationConfig.AspectCheckpointConfig();
    prePatchConfig.setEnabled(false);
    config.setPrePatch(prePatchConfig);

    when(ebeanAspectV2.getMetadata()).thenReturn(generateLargeMetadata(20000000)); // 20MB
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .prePatchValidationConfig(config)
            .forUpdate(ebeanAspectV2, entityRegistry);
    assertNotNull(aspect);
  }

  @Test
  public void testPrePatchValidationNullMetadata() throws Exception {
    com.linkedin.metadata.config.AspectSizeValidationConfig config =
        createPrePatchConfig(
            15728640L, com.linkedin.metadata.config.OversizedAspectRemediation.DELETE);

    when(ebeanAspectV2.getMetadata()).thenReturn(null);
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .prePatchValidationConfig(config)
            .forUpdate(ebeanAspectV2, entityRegistry);
    assertNotNull(aspect);
  }

  @Test
  public void testPrePatchValidationSmallAspectPasses() throws Exception {
    com.linkedin.metadata.config.AspectSizeValidationConfig config =
        createPrePatchConfig(
            15728640L, com.linkedin.metadata.config.OversizedAspectRemediation.DELETE);

    when(ebeanAspectV2.getMetadata()).thenReturn(generateLargeMetadata(1000)); // 1KB
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .prePatchValidationConfig(config)
            .aspectDao(mock(AspectDao.class))
            .forUpdate(ebeanAspectV2, entityRegistry);
    assertNotNull(aspect);
  }

  @Test
  public void testPrePatchValidationAtThreshold() throws Exception {
    long threshold = 15728640L;
    com.linkedin.metadata.config.AspectSizeValidationConfig config =
        createPrePatchConfig(
            threshold, com.linkedin.metadata.config.OversizedAspectRemediation.DELETE);

    when(ebeanAspectV2.getMetadata()).thenReturn(generateLargeMetadata((int) threshold));
    EbeanSystemAspect aspect =
        EbeanSystemAspect.builder()
            .prePatchValidationConfig(config)
            .aspectDao(mock(AspectDao.class))
            .forUpdate(ebeanAspectV2, entityRegistry);
    assertNotNull(aspect);
  }

  @Test(
      expectedExceptions =
          com.linkedin.metadata.entity.validation.AspectSizeExceededException.class)
  public void testPrePatchValidationOversizedWithDelete() throws Exception {
    AspectDao mockDao = mock(AspectDao.class);
    com.linkedin.metadata.config.AspectSizeValidationConfig config =
        createPrePatchConfig(
            15728640L, com.linkedin.metadata.config.OversizedAspectRemediation.DELETE);

    when(ebeanAspectV2.getMetadata()).thenReturn(generateLargeMetadata(20000000)); // 20MB

    try {
      EbeanSystemAspect.builder()
          .prePatchValidationConfig(config)
          .aspectDao(mockDao)
          .forUpdate(ebeanAspectV2, entityRegistry);
    } catch (com.linkedin.metadata.entity.validation.AspectSizeExceededException e) {
      assertEquals(
          e.getValidationPoint(),
          com.linkedin.metadata.entity.validation.ValidationPoint.PRE_DB_PATCH);
      assertEquals(e.getActualSize(), 20000000L);
      assertEquals(e.getThreshold(), 15728640L);
      assertEquals(e.getUrn(), URN_STRING);
      assertEquals(e.getAspectName(), STATUS_ASPECT_NAME);
      verify(mockDao, times(1)).deleteAspect(eq(urn), eq(STATUS_ASPECT_NAME), eq(0L));
      throw e;
    }
  }

  @Test(
      expectedExceptions =
          com.linkedin.metadata.entity.validation.AspectSizeExceededException.class)
  public void testPrePatchValidationOversizedWithIgnore() throws Exception {
    AspectDao mockDao = mock(AspectDao.class);
    com.linkedin.metadata.config.AspectSizeValidationConfig config =
        createPrePatchConfig(
            15728640L, com.linkedin.metadata.config.OversizedAspectRemediation.IGNORE);

    when(ebeanAspectV2.getMetadata()).thenReturn(generateLargeMetadata(20000000)); // 20MB

    try {
      EbeanSystemAspect.builder()
          .prePatchValidationConfig(config)
          .aspectDao(mockDao)
          .forUpdate(ebeanAspectV2, entityRegistry);
    } catch (com.linkedin.metadata.entity.validation.AspectSizeExceededException e) {
      assertEquals(
          e.getValidationPoint(),
          com.linkedin.metadata.entity.validation.ValidationPoint.PRE_DB_PATCH);
      verifyNoInteractions(mockDao);
      throw e;
    }
  }

  @Test(
      expectedExceptions =
          com.linkedin.metadata.entity.validation.AspectSizeExceededException.class)
  public void testPrePatchValidationOneByteOverThreshold() throws Exception {
    long threshold = 15728640L;
    AspectDao mockDao = mock(AspectDao.class);
    com.linkedin.metadata.config.AspectSizeValidationConfig config =
        createPrePatchConfig(
            threshold, com.linkedin.metadata.config.OversizedAspectRemediation.DELETE);

    when(ebeanAspectV2.getMetadata()).thenReturn(generateLargeMetadata((int) (threshold + 1)));

    try {
      EbeanSystemAspect.builder()
          .prePatchValidationConfig(config)
          .aspectDao(mockDao)
          .forUpdate(ebeanAspectV2, entityRegistry);
    } catch (com.linkedin.metadata.entity.validation.AspectSizeExceededException e) {
      assertEquals(e.getActualSize(), threshold + 1);
      assertEquals(e.getThreshold(), threshold);
      verify(mockDao, times(1)).deleteAspect(eq(urn), eq(STATUS_ASPECT_NAME), eq(0L));
      throw e;
    }
  }

  @Test(
      expectedExceptions =
          com.linkedin.metadata.entity.validation.AspectSizeExceededException.class)
  public void testPrePatchValidationDeleteFailureStillThrows() throws Exception {
    AspectDao mockDao = mock(AspectDao.class);
    com.linkedin.metadata.config.AspectSizeValidationConfig config =
        createPrePatchConfig(
            15728640L, com.linkedin.metadata.config.OversizedAspectRemediation.DELETE);

    when(ebeanAspectV2.getMetadata()).thenReturn(generateLargeMetadata(20000000)); // 20MB
    doThrow(new RuntimeException("Database error"))
        .when(mockDao)
        .deleteAspect(any(Urn.class), anyString(), anyLong());

    try {
      EbeanSystemAspect.builder()
          .prePatchValidationConfig(config)
          .aspectDao(mockDao)
          .forUpdate(ebeanAspectV2, entityRegistry);
    } catch (com.linkedin.metadata.entity.validation.AspectSizeExceededException e) {
      verify(mockDao, times(1)).deleteAspect(eq(urn), eq(STATUS_ASPECT_NAME), eq(0L));
      throw e;
    }
  }

  private com.linkedin.metadata.config.AspectSizeValidationConfig createPrePatchConfig(
      long maxSizeBytes, com.linkedin.metadata.config.OversizedAspectRemediation remediation) {
    com.linkedin.metadata.config.AspectSizeValidationConfig config =
        new com.linkedin.metadata.config.AspectSizeValidationConfig();
    com.linkedin.metadata.config.AspectSizeValidationConfig.AspectCheckpointConfig prePatchConfig =
        new com.linkedin.metadata.config.AspectSizeValidationConfig.AspectCheckpointConfig();
    prePatchConfig.setEnabled(true);
    prePatchConfig.setMaxSizeBytes(maxSizeBytes);
    prePatchConfig.setOversizedRemediation(remediation);
    config.setPrePatch(prePatchConfig);
    return config;
  }

  private String generateLargeMetadata(int size) {
    StringBuilder sb = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      sb.append('x');
    }
    return sb.toString();
  }
}
