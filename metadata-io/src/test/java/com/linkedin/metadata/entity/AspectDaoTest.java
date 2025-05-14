package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.EbeanSystemAspect;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectDaoTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.DEFAULT,
          () -> {
            // Set up OpenTelemetry SDK for testing
            SdkTracerProvider tracerProvider = SdkTracerProvider.builder().build();
            OpenTelemetry openTelemetry =
                OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

            // Create a tracer
            Tracer tracer = openTelemetry.getTracer("test-tracer");
            return TraceContext.builder().tracer(tracer).build();
          });
  private final EntitySpec corpUserEntitySpec =
      opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME);

  @Spy private TestAspectDao aspectDao;

  @Mock private TransactionContext txContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  private SystemAspect createSystemAspect(String version) {
    SystemMetadata metadata = new SystemMetadata();
    metadata.setVersion(version, SetMode.IGNORE_NULL);
    return EbeanSystemAspect.builder()
        .forInsert(
            UrnUtils.getUrn("urn:li:corpuser:test"),
            STATUS_ASPECT_NAME,
            corpUserEntitySpec,
            corpUserEntitySpec.getAspectSpec(STATUS_ASPECT_NAME),
            new Status().setRemoved(false),
            metadata,
            AuditStampUtils.createDefaultAuditStamp());
  }

  @Test
  public void testSaveLatestAspect_InitialInsert() {
    // Setup
    SystemAspect newAspect = createSystemAspect("1");

    // Execute
    Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
        aspectDao.saveLatestAspect(opContext, txContext, null, newAspect);

    // Verify
    assertFalse(result.getFirst().isPresent(), "Should not have inserted previous version");
    assertTrue(result.getSecond().isPresent(), "Should have inserted new version");
    assertEquals(result.getSecond().get(), newAspect.withVersion(0));
  }

  @Test
  public void testSaveLatestAspect_UpdateExisting() {
    // Setup
    SystemAspect currentAspect = createSystemAspect("1");
    SystemAspect newAspect = createSystemAspect("2");
    SystemAspect dbAspect = createSystemAspect("1");
    currentAspect.setDatabaseAspect(dbAspect);

    // Execute
    Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
        aspectDao.saveLatestAspect(opContext, txContext, currentAspect, newAspect);

    // Verify
    assertTrue(result.getFirst().isPresent(), "Should have inserted previous version");
    assertTrue(result.getSecond().isPresent(), "Should have updated current version");
  }

  @Test
  public void testSaveLatestAspect_SameVersion() {
    // Setup
    SystemAspect currentAspect = createSystemAspect("1");
    SystemAspect newAspect = createSystemAspect("1");
    newAspect
        .getSystemMetadata()
        .setLastObserved(newAspect.getSystemMetadata().getLastObserved() + 1);
    SystemAspect dbAspect = createSystemAspect("1");
    currentAspect.setDatabaseAspect(dbAspect);

    // Execute
    Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
        aspectDao.saveLatestAspect(opContext, txContext, currentAspect, newAspect);

    // Verify
    assertFalse(result.getFirst().isPresent(), "Should not have inserted previous version");
    assertTrue(
        result.getSecond().isPresent(), "Should have updated current version due to lastObserved");
  }

  @Test
  public void testSaveLatestAspect_NoOp() {
    // Setup
    SystemAspect currentAspect = createSystemAspect("1");
    SystemAspect newAspect = createSystemAspect("1");
    SystemAspect dbAspect = createSystemAspect("1");
    currentAspect.setDatabaseAspect(dbAspect);

    // Execute
    Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
        aspectDao.saveLatestAspect(opContext, txContext, currentAspect, newAspect);

    // Verify
    assertFalse(result.getFirst().isPresent(), "Should not have inserted previous version");
    assertFalse(result.getSecond().isPresent(), "Should not have updated current version");
  }

  @Test
  public void testNextVersionResolution_CurrentNull() {
    // Setup
    SystemMetadata currentMetadata = new SystemMetadata();
    currentMetadata.setVersion(null, SetMode.IGNORE_NULL);
    SystemMetadata newMetadata = new SystemMetadata();
    newMetadata.setVersion("3", SetMode.IGNORE_NULL);

    SystemAspect currentAspect = createSystemAspect(null);
    currentAspect.setSystemMetadata(currentMetadata);
    SystemAspect newAspect = createSystemAspect(null);
    newAspect.setSystemMetadata(newMetadata);
    SystemAspect dbAspect = createSystemAspect(null);
    dbAspect.setSystemMetadata(currentMetadata);

    currentAspect.setDatabaseAspect(dbAspect);

    // Execute
    Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
        aspectDao.saveLatestAspect(opContext, null, currentAspect, newAspect);

    // Verify
    assertTrue(result.getFirst().isPresent(), "Should have inserted previous version");
    assertTrue(result.getSecond().isPresent(), "Should have updated current version");
    assertEquals(result.getFirst().get().getVersion(), 2);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNextVersionResolution_NewNull() {
    // Setup
    SystemAspect currentAspect = createSystemAspect("2");
    SystemAspect newAspect = createSystemAspect(null); // This should trigger the exception
    SystemAspect dbAspect = createSystemAspect("2");
    currentAspect.setDatabaseAspect(dbAspect);

    // Execute - should throw IllegalArgumentException
    aspectDao.saveLatestAspect(opContext, null, currentAspect, newAspect);
  }

  @Test
  public void testNextVersionResolution_VersionMismatch() {
    // Setup
    SystemAspect currentAspect = createSystemAspect("1");
    SystemAspect newAspect = createSystemAspect("4");
    SystemAspect dbAspect = createSystemAspect("1");
    currentAspect.setDatabaseAspect(dbAspect);

    // Execute
    Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
        aspectDao.saveLatestAspect(opContext, txContext, currentAspect, newAspect);

    // Verify
    assertTrue(result.getFirst().isPresent(), "Should have inserted previous version");
    assertEquals(result.getFirst().get().getVersion(), 3);
    assertTrue(result.getSecond().isPresent(), "Should have updated current version");
    assertEquals(result.getSecond().get().getVersion(), 0);
    assertEquals(
        RecordUtils.toRecordTemplate(
                SystemMetadata.class, result.getSecond().get().getSystemMetadata())
            .getVersion(),
        "4");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSaveLatestAspect_ThrowsOnNullNewVersion() {
    // Setup
    SystemAspect newAspect = createSystemAspect(null);

    // Execute - should throw IllegalArgumentException
    aspectDao.saveLatestAspect(opContext, null, null, newAspect);
  }

  @Test
  public void testSaveLatestAspect_NoOpTracingSet() {
    // Setup
    SystemAspect currentAspect = createSystemAspect("1");
    SystemAspect newAspect = createSystemAspect("1");
    SystemAspect dbAspect = createSystemAspect("1");
    currentAspect.setDatabaseAspect(dbAspect);

    // Execute
    Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
        aspectDao.saveLatestAspect(opContext, txContext, currentAspect, newAspect);

    // Verify
    // Should not have any changes since it's a true no-op (same version and content)
    assertFalse(result.getFirst().isPresent(), "Should not have inserted previous version");
    assertFalse(result.getSecond().isPresent(), "Should not have updated current version");

    // The input aspect should not be modified since no update occurred
    assertNull(
        newAspect.getSystemMetadata().getProperties(),
        "SystemMetadata should not be modified for no-op case with no update");
  }

  @Test
  public void testSaveLatestAspect_NoOpWithMetadataChange() {
    opContext.withSpan(
        "testSaveLatestAspect_NoOpWithMetadataChange",
        () -> {
          // Verify span context is valid
          SpanContext currentSpanContext = Span.current().getSpanContext();
          assertTrue(currentSpanContext.isValid(), "Span context should be valid");

          // Setup
          SystemAspect currentAspect = createSystemAspect("1");
          SystemAspect newAspect = createSystemAspect("1");
          SystemAspect dbAspect = createSystemAspect("1");
          currentAspect.setDatabaseAspect(dbAspect);

          // Modify system metadata but keep same content
          newAspect
              .getSystemMetadata()
              .setLastObserved(newAspect.getSystemMetadata().getLastObserved() + 1);

          // Execute
          Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
              aspectDao.saveLatestAspect(opContext, txContext, currentAspect, newAspect);

          // Verify
          assertFalse(result.getFirst().isPresent(), "Should not have inserted previous version");
          assertTrue(result.getSecond().isPresent(), "Should have updated current version");

          SystemMetadata updatedMetadata =
              RecordUtils.toRecordTemplate(
                  SystemMetadata.class, result.getSecond().get().getSystemMetadata());

          assertTrue(
              SystemMetadataUtils.isNoOp(updatedMetadata),
              "NoOp should be true for metadata-only change");
          assertTrue(
              updatedMetadata.getProperties().containsKey("telemetryTraceId"),
              "TraceId should be set");
        });
  }

  @Test
  public void testSaveLatestAspect_ContentChangeTracing() {
    opContext.withSpan(
        "testSaveLatestAspect_ContentChangeTracing",
        () -> {
          // Verify span context is valid
          SpanContext currentSpanContext = Span.current().getSpanContext();
          assertTrue(currentSpanContext.isValid(), "Span context should be valid");

          // Setup
          SystemAspect currentAspect = createSystemAspect("1");
          SystemAspect newAspect = createSystemAspect("2");
          // Modify the content to ensure it's not a no-op
          Status newStatus = new Status().setRemoved(true);
          newAspect.setRecordTemplate(newStatus);

          SystemAspect dbAspect = createSystemAspect("1");
          currentAspect.setDatabaseAspect(dbAspect);

          // Execute
          Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
              aspectDao.saveLatestAspect(opContext, txContext, currentAspect, newAspect);

          // Verify
          assertTrue(result.getFirst().isPresent(), "Should have inserted previous version");
          assertTrue(result.getSecond().isPresent(), "Should have updated current version");

          SystemMetadata updatedMetadata =
              RecordUtils.toRecordTemplate(
                  SystemMetadata.class, result.getSecond().get().getSystemMetadata());

          assertFalse(
              SystemMetadataUtils.isNoOp(updatedMetadata),
              "NoOp should be false for content change");
          assertTrue(
              updatedMetadata.getProperties().containsKey("telemetryTraceId"),
              "TraceId should be set");

          // Verify previous version's metadata is unchanged
          SystemMetadata previousMetadata =
              RecordUtils.toRecordTemplate(
                  SystemMetadata.class, result.getFirst().get().getSystemMetadata());
          assertFalse(
              SystemMetadataUtils.isNoOp(previousMetadata),
              "Previous version should not have NoOp flag");
        });
  }

  @Test
  public void testSaveLatestAspect_TraceIdPropagation() {
    opContext.withSpan(
        "testSaveLatestAspect_TraceIdPropagation",
        () -> {
          // Verify span context is valid
          SpanContext currentSpanContext = Span.current().getSpanContext();
          assertTrue(currentSpanContext.isValid(), "Span context should be valid");

          // Setup
          String existingTraceId = "existing-trace-123";
          SystemAspect currentAspect = createSystemAspect("1");
          currentAspect
              .getSystemMetadata()
              .setProperties(new StringMap(Map.of("telemetryTraceId", existingTraceId)));

          SystemAspect newAspect = createSystemAspect("2");
          // Set a different trace ID to verify overwrite behavior
          newAspect
              .getSystemMetadata()
              .setProperties(new StringMap(Map.of("telemetryTraceId", "new-trace-456")));

          SystemAspect dbAspect = createSystemAspect("1");
          dbAspect
              .getSystemMetadata()
              .setProperties(new StringMap(Map.of("telemetryTraceId", existingTraceId)));
          currentAspect.setDatabaseAspect(dbAspect);

          // Execute
          Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
              aspectDao.saveLatestAspect(opContext, txContext, currentAspect, newAspect);

          // Verify
          assertTrue(result.getSecond().isPresent(), "Should have updated current version");

          SystemMetadata updatedMetadata =
              RecordUtils.toRecordTemplate(
                  SystemMetadata.class, result.getSecond().get().getSystemMetadata());

          assertTrue(
              updatedMetadata.getProperties().containsKey("telemetryTraceId"),
              "TraceId should be set");
          assertNotEquals(
              updatedMetadata.getProperties().get("telemetryTraceId"),
              existingTraceId,
              "TraceId should be overwritten for version increment");
          assertFalse(
              updatedMetadata.getProperties().get("telemetryTraceId").contains("-trace-"),
              "TraceId should match operation context and not the test trace ids");
        });
  }

  // Concrete implementation for testing default methods
  private class TestAspectDao implements AspectDao {

    @Override
    public EntityAspect getAspect(String urn, String aspectName, long version) {
      return null;
    }

    @Override
    public EntityAspect getAspect(EntityAspectIdentifier key) {
      return null;
    }

    @Override
    public Map<EntityAspectIdentifier, EntityAspect> batchGet(
        Set<EntityAspectIdentifier> keys, boolean forUpdate) {
      return null;
    }

    @Override
    public List<EntityAspect> getAspectsInRange(
        Urn urn, Set<String> aspectNames, long startTimeMillis, long endTimeMillis) {
      return null;
    }

    @Override
    public Map<String, Map<String, SystemAspect>> getLatestAspects(
        OperationContext opContext, Map<String, Set<String>> urnAspects, boolean forUpdate) {
      return null;
    }

    @Nonnull
    @Override
    public Optional<EntityAspect> updateAspect(TransactionContext txContext, SystemAspect aspect) {
      return Optional.of(aspect.withVersion(0));
    }

    @Nonnull
    @Override
    public Optional<EntityAspect> insertAspect(
        TransactionContext txContext, SystemAspect aspect, long version) {
      return Optional.of(aspect.withVersion(version));
    }

    // Implementing remaining interface methods
    @Override
    public void deleteAspect(Urn urn, String aspect, Long version) {}

    @Override
    public ListResult<String> listUrns(
        String entityName, String aspectName, int start, int pageSize) {
      return null;
    }

    @Override
    public Integer countAspect(String aspectName, String urnLike) {
      return null;
    }

    @Override
    public PartitionedStream<EbeanAspectV2> streamAspectBatches(RestoreIndicesArgs args) {
      return null;
    }

    @Override
    public Stream<EntityAspect> streamAspects(String entityName, String aspectName) {
      return null;
    }

    @Override
    public int deleteUrn(TransactionContext txContext, String urn) {
      return 0;
    }

    @Override
    public ListResult<String> listLatestAspectMetadata(
        String entityName, String aspectName, int start, int pageSize) {
      return null;
    }

    @Override
    public ListResult<String> listAspectMetadata(
        String entityName, String aspectName, long version, int start, int pageSize) {
      return null;
    }

    @Override
    public Map<String, Map<String, Long>> getNextVersions(Map<String, Set<String>> urnAspectMap) {
      return null;
    }

    @Override
    public long getMaxVersion(String urn, String aspectName) {
      return 0;
    }

    @Override
    public Pair<Long, Long> getVersionRange(String urn, String aspectName) {
      return null;
    }

    @Override
    public void setWritable(boolean canWrite) {}

    @Override
    public <T> Optional<T> runInTransactionWithRetry(
        Function<TransactionContext, TransactionResult<T>> block, int maxTransactionRetry) {
      return Optional.empty();
    }
  }
}
