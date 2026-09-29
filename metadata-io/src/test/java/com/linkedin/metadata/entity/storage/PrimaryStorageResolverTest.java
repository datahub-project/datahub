package com.linkedin.metadata.entity.storage;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.PrimaryStorageContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.metadata.context.StorageTarget;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class PrimaryStorageResolverTest {

  @Test
  public void resolveTarget_readPreferenceWithoutReadPool_fallsBackToPrimary() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, Mockito.mock(Database.class));
    PrimaryStorageResolver resolver = new PrimaryStorageResolver(registry, ReadPreference.PRIMARY);

    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().withReadPreference(ReadPreference.READ);

    assertEquals(resolver.resolveTarget(opContext, false), StorageTarget.PRIMARY);
  }

  @Test
  public void resolveTarget_readPoolRegistered_usesReadForReads() {
    Database primary = Mockito.mock(Database.class);
    Database read = Mockito.mock(Database.class);
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(true);
    registry.register(StorageTarget.PRIMARY, primary);
    registry.register(StorageTarget.READ, read);
    PrimaryStorageResolver resolver = new PrimaryStorageResolver(registry, ReadPreference.READ);

    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().withReadPreference(ReadPreference.READ);

    assertEquals(resolver.resolveTarget(opContext, false), StorageTarget.READ);
    assertEquals(resolver.resolveEbean(opContext, false), read);
    assertEquals(resolver.resolveEbeanPrimary(), primary);
  }

  @Test
  public void resolveTarget_forUpdate_alwaysPrimary() {
    Database primary = Mockito.mock(Database.class);
    Database read = Mockito.mock(Database.class);
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(true);
    registry.register(StorageTarget.PRIMARY, primary);
    registry.register(StorageTarget.READ, read);
    PrimaryStorageResolver resolver = new PrimaryStorageResolver(registry, ReadPreference.READ);

    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().withReadPreference(ReadPreference.READ);

    assertEquals(resolver.resolveTarget(opContext, true), StorageTarget.PRIMARY);
    assertEquals(resolver.resolveEbean(opContext, true), primary);
  }

  @Test
  public void buildDefaultPrimaryStorageContext_distinctReplica_includesPreferenceInCacheKey() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(true);
    registry.register(StorageTarget.PRIMARY, Mockito.mock(Database.class));
    registry.register(StorageTarget.READ, Mockito.mock(Database.class));

    PrimaryStorageContext context =
        PrimaryStorageResolver.buildDefaultPrimaryStorageContext(registry);

    assertEquals(context.getReadPreference(), ReadPreference.READ);
    assertFalse(context.getCacheKeyComponent().isEmpty());
  }

  @Test
  public void buildDefaultPrimaryStorageContext_splitPool_omitsPreferenceFromCacheKey() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, Mockito.mock(Database.class));
    registry.register(StorageTarget.READ, Mockito.mock(Database.class));

    PrimaryStorageContext context =
        PrimaryStorageResolver.buildDefaultPrimaryStorageContext(registry);

    assertEquals(context.getReadPreference(), ReadPreference.READ);
    assertFalse(context.isIncludeReadPreferenceInEntityCacheKey());
  }

  @Test
  public void resolveTarget_nullOpContext_usesPrimary() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, Mockito.mock(Database.class));
    PrimaryStorageResolver resolver = new PrimaryStorageResolver(registry, ReadPreference.READ);

    assertEquals(resolver.resolveTarget(null, false), StorageTarget.PRIMARY);
  }

  @Test
  public void resolveTarget_nullPrimaryStorageContext_usesPrimary() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, Mockito.mock(Database.class));
    PrimaryStorageResolver resolver = new PrimaryStorageResolver(registry, ReadPreference.READ);

    OperationContext opContext = Mockito.mock(OperationContext.class);
    Mockito.when(opContext.getPrimaryStorageContext()).thenReturn(null);

    assertEquals(resolver.resolveTarget(opContext, false), StorageTarget.PRIMARY);
  }

  @Test
  public void resolveTarget_storageTargetOverride_usesRegisteredTarget() {
    Database primary = Mockito.mock(Database.class);
    Database read = Mockito.mock(Database.class);
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(true);
    registry.register(StorageTarget.PRIMARY, primary);
    registry.register(StorageTarget.READ, read);
    PrimaryStorageResolver resolver = new PrimaryStorageResolver(registry, ReadPreference.PRIMARY);

    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .primaryStorageContext(
                PrimaryStorageContext.builder()
                    .readPreference(ReadPreference.PRIMARY)
                    .storageTargetOverride(StorageTarget.READ)
                    .includeReadPreferenceInEntityCacheKey(true)
                    .build())
            .build(
                TestOperationContexts.systemContextNoValidate().getSessionAuthentication(), false);

    assertEquals(resolver.resolveTarget(opContext, false), StorageTarget.READ);
  }

  @Test
  public void resolveTarget_overrideNotRegistered_fallsBackToPrimary() {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, Mockito.mock(Database.class));
    PrimaryStorageResolver resolver = new PrimaryStorageResolver(registry, ReadPreference.READ);

    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .primaryStorageContext(
                PrimaryStorageContext.builder()
                    .readPreference(ReadPreference.READ)
                    .storageTargetOverride(StorageTarget.READ)
                    .build())
            .build(
                TestOperationContexts.systemContextNoValidate().getSessionAuthentication(), false);

    assertEquals(resolver.resolveTarget(opContext, false), StorageTarget.PRIMARY);
  }

  @Test
  public void resolveCassandra_usesReadPool() {
    CqlSession primary = Mockito.mock(CqlSession.class);
    CqlSession read = Mockito.mock(CqlSession.class);
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(true);
    registry.register(StorageTarget.PRIMARY, primary);
    registry.register(StorageTarget.READ, read);
    PrimaryStorageResolver resolver = new PrimaryStorageResolver(registry, ReadPreference.READ);

    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().withReadPreference(ReadPreference.READ);

    assertEquals(resolver.resolveCassandra(opContext, false), read);
    assertEquals(resolver.resolveCassandraPrimary(), primary);
  }

  @Test
  public void forSingleCassandraSession_registersPrimaryOnly() {
    CqlSession session = Mockito.mock(CqlSession.class);
    PrimaryStorageResolver resolver = PrimaryStorageResolver.forSingleCassandraSession(session);

    assertEquals(resolver.resolveCassandra(null, false), session);
    assertFalse(resolver.getRegistry().has(StorageTarget.READ));
  }

  @Test
  public void recordMetrics_incrementsTargetAndFallback() {
    MetricUtils metricUtils = Mockito.mock(MetricUtils.class);
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, Mockito.mock(Database.class));
    PrimaryStorageResolver resolver =
        new PrimaryStorageResolver(registry, ReadPreference.PRIMARY, metricUtils, "ebean");

    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().withReadPreference(ReadPreference.READ);
    resolver.resolveTarget(opContext, false);

    verify(metricUtils)
        .incrementMicrometer(
            eq(PrimaryStorageResolver.METRIC_TARGET_USED),
            eq(1.0),
            eq("target"),
            anyString(),
            eq("store"),
            eq("ebean"),
            eq("forUpdate"),
            eq("false"));
    verify(metricUtils)
        .incrementMicrometer(
            eq(PrimaryStorageResolver.METRIC_READ_FALLBACK), eq(1.0), eq("store"), eq("ebean"));
  }
}
