package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.retention.RetentionPolicyCache;
import com.linkedin.metadata.key.DataHubRetentionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.VersionBasedRetention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetentionServiceCacheTest {

  private OperationContext mockOpContext;
  private EntityService<ChangeMCP> mockEntityService;
  private RetentionService<ChangeMCP> retentionService;
  private RecordingCache policyCache;

  @BeforeMethod
  public void setup() {
    mockOpContext = mock(OperationContext.class);
    mockEntityService = mock(EntityService.class);
    policyCache = new RecordingCache();
    retentionService =
        new RetentionService<ChangeMCP>() {
          @Override
          protected EntityService<ChangeMCP> getEntityService() {
            return mockEntityService;
          }

          @Override
          protected AspectsBatch buildAspectsBatch(
              @Nonnull OperationContext opContext,
              List<MetadataChangeProposal> mcps,
              @Nonnull AuditStamp auditStamp) {
            return mock(AspectsBatch.class);
          }

          @Override
          protected void applyRetention(
              @Nonnull OperationContext opContext, List<RetentionContext> retentionContexts) {}

          @Override
          public void batchApplyRetention(
              @Nonnull OperationContext opContext, String entityName, String aspectName) {}

          @Override
          public BulkApplyRetentionResult batchApplyRetentionEntities(
              @Nonnull BulkApplyRetentionArgs args) {
            return new BulkApplyRetentionResult();
          }
        };
    retentionService.setPolicyCache(policyCache);
  }

  @Test
  public void testGetRetention_CachesResolvedPolicy() {
    stubLatestAspects(policyWithMaxVersions(20));

    Retention first = retentionService.getRetention(mockOpContext, "dataset", "schemaMetadata");
    Retention second = retentionService.getRetention(mockOpContext, "dataset", "schemaMetadata");

    assertEquals(first.getVersion().getMaxVersions(), 20);
    assertEquals(second.getVersion().getMaxVersions(), 20);
    verify(mockEntityService, times(1)).getLatestAspects(any(), any(), any());
    assertEquals(policyCache.puts.get(), 1);
  }

  @Test
  public void testGetRetention_SeparateKeysAreCachedIndependently() {
    stubLatestAspects(policyWithMaxVersions(20));

    retentionService.getRetention(mockOpContext, "dataset", "schemaMetadata");
    retentionService.getRetention(mockOpContext, "corpuser", "status");
    retentionService.getRetention(mockOpContext, "dataset", "schemaMetadata");

    verify(mockEntityService, times(2)).getLatestAspects(any(), any(), any());
  }

  @Test
  public void testSetRetention_InvalidatesCache() {
    stubLatestAspects(policyWithMaxVersions(20));
    when(mockEntityService.ingestProposal(any(), any(), anyBoolean())).thenReturn(List.of());

    retentionService.getRetention(mockOpContext, "dataset", "schemaMetadata");
    retentionService.setRetention(
        mockOpContext,
        null,
        null,
        new DataHubRetentionConfig().setRetention(policyWithMaxVersions(5)));

    assertEquals(policyCache.invalidates.get(), 1);
    assertTrue(policyCache.store.isEmpty());
  }

  @Test
  public void testDeleteRetention_InvalidatesCache() {
    stubLatestAspects(policyWithMaxVersions(20));

    retentionService.getRetention(mockOpContext, "dataset", "schemaMetadata");
    retentionService.deleteRetention(mockOpContext, "*", "*");

    assertEquals(policyCache.invalidates.get(), 1);
    assertTrue(policyCache.store.isEmpty());
  }

  private void stubLatestAspects(Retention retention) {
    Urn defaultUrn =
        EntityKeyUtils.convertEntityKeyToUrn(
            new DataHubRetentionKey().setEntityName("*").setAspectName("*"),
            Constants.DATAHUB_RETENTION_ENTITY);
    Map<Urn, List<RecordTemplate>> fetched =
        Map.of(defaultUrn, List.of(new DataHubRetentionConfig().setRetention(retention)));
    when(mockEntityService.getLatestAspects(any(), any(), any())).thenReturn(fetched);
  }

  private static Retention policyWithMaxVersions(int maxVersions) {
    return new Retention().setVersion(new VersionBasedRetention().setMaxVersions(maxVersions));
  }

  private static final class RecordingCache implements RetentionPolicyCache {
    private final Map<String, Retention> store = new ConcurrentHashMap<>();
    private final AtomicInteger puts = new AtomicInteger();
    private final AtomicInteger invalidates = new AtomicInteger();

    @Override
    public Retention get(@Nonnull String entityName, @Nonnull String aspectName) {
      return store.get(entityName + '\0' + aspectName);
    }

    @Override
    public void put(
        @Nonnull String entityName, @Nonnull String aspectName, @Nonnull Retention retention) {
      puts.incrementAndGet();
      store.put(entityName + '\0' + aspectName, retention);
    }

    @Override
    public void invalidateAll() {
      invalidates.incrementAndGet();
      store.clear();
    }
  }
}
