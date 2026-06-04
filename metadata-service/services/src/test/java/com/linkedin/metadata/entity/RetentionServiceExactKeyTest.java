package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.VersionBasedRetention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetentionServiceExactKeyTest {

  private static final DataHubRetentionConfig CONFIG_A =
      new DataHubRetentionConfig()
          .setRetention(new Retention().setVersion(new VersionBasedRetention().setMaxVersions(20)));
  private static final DataHubRetentionConfig CONFIG_B =
      new DataHubRetentionConfig()
          .setRetention(new Retention().setVersion(new VersionBasedRetention().setMaxVersions(10)));

  private OperationContext mockOpContext;
  private EntityService<ChangeMCP> mockEntityService;
  private RetentionService<ChangeMCP> retentionService;

  @BeforeMethod
  public void setup() {
    mockOpContext = mock(OperationContext.class);
    mockEntityService = mock(EntityService.class);
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
          protected void applyRetention(List<RetentionContext> retentionContexts) {}

          @Override
          public void batchApplyRetention(String entityName, String aspectName) {}

          @Override
          public BulkApplyRetentionResult batchApplyRetentionEntities(
              @Nonnull BulkApplyRetentionArgs args) {
            return new BulkApplyRetentionResult();
          }
        };
  }

  @Test
  public void testGetRetentionConfigAtExactKeyReturnsStoredConfig() {
    Urn retentionUrn = RetentionService.toRetentionUrn("*", "*");
    when(mockEntityService.getLatestAspects(
            any(), eq(Set.of(retentionUrn)), eq(Set.of(Constants.DATAHUB_RETENTION_ASPECT))))
        .thenReturn(Map.of(retentionUrn, List.of((RecordTemplate) CONFIG_A)));

    Optional<DataHubRetentionConfig> result =
        retentionService.getRetentionConfigAtExactKey(mockOpContext, "*", "*");

    assertTrue(result.isPresent());
    assertEquals(result.get(), CONFIG_A);
  }

  @Test
  public void testRetentionConfigEquals() {
    assertTrue(retentionService.retentionConfigEquals(CONFIG_A, CONFIG_A));
    assertFalse(retentionService.retentionConfigEquals(CONFIG_A, CONFIG_B));
  }

  @Test
  public void testIsRetentionPolicySystemManagedWhenMissing() {
    Urn retentionUrn = RetentionService.toRetentionUrn("dataset", "schemaMetadata");
    when(mockEntityService.getLatestAspects(any(), eq(Set.of(retentionUrn)), any()))
        .thenReturn(Collections.emptyMap());

    assertTrue(
        retentionService.isRetentionPolicySystemManaged(
            mockOpContext, "dataset", "schemaMetadata"));
  }

  @Test
  public void testIsRetentionPolicySystemManagedWhenSystemActor() throws Exception {
    Urn retentionUrn = RetentionService.toRetentionUrn("*", "*");
    when(mockEntityService.getLatestAspects(any(), eq(Set.of(retentionUrn)), any()))
        .thenReturn(Map.of(retentionUrn, List.of((RecordTemplate) CONFIG_A)));
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setCreated(
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(1L));
    when(mockEntityService.getLatestEnvelopedAspect(
            any(), eq(Constants.DATAHUB_RETENTION_ENTITY), eq(retentionUrn), any()))
        .thenReturn(envelopedAspect);

    assertTrue(retentionService.isRetentionPolicySystemManaged(mockOpContext, "*", "*"));
  }

  @Test
  public void testIsRetentionPolicySystemManagedWhenUserActor() throws Exception {
    Urn retentionUrn = RetentionService.toRetentionUrn("*", "*");
    when(mockEntityService.getLatestAspects(any(), eq(Set.of(retentionUrn)), any()))
        .thenReturn(Map.of(retentionUrn, List.of((RecordTemplate) CONFIG_A)));
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setCreated(
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:operator")).setTime(1L));
    when(mockEntityService.getLatestEnvelopedAspect(
            any(), eq(Constants.DATAHUB_RETENTION_ENTITY), eq(retentionUrn), any()))
        .thenReturn(envelopedAspect);

    assertFalse(retentionService.isRetentionPolicySystemManaged(mockOpContext, "*", "*"));
  }
}
