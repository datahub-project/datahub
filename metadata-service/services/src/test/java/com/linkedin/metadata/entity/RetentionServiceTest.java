package com.linkedin.metadata.entity;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.retention.Retention;
import com.linkedin.retention.VersionBasedRetention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for {@link RetentionService#getMaxVersionsToKeepForWrite}. */
public class RetentionServiceTest {

  private OperationContext mockOpContext;
  private RetentionService<ChangeMCP> stubRetentionService;

  @BeforeMethod
  public void setup() {
    mockOpContext = mock(OperationContext.class);
    stubRetentionService =
        new RetentionService<ChangeMCP>() {
          @Override
          protected EntityService<ChangeMCP> getEntityService() {
            return mock(EntityService.class);
          }

          @Override
          public Retention getRetention(
              @Nonnull OperationContext opContext,
              @Nonnull String entityName,
              @Nonnull String aspectName) {
            if ("dataset".equals(entityName) && "schemaMetadata".equals(aspectName)) {
              return new Retention().setVersion(new VersionBasedRetention().setMaxVersions(10));
            }
            if ("corpuser".equals(entityName) && "status".equals(aspectName)) {
              return new Retention().setVersion(new VersionBasedRetention().setMaxVersions(1));
            }
            return new Retention();
          }

          @Override
          protected AspectsBatch buildAspectsBatch(
              @Nonnull OperationContext opContext,
              List<MetadataChangeProposal> mcps,
              @Nonnull com.linkedin.common.AuditStamp auditStamp) {
            return mock(AspectsBatch.class);
          }

          @Override
          protected void applyRetention(List<RetentionContext> retentionContexts) {}

          @Override
          public void batchApplyRetention(
              @javax.annotation.Nullable String entityName,
              @javax.annotation.Nullable String aspectName) {}

          @Override
          public BulkApplyRetentionResult batchApplyRetentionEntities(
              @Nonnull com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs args) {
            return new BulkApplyRetentionResult();
          }
        };
  }

  @Test
  public void testGetMaxVersionsToKeepForWrite_WhenVersionPolicySet() {
    assertEquals(
        stubRetentionService.getMaxVersionsToKeepForWrite(
            mockOpContext, "dataset", "schemaMetadata"),
        10);
    assertEquals(
        stubRetentionService.getMaxVersionsToKeepForWrite(mockOpContext, "corpuser", "status"), 1);
  }

  @Test
  public void testGetMaxVersionsToKeepForWrite_WhenNoVersionPolicy_ReturnsOne() {
    assertEquals(
        stubRetentionService.getMaxVersionsToKeepForWrite(
            mockOpContext, "unknownEntity", "unknownAspect"),
        1);
  }
}
