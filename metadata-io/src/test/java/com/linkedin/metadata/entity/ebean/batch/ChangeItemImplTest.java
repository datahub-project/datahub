package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertFalse;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class ChangeItemImplTest {
  private static final AuditStamp TEST_AUDIT_STAMP = AspectGenerationUtils.createAuditStamp();

  @Test
  public void testBatchDuplicate() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:batchDuplicateTest");
    SystemMetadata systemMetadata = AspectGenerationUtils.createSystemMetadata();
    ChangeItemImpl item1 =
        ChangeItemImpl.builder()
            .urn(entityUrn)
            .aspectName(STATUS_ASPECT_NAME)
            .recordTemplate(new Status().setRemoved(true))
            .systemMetadata(systemMetadata.copy())
            .auditStamp(TEST_AUDIT_STAMP)
            .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));
    ChangeItemImpl item2 =
        ChangeItemImpl.builder()
            .urn(entityUrn)
            .aspectName(STATUS_ASPECT_NAME)
            .recordTemplate(new Status().setRemoved(false))
            .systemMetadata(systemMetadata.copy())
            .auditStamp(TEST_AUDIT_STAMP)
            .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));

    assertFalse(item1.isDatabaseDuplicateOf(item2));
  }
}
