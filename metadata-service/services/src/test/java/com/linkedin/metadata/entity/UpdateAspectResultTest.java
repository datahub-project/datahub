package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class UpdateAspectResultTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:corpuser:updateAspectResultTest");
  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final AuditStamp AUDIT_STAMP = AuditStampUtils.createDefaultAuditStamp();

  @Test
  public void testToMclOmitsDatabaseAspectRowVersionWhenUnset() {
    ChangeMCP changeMCP = buildChangeMCP(new Status().setRemoved(false));

    UpdateAspectResult result =
        UpdateAspectResult.builder()
            .urn(TEST_URN)
            .request(changeMCP)
            .newValue(new Status().setRemoved(true))
            .newSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
            .operation(MetadataAuditOperation.UPDATE)
            .auditStamp(AUDIT_STAMP)
            .maxVersion(1)
            .build();

    MetadataChangeLog mcl = result.toMCL();

    assertFalse(mcl.hasHeaders());
  }

  @Test
  public void testToMclStampsDatabaseAspectRowVersion() {
    ChangeMCP changeMCP = buildChangeMCP(new Status().setRemoved(false));
    SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();

    UpdateAspectResult result =
        UpdateAspectResult.builder()
            .urn(TEST_URN)
            .request(changeMCP)
            .newValue(new Status().setRemoved(true))
            .newSystemMetadata(systemMetadata)
            .operation(MetadataAuditOperation.UPDATE)
            .auditStamp(AUDIT_STAMP)
            .maxVersion(1)
            .databaseAspectRowVersion(42L)
            .build();

    MetadataChangeLog mcl = result.toMCL();

    assertEquals(mcl.getHeaders().get(Constants.MCL_HEADER_DATABASE_ASPECT_VERSION), "42");
    assertNull(result.getOldValue());
  }

  private static ChangeMCP buildChangeMCP(Status status) {
    return ChangeItemImpl.builder()
        .urn(TEST_URN)
        .aspectName(STATUS_ASPECT_NAME)
        .recordTemplate(status)
        .auditStamp(AUDIT_STAMP)
        .build(OP_CONTEXT.getAspectRetriever());
  }
}
