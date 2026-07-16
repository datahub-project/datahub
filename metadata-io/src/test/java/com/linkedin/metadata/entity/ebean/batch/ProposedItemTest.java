package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProposedItemTest {
  private static final String URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)";
  private static final String ASPECT_NAME = "status";
  private static final String ACTOR_URN = "urn:li:corpuser:testUser";

  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();
  private final EntityRegistry entityRegistry = opContext.getEntityRegistry();

  private final EntitySpec entitySpec = entityRegistry.getEntitySpec(DATASET_ENTITY_NAME);
  private final AspectSpec aspectSpec = entitySpec.getAspectSpec(STATUS_ASPECT_NAME);

  private Urn urn;
  private MetadataChangeProposal mcp;
  private AuditStamp auditStamp;

  @BeforeMethod
  public void setup() throws Exception {
    // Set up URN and audit stamp
    urn = Urn.createFromString(URN_STRING);
    auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(ACTOR_URN))
            .setTime(System.currentTimeMillis());

    // Create MCP
    mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setAspectName(ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
  }

  @Test
  public void testBuild() {
    ProposedItem item = ProposedItem.builder().build(mcp, auditStamp, entityRegistry);

    assertNotNull(item);
    assertEquals(item.getUrn(), urn);
    assertEquals(item.getMetadataChangeProposal(), mcp);
    assertEquals(item.getAuditStamp(), auditStamp);
    assertEquals(item.getEntitySpec(), entitySpec);
    assertEquals(item.getAspectSpec(), aspectSpec);
  }

  @Test
  public void testGetSystemMetadata() {
    // Case 1: When systemMetadata is null in MCP
    ProposedItem itemNullMetadata = ProposedItem.builder().build(mcp, auditStamp, entityRegistry);

    SystemMetadata metadata = itemNullMetadata.getSystemMetadata();
    assertNotNull(metadata);
    assertEquals(metadata.getRunId(), "no-run-id-provided");

    // Verify it was set on the MCP
    assertNotNull(mcp.getSystemMetadata());
    assertEquals(mcp.getSystemMetadata(), metadata);

    // Case 2: When systemMetadata is already set in MCP
    SystemMetadata customMetadata = new SystemMetadata();
    customMetadata.setRunId("custom-run-id");
    customMetadata.setLastObserved(1234567890L);

    MetadataChangeProposal mcpWithMetadata = new MetadataChangeProposal();
    mcpWithMetadata.setEntityUrn(urn);
    mcpWithMetadata.setEntityType(DATASET_ENTITY_NAME);
    mcpWithMetadata.setAspectName(ASPECT_NAME);
    mcpWithMetadata.setChangeType(ChangeType.UPSERT);
    mcpWithMetadata.setSystemMetadata(customMetadata);

    ProposedItem itemWithMetadata =
        ProposedItem.builder().build(mcpWithMetadata, auditStamp, entityRegistry);

    SystemMetadata retrievedMetadata = itemWithMetadata.getSystemMetadata();
    assertNotNull(retrievedMetadata);
    assertEquals(retrievedMetadata.getRunId(), "custom-run-id");
    assertEquals(retrievedMetadata.getLastObserved().longValue(), 1234567890L);
    assertEquals(retrievedMetadata, customMetadata);
  }
}
