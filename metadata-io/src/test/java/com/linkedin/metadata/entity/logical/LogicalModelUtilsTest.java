package com.linkedin.metadata.entity.logical;

import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.logical.LogicalParent;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class LogicalModelUtilsTest {

  private static final Urn CHILD_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.child,PROD)");
  private static final Urn PARENT_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.parent,PROD)");

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void testCreateLogicalParentWithParentSetsEdge() {
    LogicalParent parent = LogicalModelUtils.createLogicalParent(PARENT_URN, opContext);

    assertNotNull(parent.getParent());
    assertEquals(parent.getParent().getDestinationUrn(), PARENT_URN);
    Urn actor = opContext.getActorContext().getActorUrn();
    assertEquals(parent.getParent().getCreated().getActor(), actor);
    assertEquals(parent.getParent().getLastModified().getActor(), actor);
  }

  @Test
  public void testCreateLogicalParentNullClearsParent() {
    LogicalParent parent = LogicalModelUtils.createLogicalParent(null, opContext);
    assertNull(parent.getParent());
  }

  @Test
  public void testCreateLogicalParentProposal() {
    MetadataChangeProposal mcp =
        LogicalModelUtils.createLogicalParentProposal(CHILD_URN, PARENT_URN, opContext);

    assertEquals(mcp.getEntityUrn(), CHILD_URN);
    assertEquals(mcp.getEntityType(), CHILD_URN.getEntityType());
    assertEquals(mcp.getChangeType(), ChangeType.UPSERT);
    assertEquals(mcp.getAspectName(), LOGICAL_PARENT_ASPECT_NAME);
    assertNotNull(mcp.getAspect());
  }
}
