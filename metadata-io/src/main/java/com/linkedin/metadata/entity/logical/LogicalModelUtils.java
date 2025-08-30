package com.linkedin.metadata.entity.logical;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import javax.annotation.Nullable;

import static com.linkedin.data.template.SetMode.REMOVE_IF_NULL;
import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;


public class LogicalModelUtils {
  public static MetadataChangeProposal createLogicalParentProposal(
      Urn entityUrn, @Nullable Urn parentUrn, OperationContext context) throws URISyntaxException {
    LogicalParent logicalParent = createLogicalParent(parentUrn, context);
    return new MetadataChangeProposal()
        .setEntityUrn(entityUrn)
        .setAspectName(LOGICAL_PARENT_ASPECT_NAME)
        .setAspect(GenericRecordUtils.serializeAspect(logicalParent));
  }

  public static LogicalParent createLogicalParent(
      @Nullable Urn parentUrn, OperationContext context) {
    LogicalParent logicalParent = new LogicalParent();
    if (parentUrn == null) {
      return new LogicalParent().setParent(null, REMOVE_IF_NULL);
    }

    Urn actor = context.getActorContext().getActorUrn();
    long now = System.currentTimeMillis();
    Edge edge =
        new Edge()
            .setDestinationUrn(parentUrn)
            .setCreated(new AuditStamp().setTime(now).setActor(actor))
            .setLastModified(new AuditStamp().setTime(now).setActor(actor));

    return new LogicalParent().setParent(edge);
  }
}
