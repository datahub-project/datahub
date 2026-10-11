package com.linkedin.metadata.entity.logical;

import static com.linkedin.data.template.SetMode.REMOVE_IF_NULL;
import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public final class LogicalModelUtils {

  private LogicalModelUtils() {}

  public static MetadataChangeProposal createLogicalParentProposal(
      Urn childUrn, @Nullable Urn parentUrn, OperationContext context) {
    LogicalParent logicalParent = createLogicalParent(parentUrn, context);
    return new MetadataChangeProposal()
        .setEntityUrn(childUrn)
        .setEntityType(childUrn.getEntityType())
        .setChangeType(ChangeType.UPSERT)
        .setAspectName(LOGICAL_PARENT_ASPECT_NAME)
        .setAspect(GenericRecordUtils.serializeAspect(logicalParent));
  }

  public static LogicalParent createLogicalParent(
      @Nullable Urn parentUrn, OperationContext context) {
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

  /** Validates every mapping references field paths present on both parent and child. */
  public static void validateFieldPaths(
      Set<String> parentFieldPaths, Set<String> childFieldPaths, Map<String, String> fieldPathMap) {
    for (Map.Entry<String, String> mapping : fieldPathMap.entrySet()) {
      if (!parentFieldPaths.contains(mapping.getKey())) {
        throw new IllegalArgumentException(
            String.format("Field path not found on parent: %s", mapping.getKey()));
      }
      if (!childFieldPaths.contains(mapping.getValue())) {
        throw new IllegalArgumentException(
            String.format("Field path not found on child: %s", mapping.getValue()));
      }
    }
  }

  /** Builds the dataset-level + column-level LogicalParent proposals for a link. */
  public static List<MetadataChangeProposal> buildLinkProposals(
      Urn childDatasetUrn,
      Urn parentDatasetUrn,
      Map<String, String> fieldPathMap,
      OperationContext context) {
    List<MetadataChangeProposal> proposals = new ArrayList<>();
    proposals.add(createLogicalParentProposal(childDatasetUrn, parentDatasetUrn, context));
    for (Map.Entry<String, String> mapping : fieldPathMap.entrySet()) {
      Urn parentFieldUrn =
          SchemaFieldUtils.generateSchemaFieldUrn(parentDatasetUrn, mapping.getKey());
      Urn childFieldUrn =
          SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, mapping.getValue());
      proposals.add(createLogicalParentProposal(childFieldUrn, parentFieldUrn, context));
    }
    return proposals;
  }

  /** Builds the proposals that unset the dataset-level + given column-level LogicalParents. */
  public static List<MetadataChangeProposal> buildUnlinkProposals(
      Urn childDatasetUrn, Collection<String> childFieldPaths, OperationContext context) {
    List<MetadataChangeProposal> proposals = new ArrayList<>();
    proposals.add(createLogicalParentProposal(childDatasetUrn, null, context));
    for (String fieldPath : childFieldPaths) {
      Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, fieldPath);
      proposals.add(createLogicalParentProposal(childFieldUrn, null, context));
    }
    return proposals;
  }

  /**
   * Builds proposals that surgically unset only the given column-level LogicalParents on a child,
   * optionally also unsetting the dataset-level LogicalParent. Used when a breaking parent-schema
   * change affects only some of a child's mapped columns: the unaffected column edges (and their
   * propagated metadata) are left intact, and the dataset-level link is cleared only when the child
   * retains no surviving column mappings.
   */
  public static List<MetadataChangeProposal> buildPartialUnlinkProposals(
      Urn childDatasetUrn,
      Collection<String> childFieldPathsToClear,
      boolean clearDatasetLevel,
      OperationContext context) {
    List<MetadataChangeProposal> proposals = new ArrayList<>();
    if (clearDatasetLevel) {
      proposals.add(createLogicalParentProposal(childDatasetUrn, null, context));
    }
    for (String fieldPath : childFieldPathsToClear) {
      Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, fieldPath);
      proposals.add(createLogicalParentProposal(childFieldUrn, null, context));
    }
    return proposals;
  }
}
