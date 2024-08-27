package com.linkedin.datahub.graphql.types.notebook.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.EditableDashboardProperties;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.NotebookUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class NotebookUpdateInputMapper
    implements InputModelMapper<NotebookUpdateInput, Collection<MetadataChangeProposal>, Urn> {

  public static final NotebookUpdateInputMapper INSTANCE = new NotebookUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nullable final QueryContext context,
      @Nonnull final NotebookUpdateInput notebookUpdateInput,
      @Nonnull final Urn actor) {
    return INSTANCE.apply(context, notebookUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(
      @Nullable final QueryContext context, NotebookUpdateInput input, Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(3);
    final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(NOTEBOOK_ENTITY_NAME);
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    if (input.getOwnership() != null) {
      proposals.add(
          updateMappingHelper.aspectToProposal(
              OwnershipUpdateMapper.map(context, input.getOwnership(), actor),
              OWNERSHIP_ASPECT_NAME));
    }

    if (input.getTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      globalTags.setTags(
          new TagAssociationArray(
              input.getTags().getTags().stream()
                  .map(t -> TagAssociationUpdateMapper.map(context, t))
                  .collect(Collectors.toList())));
      proposals.add(updateMappingHelper.aspectToProposal(globalTags, GLOBAL_TAGS_ASPECT_NAME));
    }

    if (input.getEditableProperties() != null) {
      final EditableDashboardProperties editableDashboardProperties =
          new EditableDashboardProperties();
      editableDashboardProperties.setDescription(input.getEditableProperties().getDescription());
      if (!editableDashboardProperties.hasCreated()) {
        editableDashboardProperties.setCreated(auditStamp);
      }
      editableDashboardProperties.setLastModified(auditStamp);
      proposals.add(
          updateMappingHelper.aspectToProposal(
              editableDashboardProperties, EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME));
    }

    return proposals;
  }
}
