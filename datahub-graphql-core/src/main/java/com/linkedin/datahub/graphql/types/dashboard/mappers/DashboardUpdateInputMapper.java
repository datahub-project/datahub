package com.linkedin.datahub.graphql.types.dashboard.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.EditableDashboardProperties;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DashboardUpdateInput;
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

public class DashboardUpdateInputMapper
    implements InputModelMapper<DashboardUpdateInput, Collection<MetadataChangeProposal>, Urn> {
  public static final DashboardUpdateInputMapper INSTANCE = new DashboardUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nullable final QueryContext context,
      @Nonnull final DashboardUpdateInput dashboardUpdateInput,
      @Nonnull final Urn actor) {
    return INSTANCE.apply(context, dashboardUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(
      @Nullable final QueryContext context,
      @Nonnull final DashboardUpdateInput dashboardUpdateInput,
      @Nonnull final Urn actor) {

    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(3);
    final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(DASHBOARD_ENTITY_NAME);
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    if (dashboardUpdateInput.getOwnership() != null) {
      proposals.add(
          updateMappingHelper.aspectToProposal(
              OwnershipUpdateMapper.map(context, dashboardUpdateInput.getOwnership(), actor),
              OWNERSHIP_ASPECT_NAME));
    }

    if (dashboardUpdateInput.getTags() != null || dashboardUpdateInput.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      if (dashboardUpdateInput.getGlobalTags() != null) {
        globalTags.setTags(
            new TagAssociationArray(
                dashboardUpdateInput.getGlobalTags().getTags().stream()
                    .map(element -> TagAssociationUpdateMapper.map(context, element))
                    .collect(Collectors.toList())));
      } else {
        // Tags override global tags
        globalTags.setTags(
            new TagAssociationArray(
                dashboardUpdateInput.getTags().getTags().stream()
                    .map(element -> TagAssociationUpdateMapper.map(context, element))
                    .collect(Collectors.toList())));
      }
      proposals.add(updateMappingHelper.aspectToProposal(globalTags, GLOBAL_TAGS_ASPECT_NAME));
    }

    if (dashboardUpdateInput.getEditableProperties() != null) {
      final EditableDashboardProperties editableDashboardProperties =
          new EditableDashboardProperties();
      editableDashboardProperties.setDescription(
          dashboardUpdateInput.getEditableProperties().getDescription());
      if (!editableDashboardProperties.hasCreated()) {
        editableDashboardProperties.setCreated(auditStamp);
      }
      editableDashboardProperties.setLastModified(auditStamp);
      proposals.add(
          updateMappingHelper.aspectToProposal(
              editableDashboardProperties, EDITABLE_DASHBOARD_PROPERTIES_ASPECT_NAME));
    }

    return proposals;
  }
}
