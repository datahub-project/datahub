package com.linkedin.datahub.graphql.types.chart.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.chart.EditableChartProperties;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.ChartUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class ChartUpdateInputMapper
    implements InputModelMapper<ChartUpdateInput, Collection<MetadataChangeProposal>, Urn> {

  public static final ChartUpdateInputMapper INSTANCE = new ChartUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nonnull final ChartUpdateInput chartUpdateInput, @Nonnull final Urn actor) {
    return INSTANCE.apply(chartUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(
      @Nonnull final ChartUpdateInput chartUpdateInput, @Nonnull final Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(3);
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());
    final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(CHART_ENTITY_NAME);

    if (chartUpdateInput.getOwnership() != null) {
      proposals.add(
          updateMappingHelper.aspectToProposal(
              OwnershipUpdateMapper.map(chartUpdateInput.getOwnership(), actor),
              OWNERSHIP_ASPECT_NAME));
    }

    if (chartUpdateInput.getTags() != null || chartUpdateInput.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      if (chartUpdateInput.getGlobalTags() != null) {
        globalTags.setTags(
            new TagAssociationArray(
                chartUpdateInput.getGlobalTags().getTags().stream()
                    .map(element -> TagAssociationUpdateMapper.map(element))
                    .collect(Collectors.toList())));
      }
      // Tags overrides global tags if provided
      if (chartUpdateInput.getTags() != null) {
        globalTags.setTags(
            new TagAssociationArray(
                chartUpdateInput.getTags().getTags().stream()
                    .map(element -> TagAssociationUpdateMapper.map(element))
                    .collect(Collectors.toList())));
      }
      proposals.add(updateMappingHelper.aspectToProposal(globalTags, GLOBAL_TAGS_ASPECT_NAME));
    }

    if (chartUpdateInput.getEditableProperties() != null) {
      final EditableChartProperties editableChartProperties = new EditableChartProperties();
      editableChartProperties.setDescription(
          chartUpdateInput.getEditableProperties().getDescription());
      if (!editableChartProperties.hasCreated()) {
        editableChartProperties.setCreated(auditStamp);
      }
      editableChartProperties.setLastModified(auditStamp);
      proposals.add(
          updateMappingHelper.aspectToProposal(
              editableChartProperties, EDITABLE_CHART_PROPERTIES_ASPECT_NAME));
    }

    return proposals;
  }
}
