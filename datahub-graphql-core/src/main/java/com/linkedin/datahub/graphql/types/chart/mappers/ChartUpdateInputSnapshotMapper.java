package com.linkedin.datahub.graphql.types.chart.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.chart.EditableChartProperties;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.ChartUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.metadata.aspect.ChartAspect;
import com.linkedin.metadata.aspect.ChartAspectArray;
import com.linkedin.metadata.snapshot.ChartSnapshot;

import javax.annotation.Nonnull;
import java.net.URISyntaxException;
import java.util.stream.Collectors;

public class ChartUpdateInputSnapshotMapper implements InputModelMapper<ChartUpdateInput, ChartSnapshot, Urn> {

    public static final ChartUpdateInputSnapshotMapper INSTANCE = new ChartUpdateInputSnapshotMapper();

    public static ChartSnapshot map(@Nonnull final ChartUpdateInput chartUpdateInput,
                                    @Nonnull final Urn actor) {
        return INSTANCE.apply(chartUpdateInput, actor);
    }

    @Override
    public ChartSnapshot apply(@Nonnull final ChartUpdateInput chartUpdateInput,
                               @Nonnull final Urn actor) {
        final ChartSnapshot result = new ChartSnapshot();
        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());
        try {
            result.setUrn(ChartUrn.createFromString(chartUpdateInput.getUrn()));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(String.format("Failed to validate provided urn with value %s", chartUpdateInput.getUrn()));
        }

        final ChartAspectArray aspects = new ChartAspectArray();

        if (chartUpdateInput.getOwnership() != null) {
            aspects.add(ChartAspect.create(OwnershipUpdateMapper.map(chartUpdateInput.getOwnership(), actor)));
        }

        if (chartUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                    new TagAssociationArray(
                            chartUpdateInput.getGlobalTags().getTags().stream().map(
                                    element -> TagAssociationUpdateMapper.map(element)
                            ).collect(Collectors.toList())
                    )
            );
            aspects.add(ChartAspect.create(globalTags));
        }

        if (chartUpdateInput.getEditableProperties() != null) {
            final EditableChartProperties editableChartProperties = new EditableChartProperties();
            editableChartProperties.setDescription(chartUpdateInput.getEditableProperties().getDescription());
            if (!editableChartProperties.hasCreated()) {
                editableChartProperties.setCreated(auditStamp);
            }
            editableChartProperties.setLastModified(auditStamp);
            aspects.add(ChartAspect.create(editableChartProperties));
        }

        result.setAspects(aspects);

        return result;
    }

}
