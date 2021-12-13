package com.linkedin.datahub.graphql.types.glossary.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.GlossaryTermUpdateInput;
import com.linkedin.metadata.aspect.GlossaryTermAspect;
import com.linkedin.metadata.aspect.GlossaryTermAspectArray;
import javax.annotation.Nonnull;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.metadata.snapshot.GlossaryTermSnapshot;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;

import java.net.URISyntaxException;
import java.util.stream.Collectors;

public class GlossaryTermUpdateInputSnapshotMapper implements InputModelMapper<GlossaryTermUpdateInput, GlossaryTermSnapshot, Urn> {

    public static final GlossaryTermUpdateInputSnapshotMapper INSTANCE = new GlossaryTermUpdateInputSnapshotMapper();

    public static GlossaryTermSnapshot map(
            @Nonnull final GlossaryTermUpdateInput glossaryTermUpdateInput,
            @Nonnull final Urn actor) {
        return INSTANCE.apply(glossaryTermUpdateInput, actor);
    }

    @Override
    public GlossaryTermSnapshot apply(
            @Nonnull final GlossaryTermUpdateInput glossaryTermUpdateInput,
            @Nonnull final Urn actor) {
        final GlossaryTermSnapshot result = new GlossaryTermSnapshot();
        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());
        try {
            GlossaryTermUrn glossaryTermUrn=   GlossaryTermUrn.createFromString(glossaryTermUpdateInput.getUrn());
            result.setUrn(glossaryTermUrn);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to validate provided urn with value %s", glossaryTermUpdateInput.getUrn()));
        }
        final GlossaryTermAspectArray aspects = new GlossaryTermAspectArray();

        if (glossaryTermUpdateInput.getEditableSchemaMetadata() != null) {
            final EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
            editableSchemaMetadata.setEditableSchemaFieldInfo(
                    new EditableSchemaFieldInfoArray(
                            glossaryTermUpdateInput.getEditableSchemaMetadata().getEditableSchemaFieldInfo().stream().map(
                                    element -> mapSchemaFieldInfo(element)
                            ).collect(Collectors.toList())));
            editableSchemaMetadata.setLastModified(auditStamp);
            editableSchemaMetadata.setCreated(auditStamp);
            aspects.add(GlossaryTermAspect.create(editableSchemaMetadata));
        }
        result.setAspects(aspects);

        return result;
    }

    private EditableSchemaFieldInfo mapSchemaFieldInfo(
            final com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfoUpdate schemaFieldInfo
    ) {
        final EditableSchemaFieldInfo output = new EditableSchemaFieldInfo();

        if (schemaFieldInfo.getDescription() != null) {
            output.setDescription(schemaFieldInfo.getDescription());
        }
        output.setFieldPath(schemaFieldInfo.getFieldPath());

        if (schemaFieldInfo.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(new TagAssociationArray(schemaFieldInfo.getGlobalTags().getTags().stream().map(
                    element -> TagAssociationUpdateMapper.map(element)).collect(Collectors.toList())));
            output.setGlobalTags(globalTags);
        }
        return output;
    }
}