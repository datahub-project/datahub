package com.linkedin.datahub.graphql.types.dataset.mappers;

import javax.annotation.Nonnull;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;

import java.util.stream.Collectors;

public class DatasetUpdateInputMapper implements InputModelMapper<DatasetUpdateInput, Dataset, Urn> {

    public static final DatasetUpdateInputMapper INSTANCE = new DatasetUpdateInputMapper();

    public static Dataset map(@Nonnull final DatasetUpdateInput datasetUpdateInput,
                              @Nonnull final Urn actor) {
        return INSTANCE.apply(datasetUpdateInput, actor);
    }

    @Override
    public Dataset apply(@Nonnull final DatasetUpdateInput datasetUpdateInput,
                         @Nonnull final Urn actor) {
        final Dataset result = new Dataset();

        if (datasetUpdateInput.getOwnership() != null) {
            result.setOwnership(OwnershipUpdateMapper.map(datasetUpdateInput.getOwnership(), actor));
        }

        if (datasetUpdateInput.getDeprecation() != null) {
            final DatasetDeprecation deprecation = new DatasetDeprecation();
            deprecation.setDeprecated(datasetUpdateInput.getDeprecation().getDeprecated());
            if (datasetUpdateInput.getDeprecation().getDecommissionTime() != null) {
                deprecation.setDecommissionTime(datasetUpdateInput.getDeprecation().getDecommissionTime());
            }
            deprecation.setNote(datasetUpdateInput.getDeprecation().getNote());
            result.setDeprecation(deprecation);
        }

        if (datasetUpdateInput.getInstitutionalMemory() != null) {
            result.setInstitutionalMemory(
                    InstitutionalMemoryUpdateMapper.map(datasetUpdateInput.getInstitutionalMemory()));
        }

        if (datasetUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                    new TagAssociationArray(
                            datasetUpdateInput.getGlobalTags().getTags().stream().map(
                                    element -> TagAssociationUpdateMapper.map(element)
                            ).collect(Collectors.toList())
                    )
            );
            result.setGlobalTags(globalTags);
        }

        if (datasetUpdateInput.getEditableSchemaMetadata() != null) {
            final EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
            editableSchemaMetadata.setEditableSchemaFieldInfo(
                    new EditableSchemaFieldInfoArray(
                            datasetUpdateInput.getEditableSchemaMetadata().getEditableSchemaFieldInfo().stream().map(
                                    element -> mapSchemaFieldInfo(element)
                            ).collect(Collectors.toList())));
            result.setEditableSchemaMetadata(editableSchemaMetadata);

        }

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
