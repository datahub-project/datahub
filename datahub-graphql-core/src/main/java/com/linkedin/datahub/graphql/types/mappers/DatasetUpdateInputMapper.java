package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadataUpdate;
import com.linkedin.datahub.graphql.generated.OwnerUpdate;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;

import javax.annotation.Nonnull;
import java.net.URISyntaxException;
import java.util.stream.Collectors;

public class DatasetUpdateInputMapper implements ModelMapper<DatasetUpdateInput, Dataset> {

    public static final DatasetUpdateInputMapper INSTANCE = new DatasetUpdateInputMapper();

    public static Dataset map(@Nonnull final DatasetUpdateInput datasetUpdateInput) {
        return INSTANCE.apply(datasetUpdateInput);
    }

    @Override
    public Dataset apply(@Nonnull final DatasetUpdateInput datasetUpdateInput) {
        final Dataset result = new Dataset();

        if (datasetUpdateInput.getOwnership() != null) {
            final Ownership ownership = new Ownership();
            ownership.setOwners(new OwnerArray(datasetUpdateInput.getOwnership().getOwners()
                    .stream()
                    .map(this::mapOwner)
                    .collect(Collectors.toList())));
            result.setOwnership(ownership);
        }

        if (datasetUpdateInput.getDeprecation() != null) {
            final DatasetDeprecation deprecation = new DatasetDeprecation();
            deprecation.setDeprecated(datasetUpdateInput.getDeprecation().getDeprecated());
            deprecation.setDecommissionTime(datasetUpdateInput.getDeprecation().getDecommissionTime());
            deprecation.setNote(datasetUpdateInput.getDeprecation().getNote());
            result.setDeprecation(deprecation);
        }

        if (datasetUpdateInput.getInstitutionalMemory() != null) {
            final InstitutionalMemory institutionalMemory = new InstitutionalMemory();
            institutionalMemory.setElements(new InstitutionalMemoryMetadataArray(
                    datasetUpdateInput.getInstitutionalMemory().getElements()
                            .stream()
                            .map(element -> mapElement(element))
                            .collect(Collectors.toList())));
            result.setInstitutionalMemory(institutionalMemory);
        }

        if (datasetUpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(new TagAssociationArray(datasetUpdateInput.getGlobalTags().getTags().stream().map(element -> mapTag(element)).collect(Collectors.toList())));
            result.setGlobalTags(globalTags);
        }

        if (datasetUpdateInput.getEditableSchemaMetadata() != null) {
            final EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
            editableSchemaMetadata.setEditableSchemaFieldInfo(
                    new EditableSchemaFieldInfoArray(datasetUpdateInput.getEditableSchemaMetadata().getEditableSchemaFieldInfo().stream().map(element -> mapSchemaFieldInfo(element)).collect(Collectors.toList())));
            result.setEditableSchemaMetadata(editableSchemaMetadata);

        }

        return result;
    }

    private EditableSchemaFieldInfo mapSchemaFieldInfo(final com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfoUpdate schemaFieldInfo) {
        final EditableSchemaFieldInfo output = new EditableSchemaFieldInfo();
        output.setDescription(schemaFieldInfo.getDescription());
        output.setFieldPath(schemaFieldInfo.getFieldPath());

        final GlobalTags globalTags = new GlobalTags();
        globalTags.setTags(
                new TagAssociationArray(schemaFieldInfo.getGlobalTags().getTags().stream().map(element -> mapTag(element)).collect(Collectors.toList()))
        );
        output.setGlobalTags(globalTags);

        return output;
    }

    private TagAssociation mapTag(final com.linkedin.datahub.graphql.generated.TagAssociationUpdate tagAssociation) {
        final TagAssociation output = new TagAssociation();
        output.setTag(new TagUrn(tagAssociation.getTag().getUrn()));
        return output;
    }

    private Owner mapOwner(final OwnerUpdate update) {
        final Owner owner = new Owner();
        owner.setOwner(getCorpUserUrn(update.getOwner()));
        owner.setType(OwnershipType.valueOf(update.getType().toString()));
        owner.setSource(new OwnershipSource().setType(OwnershipSourceType.SERVICE));
        return owner;
    }

    private InstitutionalMemoryMetadata mapElement(final InstitutionalMemoryMetadataUpdate update) {
        final InstitutionalMemoryMetadata metadata = new InstitutionalMemoryMetadata();
        metadata.setDescription(update.getDescription());
        metadata.setUrl(new Url(update.getUrl()));
        metadata.setCreateStamp(new AuditStamp()
                .setActor(getCorpUserUrn(update.getAuthor()))
                .setTime(update.getCreatedAt() == null ? System.currentTimeMillis() : update.getCreatedAt())
        );
        return metadata;
    }

    private CorpuserUrn getCorpUserUrn(final String urnStr) {
        if (urnStr == null) {
            return null;
        }
        try {
            return CorpuserUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to create CorpUserUrn from string %s", urnStr), e);
        }
    }
}
