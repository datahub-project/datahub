package com.linkedin.metadata.builders.search;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.search.MLModelDocument;
import com.linkedin.metadata.snapshot.MLModelSnapshot;
import com.linkedin.ml.metadata.BaseData;
import com.linkedin.ml.metadata.EvaluationData;
import com.linkedin.ml.metadata.MLModelProperties;
import com.linkedin.ml.metadata.TrainingData;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MLModelIndexBuilder extends BaseIndexBuilder<MLModelDocument> {

    public MLModelIndexBuilder() {
        super(Collections.singletonList(MLModelSnapshot.class), MLModelDocument.class);
    }

    @Nonnull
    private static String buildBrowsePath(@Nonnull MLModelUrn urn) {
        return ("/" + urn.getOriginEntity() + "/"  + urn.getPlatformEntity().getPlatformNameEntity() + "/" + urn.getMlModelNameEntity())
            .replace('.', '/').toLowerCase();
    }

    /**
     * Given model urn, this returns a {@link MLModelDocument} model that has urn, dataset name, platform and origin fields set
     *
     * @param urn {@link MLModelUrn} that needs to be set
     * @return {@link MLModelDocument} model with relevant fields set that are extracted from the urn
     */
    @Nonnull
    private static MLModelDocument setUrnDerivedFields(@Nonnull MLModelUrn urn) {
        return new MLModelDocument()
            .setName(urn.getMlModelNameEntity())
            .setOrigin(urn.getOriginEntity())
            .setPlatform(urn.getPlatformEntity().getPlatformNameEntity())
            .setUrn(urn);
    }

    @Nonnull
    private List<MLModelDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull MLModelSnapshot mlModelSnapshot) {
        final MLModelUrn urn = mlModelSnapshot.getUrn();
        return mlModelSnapshot.getAspects().stream().map(aspect -> {
            if (aspect.isDeprecation()) {
                return getDocumentToUpdateFromAspect(urn, aspect.getDeprecation());
            } else if (aspect.isEvaluationData()) {
                return getDocumentToUpdateFromAspect(urn, aspect.getEvaluationData());
            } else if (aspect.isMLModelProperties()) {
                return getDocumentToUpdateFromAspect(urn, aspect.getMLModelProperties());
            } else if (aspect.isOwnership()) {
                return getDocumentToUpdateFromAspect(urn, aspect.getOwnership());
            } else if (aspect.isStatus()) {
                return getDocumentToUpdateFromAspect(urn, aspect.getStatus());
            } else if (aspect.isTrainingData()) {
                return getDocumentToUpdateFromAspect(urn, aspect.getTrainingData());
            }
            return setUrnDerivedFields(urn);
        }).collect(Collectors.toList());
    }

    @Nonnull
    private MLModelDocument getDocumentToUpdateFromAspect(MLModelUrn urn, Deprecation deprecation) {
        return setUrnDerivedFields(urn)
            .setActive(!deprecation.isDeprecated());
    }

    @Nonnull
    private MLModelDocument getDocumentToUpdateFromAspect(MLModelUrn urn, EvaluationData evaluationData) {
        final MLModelDocument doc = setUrnDerivedFields(urn);

        if (evaluationData.hasEvaluationData()) {
            final DatasetUrnArray datasetUrns = evaluationData.getEvaluationData()
                .stream()
                .map(BaseData::getDataset)
                .collect(Collectors.toCollection(DatasetUrnArray::new));
            doc.setEvaluationDatasets(datasetUrns);
        }
        return doc;
    }

    @Nonnull
    private MLModelDocument getDocumentToUpdateFromAspect(@Nonnull MLModelUrn urn, @Nonnull MLModelProperties mlModelProperties) {
        final MLModelDocument doc = setUrnDerivedFields(urn);

        if (mlModelProperties.hasDate()) {
            doc.setCreatedTimestamp(mlModelProperties.getDate());
        }

        if (mlModelProperties.hasDescription()) {
            doc.setDescription(mlModelProperties.getDescription());
        }

        return doc;
    }

    @Nonnull
    private MLModelDocument getDocumentToUpdateFromAspect(@Nonnull MLModelUrn urn, @Nonnull Ownership ownership) {
        final StringArray owners = BuilderUtils.getCorpUserOwners(ownership);
        return setUrnDerivedFields(urn)
            .setHasOwners(!owners.isEmpty())
            .setOwners(owners);
    }

    @Nonnull
    private MLModelDocument getDocumentToUpdateFromAspect(@Nonnull MLModelUrn urn, @Nonnull Status status) {
        return setUrnDerivedFields(urn)
            .setRemoved(status.isRemoved());
    }

    @Nonnull
    private MLModelDocument getDocumentToUpdateFromAspect(@Nonnull MLModelUrn urn, @Nonnull TrainingData trainingData) {
        final MLModelDocument doc = setUrnDerivedFields(urn);

        if (trainingData.hasTrainingData()) {
            final DatasetUrnArray datasetUrns = trainingData.getTrainingData()
                .stream()
                .map(BaseData::getDataset)
                .collect(Collectors.toCollection(DatasetUrnArray::new));
            doc.setTrainingDatasets(datasetUrns);
        }

        return doc;
    }

    @Override
    @Nonnull
    public final List<MLModelDocument> getDocumentsToUpdate(@Nonnull RecordTemplate genericSnapshot) {
        if (genericSnapshot instanceof MLModelSnapshot) {
            return getDocumentsToUpdateFromSnapshotType((MLModelSnapshot) genericSnapshot);
        }
        return Collections.emptyList();
    }

    @Override
    public Class<MLModelDocument> getDocumentType() {
        return MLModelDocument.class;
    }
}
