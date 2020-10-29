package com.linkedin.metadata.builders.search;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataprocess.DataProcessInfo;
import com.linkedin.metadata.search.DataProcessDocument;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

@Slf4j
public class DataProcessIndexBuilder extends BaseIndexBuilder<DataProcessDocument> {

    public DataProcessIndexBuilder() {
        super(Collections.singletonList(DataProcessSnapshot.class), DataProcessDocument.class);
    }

    @Nonnull
    private static String buildBrowsePath(@Nonnull DataProcessUrn urn) {
        return ("/" + urn.getOriginEntity() + "/"  + urn.getOrchestrator() + "/" + urn.getNameEntity())
            .replace('.', '/').toLowerCase();
    }

    @Nonnull
    private static DataProcessDocument setUrnDerivedFields(@Nonnull DataProcessUrn urn) {
        return new DataProcessDocument()
            .setName(urn.getNameEntity())
            .setOrchestrator(urn.getOrchestrator())
            .setUrn(urn)
            .setBrowsePaths(new StringArray(Collections.singletonList(buildBrowsePath(urn))));
    }

    @Nonnull
    private DataProcessDocument getDocumentToUpdateFromAspect(@Nonnull DataProcessUrn urn, @Nonnull Ownership ownership) {
        final StringArray owners = BuilderUtils.getCorpUserOwners(ownership);
        return new DataProcessDocument()
            .setUrn(urn)
            .setHasOwners(!owners.isEmpty())
            .setOwners(owners);
    }

    @Nonnull
    private DataProcessDocument getDocumentToUpdateFromAspect(@Nonnull DataProcessUrn urn,
        @Nonnull DataProcessInfo dataProcessInfo) {
        final DataProcessDocument dataProcessDocument = new DataProcessDocument().setUrn(urn);
        if (dataProcessInfo.getInputs() != null) {
            dataProcessDocument.setInputs(dataProcessInfo.getInputs())
                .setNumInputDatasets(dataProcessInfo.getInputs().size());
        }
        if (dataProcessInfo.getOutputs() != null) {
            dataProcessDocument.setOutputs(dataProcessInfo.getOutputs())
                .setNumOutputDatasets(dataProcessInfo.getOutputs().size());
        }
        return dataProcessDocument;
    }

    @Nonnull
    private List<DataProcessDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull DataProcessSnapshot dataProcessSnapshot) {
        final DataProcessUrn urn = dataProcessSnapshot.getUrn();
        final List<DataProcessDocument> documents = dataProcessSnapshot.getAspects().stream().map(aspect -> {
            if (aspect.isDataProcessInfo()) {
                return getDocumentToUpdateFromAspect(urn, aspect.getDataProcessInfo());
            } else if (aspect.isOwnership()) {
                return getDocumentToUpdateFromAspect(urn, aspect.getOwnership());
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
        documents.add(setUrnDerivedFields(urn));
        return documents;
    }

    @Nullable
    @Override
    public List<DataProcessDocument> getDocumentsToUpdate(@Nonnull RecordTemplate genericSnapshot) {
        if (genericSnapshot instanceof DataProcessSnapshot) {
            return getDocumentsToUpdateFromSnapshotType((DataProcessSnapshot) genericSnapshot);
        }
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Class<DataProcessDocument> getDocumentType() {
        return DataProcessDocument.class;
    }
}
