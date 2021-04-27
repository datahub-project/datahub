package com.linkedin.metadata.builders.search;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.search.DataJobDocument;
import com.linkedin.metadata.snapshot.DataJobSnapshot;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DataJobIndexBuilder extends BaseIndexBuilder<DataJobDocument> {
  public DataJobIndexBuilder() {
    super(Collections.singletonList(DataJobSnapshot.class), DataJobDocument.class);
  }

  @Nonnull
  private static String buildBrowsePath(@Nonnull DataJobUrn urn) {
    return ("/" + urn.getFlowEntity().getOrchestratorEntity() + "/" + urn.getFlowEntity().getFlowIdEntity() + "/"
        + urn.getJobIdEntity()).toLowerCase();
  }

  @Nonnull
  private static DataJobDocument setUrnDerivedFields(@Nonnull DataJobUrn urn) {
    return new DataJobDocument().setUrn(urn)
        .setDataFlow(urn.getFlowEntity().getFlowIdEntity())
        .setOrchestrator(urn.getFlowEntity().getOrchestratorEntity())
        .setJobId(urn.getJobIdEntity())
        .setBrowsePaths(new StringArray(Collections.singletonList(buildBrowsePath(urn))));
  }

  @Nonnull
  private DataJobDocument getDocumentToUpdateFromAspect(@Nonnull DataJobUrn urn, @Nonnull DataJobInfo info) {
    final DataJobDocument document = new DataJobDocument().setUrn(urn);
    document.setName(info.getName());
    if (info.getDescription() != null) {
      document.setDescription(info.getDescription());
    }
    return document;
  }

  @Nonnull
  private DataJobDocument getDocumentToUpdateFromAspect(@Nonnull DataJobUrn urn,
      @Nonnull DataJobInputOutput inputOutput) {
    final DataJobDocument document = new DataJobDocument().setUrn(urn);
    if (inputOutput.getInputDatasets() != null) {
      document.setInputs(inputOutput.getInputDatasets()).setNumInputDatasets(inputOutput.getInputDatasets().size());
    }
    if (inputOutput.getOutputDatasets() != null) {
      document.setOutputs(inputOutput.getOutputDatasets()).setNumOutputDatasets(inputOutput.getInputDatasets().size());
    }
    return document;
  }

  @Nonnull
  private DataJobDocument getDocumentToUpdateFromAspect(@Nonnull DataJobUrn urn, @Nonnull Ownership ownership) {
    final StringArray owners = BuilderUtils.getCorpUserOwners(ownership);
    return new DataJobDocument().setUrn(urn).setHasOwners(!owners.isEmpty()).setOwners(owners);
  }

  @Nonnull
  private DataJobDocument getDocumentToUpdateFromAspect(@Nonnull DataJobUrn urn,
      @Nonnull GlobalTags globalTags) {
    return new DataJobDocument().setUrn(urn)
        .setTags(new StringArray(globalTags.getTags()
            .stream()
            .map(tag -> tag.getTag().getName())
            .collect(Collectors.toList())));
  }

  @Nonnull
  private List<DataJobDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull DataJobSnapshot snapshot) {
    DataJobUrn urn = snapshot.getUrn();
    final List<DataJobDocument> documents = snapshot.getAspects().stream().map(aspect -> {
      if (aspect.isDataJobInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getDataJobInfo());
      } else if (aspect.isDataJobInputOutput()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getDataJobInputOutput());
      } else if (aspect.isOwnership()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getOwnership());
      } else if (aspect.isGlobalTags()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getGlobalTags());
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
    documents.add(setUrnDerivedFields(urn));
    return documents;
  }

  @Nonnull
  @Override
  public List<DataJobDocument> getDocumentsToUpdate(@Nonnull RecordTemplate snapshot) {
    if (snapshot instanceof DataJobSnapshot) {
      return getDocumentsToUpdateFromSnapshotType((DataJobSnapshot) snapshot);
    }
    return Collections.emptyList();
  }

  @Nonnull
  @Override
  public Class<DataJobDocument> getDocumentType() {
    return DataJobDocument.class;
  }
}
