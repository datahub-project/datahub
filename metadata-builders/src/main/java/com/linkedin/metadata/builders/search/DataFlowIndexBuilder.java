package com.linkedin.metadata.builders.search;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.search.DataFlowDocument;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataFlowIndexBuilder extends BaseIndexBuilder<DataFlowDocument> {
  public DataFlowIndexBuilder() {
    super(Collections.singletonList(DataFlowSnapshot.class), DataFlowDocument.class);
  }

  @Nonnull
  private static String buildBrowsePath(@Nonnull DataFlowUrn urn) {
    return ("/" + urn.getOrchestratorEntity() + "/" + urn.getClusterEntity() + "/" + urn.getFlowIdEntity())
        .toLowerCase();
  }

  @Nonnull
  private static DataFlowDocument setUrnDerivedFields(@Nonnull DataFlowUrn urn) {
    return new DataFlowDocument().setUrn(urn).setOrchestrator(urn.getOrchestratorEntity())
        .setFlowId(urn.getFlowIdEntity()).setCluster(urn.getClusterEntity())
        .setBrowsePaths(new StringArray(Collections.singletonList(buildBrowsePath(urn))));
  }

  @Nonnull
  private DataFlowDocument getDocumentToUpdateFromAspect(@Nonnull DataFlowUrn urn, @Nonnull DataFlowInfo info) {
    final DataFlowDocument document = new DataFlowDocument().setUrn(urn);
    document.setName(info.getName());
    if (info.getDescription() != null) {
      document.setDescription(info.getDescription());
    }
    if (info.getProject() != null) {
      document.setProject(info.getProject());
    }
    return document;
  }

  @Nonnull
  private DataFlowDocument getDocumentToUpdateFromAspect(@Nonnull DataFlowUrn urn, @Nonnull Ownership ownership) {
    final StringArray owners = BuilderUtils.getCorpUserOwners(ownership);
    return new DataFlowDocument().setUrn(urn).setHasOwners(!owners.isEmpty()).setOwners(owners);
  }

  @Nonnull
  private DataFlowDocument getDocumentToUpdateFromAspect(@Nonnull DataFlowUrn urn,
      @Nonnull GlobalTags globalTags) {
    return new DataFlowDocument().setUrn(urn)
        .setTags(new StringArray(globalTags.getTags()
            .stream()
            .map(tag -> tag.getTag().getName())
            .collect(Collectors.toList())));
  }

  @Nonnull
  private List<DataFlowDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull DataFlowSnapshot snapshot) {
    DataFlowUrn urn = snapshot.getUrn();
    final List<DataFlowDocument> documents = snapshot.getAspects().stream().map(aspect -> {
      if (aspect.isDataFlowInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getDataFlowInfo());
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
  public List<DataFlowDocument> getDocumentsToUpdate(@Nonnull RecordTemplate snapshot) {
    if (snapshot instanceof DataFlowSnapshot) {
      return getDocumentsToUpdateFromSnapshotType((DataFlowSnapshot) snapshot);
    }
    return Collections.emptyList();
  }

  @Nonnull
  @Override
  public Class<DataFlowDocument> getDocumentType() {
    return DataFlowDocument.class;
  }
}
