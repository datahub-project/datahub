package com.linkedin.metadata.builders.search;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.datajob.DataJobInfo;
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
    return ("/" + urn.getFlowEntity().getFlowIdEntity() + "/"  + urn.getJobIdEntity()).toLowerCase();
  }


  @Nonnull
  private static DataJobDocument setUrnDerivedFields(@Nonnull DataJobUrn urn) {
    return new DataJobDocument()
        .setUrn(urn)
        .setDataFlow(urn.getFlowEntity().getFlowIdEntity())
        .setJobId(urn.getJobIdEntity())
        .setBrowsePaths(new StringArray(Collections.singletonList(buildBrowsePath(urn))));
  }

  @Nonnull
  private DataJobDocument getDocumentToUpdateFromAspect(@Nonnull DataJobUrn urn,
      @Nonnull DataJobInfo info) {
    final DataJobDocument document = setUrnDerivedFields(urn);
    document.setName(info.getName());
    if (info.getDescription() != null) {
        document.setDescription(info.getDescription());
    }
    return document;
  }

  @Nonnull
  private DataJobDocument getDocumentToUpdateFromAspect(@Nonnull DataJobUrn urn,
      @Nonnull Ownership ownership) {
    return setUrnDerivedFields(urn)
        .setOwners(BuilderUtils.getCorpUserOwners(ownership));  // TODO: should be optional?
  }

  @Nonnull
  private List<DataJobDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull DataJobSnapshot snapshot) {
    DataJobUrn urn = snapshot.getUrn();
    return snapshot.getAspects().stream().map(aspect -> {
      if (aspect.isDataJobInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getDataJobInfo());
      } else if (aspect.isOwnership()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getOwnership());
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
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
