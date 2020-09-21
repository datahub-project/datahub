package com.linkedin.metadata.builders.search;

import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.search.DashboardDocument;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DashboardIndexBuilder extends BaseIndexBuilder<DashboardDocument> {
  public DashboardIndexBuilder() {
    super(Collections.singletonList(DashboardSnapshot.class), DashboardDocument.class);
  }

  @Nonnull
  private static DashboardDocument setUrnDerivedFields(@Nonnull DashboardUrn urn) {
    return new DashboardDocument()
        .setUrn(urn)
        .setTool(urn.getDashboardToolEntity());
  }

  @Nonnull
  private DashboardDocument getDocumentToUpdateFromAspect(@Nonnull DashboardUrn urn,
      @Nonnull DashboardInfo info) {
    final DashboardDocument document = setUrnDerivedFields(urn);
    document.setTitle(info.getTitle());
    document.setDescription(info.getDescription());
    if (info.getAccess() != null) {
      document.setAccess(info.getAccess());
    }
    return document;
  }

  @Nonnull
  private DashboardDocument getDocumentToUpdateFromAspect(@Nonnull DashboardUrn urn,
      @Nonnull Ownership ownership) {
    return setUrnDerivedFields(urn)
        .setOwners(BuilderUtils.getCorpUserOwners(ownership));
  }

  @Nonnull
  private DashboardDocument getDocumentToUpdateFromAspect(@Nonnull DashboardUrn urn,
      @Nonnull Status status) {
    return setUrnDerivedFields(urn)
        .setRemoved(status.isRemoved());
  }

  @Nonnull
  private List<DashboardDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull DashboardSnapshot snapshot) {
    DashboardUrn urn = snapshot.getUrn();
    return snapshot.getAspects().stream().map(aspect -> {
      if (aspect.isDashboardInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getDashboardInfo());
      } else if (aspect.isOwnership()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getOwnership());
      } else if (aspect.isStatus()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getStatus());
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Nonnull
  @Override
  public List<DashboardDocument> getDocumentsToUpdate(@Nonnull RecordTemplate snapshot) {
    if (snapshot instanceof DashboardSnapshot) {
      return getDocumentsToUpdateFromSnapshotType((DashboardSnapshot) snapshot);
    }
    return Collections.emptyList();
  }

  @Nonnull
  @Override
  public Class<DashboardDocument> getDocumentType() {
    return DashboardDocument.class;
  }
}
