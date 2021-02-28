package com.linkedin.metadata.builders.search;

import com.linkedin.chart.ChartInfo;
import com.linkedin.chart.ChartQuery;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.search.ChartDocument;
import com.linkedin.metadata.snapshot.ChartSnapshot;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ChartIndexBuilder extends BaseIndexBuilder<ChartDocument> {
  public ChartIndexBuilder() {
    super(Collections.singletonList(ChartSnapshot.class), ChartDocument.class);
  }

  @Nonnull
  private static String buildBrowsePath(@Nonnull ChartUrn urn) {
    return ("/" + urn.getDashboardToolEntity() + "/"  + urn.getChartIdEntity()).toLowerCase();
  }

  @Nonnull
  private static ChartDocument setUrnDerivedFields(@Nonnull ChartUrn urn) {
    return new ChartDocument()
        .setUrn(urn)
        .setTool(urn.getDashboardToolEntity())
        .setBrowsePaths(new StringArray(Collections.singletonList(buildBrowsePath(urn))));
  }

  @Nonnull
  private ChartDocument getDocumentToUpdateFromAspect(@Nonnull ChartUrn urn,
      @Nonnull ChartInfo info) {
    final ChartDocument document = setUrnDerivedFields(urn);
    document.setTitle(info.getTitle());
    document.setDescription(info.getDescription());
    if (info.getType() != null) {
      document.setType(info.getType());
    }
    if (info.getAccess() != null) {
      document.setAccess(info.getAccess());
    }
    return document;
  }

  @Nonnull
  private ChartDocument getDocumentToUpdateFromAspect(@Nonnull ChartUrn urn,
      @Nonnull ChartQuery query) {
    return setUrnDerivedFields(urn)
        .setQueryType(query.getType());
  }

  @Nonnull
  private ChartDocument getDocumentToUpdateFromAspect(@Nonnull ChartUrn urn,
      @Nonnull Ownership ownership) {
    return setUrnDerivedFields(urn)
        .setOwners(BuilderUtils.getCorpUserOwners(ownership));
  }

  @Nonnull
  private ChartDocument getDocumentToUpdateFromAspect(@Nonnull ChartUrn urn,
      @Nonnull Status status) {
    return setUrnDerivedFields(urn)
        .setRemoved(status.isRemoved());
  }

  @Nonnull
  private List<ChartDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull ChartSnapshot snapshot) {
    ChartUrn urn = snapshot.getUrn();
    return snapshot.getAspects().stream().map(aspect -> {
      if (aspect.isChartInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getChartInfo());
      } else if (aspect.isChartQuery()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getChartQuery());
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
  public List<ChartDocument> getDocumentsToUpdate(@Nonnull RecordTemplate snapshot) {
    if (snapshot instanceof ChartSnapshot) {
      return getDocumentsToUpdateFromSnapshotType((ChartSnapshot) snapshot);
    }
    return Collections.emptyList();
  }

  @Nonnull
  @Override
  public Class<ChartDocument> getDocumentType() {
    return ChartDocument.class;
  }
}
