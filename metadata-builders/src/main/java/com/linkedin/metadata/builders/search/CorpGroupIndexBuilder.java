package com.linkedin.metadata.builders.search;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.search.CorpGroupDocument;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CorpGroupIndexBuilder extends BaseIndexBuilder<CorpGroupDocument> {

  public CorpGroupIndexBuilder() {
    super(Collections.singletonList(CorpGroupSnapshot.class), CorpGroupDocument.class);
  }

  @Nonnull
  private CorpGroupDocument getDocumentToUpdateFromAspect(@Nonnull CorpGroupUrn urn,
      @Nonnull CorpGroupInfo corpGroupInfo) {
    return new CorpGroupDocument().setUrn(urn)
        .setAdmins(BuilderUtils.getCorpUsernames(corpGroupInfo.getAdmins()))
        .setMembers(BuilderUtils.getCorpUsernames(corpGroupInfo.getMembers()))
        .setGroups(BuilderUtils.getCorpGroupnames(corpGroupInfo.getGroups()))
        .setEmail(corpGroupInfo.getEmail());
  }

  @Nonnull
  private List<CorpGroupDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull CorpGroupSnapshot corpGroupSnapshot) {
    final CorpGroupUrn urn = corpGroupSnapshot.getUrn();
    return corpGroupSnapshot.getAspects().stream().map(aspect -> {
      if (aspect.isCorpGroupInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getCorpGroupInfo());
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  @Nonnull
  public final List<CorpGroupDocument> getDocumentsToUpdate(@Nonnull RecordTemplate genericSnapshot) {
    if (genericSnapshot instanceof CorpGroupSnapshot) {
      return getDocumentsToUpdateFromSnapshotType((CorpGroupSnapshot) genericSnapshot);
    }
    return Collections.emptyList();
  }

  @Override
  @Nonnull
  public Class<CorpGroupDocument> getDocumentType() {
    return CorpGroupDocument.class;
  }
}