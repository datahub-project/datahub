package com.linkedin.metadata.builders.search;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.search.CorpUserInfoDocument;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CorpUserInfoIndexBuilder extends BaseIndexBuilder<CorpUserInfoDocument> {

  public CorpUserInfoIndexBuilder() {
    super(Collections.singletonList(CorpUserSnapshot.class), CorpUserInfoDocument.class);
  }

  @Nonnull
  private CorpUserInfoDocument getDocumentToUpdateFromAspect(@Nonnull CorpuserUrn urn,
      @Nonnull CorpUserInfo corpUserInfo) {
    final String fullName = corpUserInfo.getFullName() == null ? "" : corpUserInfo.getFullName();
    final String title = corpUserInfo.getTitle() == null ? "" : corpUserInfo.getTitle();
    final String managerLdap =
        corpUserInfo.getManagerUrn() == null ? "" : corpUserInfo.getManagerUrn().getUsernameEntity();
    return new CorpUserInfoDocument().setUrn(urn)
        .setLdap(urn.getUsernameEntity())
        .setFullName(fullName)
        .setTitle(title)
        .setActive(corpUserInfo.isActive())
        .setManagerLdap(managerLdap);
  }

  @Nonnull
  private CorpUserInfoDocument getDocumentToUpdateFromAspect(@Nonnull CorpuserUrn urn,
      @Nonnull CorpUserEditableInfo corpUserEditableInfo) {
    final String aboutMe = corpUserEditableInfo.getAboutMe() == null ? "" : corpUserEditableInfo.getAboutMe();
    return new CorpUserInfoDocument()
        .setUrn(urn)
        .setAboutMe(aboutMe)
        .setTeams(corpUserEditableInfo.getTeams())
        .setSkills(corpUserEditableInfo.getSkills());
  }

  @Nonnull
  private List<CorpUserInfoDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull CorpUserSnapshot corpUserSnapshot) {
    CorpuserUrn urn = corpUserSnapshot.getUrn();
    return corpUserSnapshot.getAspects().stream().map(aspect -> {
      if (aspect.isCorpUserInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getCorpUserInfo());
      } else if (aspect.isCorpUserEditableInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getCorpUserEditableInfo());
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  @Nonnull
  public final List<CorpUserInfoDocument> getDocumentsToUpdate(@Nonnull RecordTemplate genericSnapshot) {
    if (genericSnapshot instanceof CorpUserSnapshot) {
      return getDocumentsToUpdateFromSnapshotType((CorpUserSnapshot) genericSnapshot);
    }
    return Collections.emptyList();
  }

  @Override
  @Nonnull
  public Class<CorpUserInfoDocument> getDocumentType() {
    return CorpUserInfoDocument.class;
  }
}