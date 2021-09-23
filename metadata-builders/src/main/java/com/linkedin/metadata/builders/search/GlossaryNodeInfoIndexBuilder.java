package com.linkedin.metadata.builders.search;

import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.search.GlossaryNodeInfoDocument;
import com.linkedin.metadata.snapshot.GlossaryNodeSnapshot;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


@Slf4j
public class GlossaryNodeInfoIndexBuilder extends BaseIndexBuilder<GlossaryNodeInfoDocument> {

  public GlossaryNodeInfoIndexBuilder() {
    super(Collections.singletonList(GlossaryNodeSnapshot.class), GlossaryNodeInfoDocument.class);
  }

  @Nonnull
  private GlossaryNodeInfoDocument getDocumentToUpdateFromAspect(@Nonnull GlossaryNodeUrn urn, @Nonnull Ownership ownership) {
    final StringArray owners = BuilderUtils.getCorpUserOwners(ownership);
    return new GlossaryNodeInfoDocument()
            .setUrn(urn)
            .setOwners(owners);
  }

  @Nonnull
  private GlossaryNodeInfoDocument getDocumentToUpdateFromAspect(@Nonnull GlossaryNodeUrn urn,
      @Nonnull GlossaryNodeInfo glossaryNodeInfo) {
    final String definition = glossaryNodeInfo.getDefinition() == null ? "" : glossaryNodeInfo.getDefinition();
    return new GlossaryNodeInfoDocument()
        .setUrn(urn)
        .setDefinition(definition);
  }

  @Nonnull
  private List<GlossaryNodeInfoDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull GlossaryNodeSnapshot glossaryNodeSnapshot) {
    GlossaryNodeUrn urn = glossaryNodeSnapshot.getUrn();
    return glossaryNodeSnapshot.getAspects().stream().map(aspect -> {
      if (aspect.isGlossaryNodeInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getGlossaryNodeInfo());
      } else if (aspect.isOwnership()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getOwnership());
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  @Nonnull
  public final List<GlossaryNodeInfoDocument> getDocumentsToUpdate(@Nonnull RecordTemplate genericSnapshot) {
    if (genericSnapshot instanceof GlossaryNodeSnapshot) {
      return getDocumentsToUpdateFromSnapshotType((GlossaryNodeSnapshot) genericSnapshot);
    }
    return Collections.emptyList();
  }

  @Override
  @Nonnull
  public Class<GlossaryNodeInfoDocument> getDocumentType() {
    return GlossaryNodeInfoDocument.class;
  }
}