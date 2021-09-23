package com.linkedin.metadata.builders.search;

import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.Ownership;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.search.GlossaryTermInfoDocument;
import com.linkedin.metadata.snapshot.GlossaryTermSnapshot;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class GlossaryTermInfoIndexBuilder extends BaseIndexBuilder<GlossaryTermInfoDocument> {

  public GlossaryTermInfoIndexBuilder() {
    super(Collections.singletonList(GlossaryTermSnapshot.class), GlossaryTermInfoDocument.class);
  }

  @Nonnull
  private GlossaryTermInfoDocument getDocumentToUpdateFromAspect(@Nonnull GlossaryTermUrn urn, @Nonnull Ownership ownership) {
    final StringArray owners = BuilderUtils.getCorpUserOwners(ownership);
    return new GlossaryTermInfoDocument()
            .setUrn(urn)
            .setOwners(owners);
  }

  @Nonnull
  public static String buildBrowsePath(@Nonnull GlossaryTermUrn urn) {
    return "/" + urn.getNameEntity().replace('.', '/').toLowerCase();
  }

  @Nonnull
  private static String getTermName(@Nonnull GlossaryTermUrn urn) {
    final String name = urn.getNameEntity() == null ? "" : urn.getNameEntity();
    if (name.contains(".")) {
      String[] nodes = name.split(Pattern.quote("."));
      return nodes[nodes.length - 1];
    }
    return name;
  }

  @Nonnull
  private static GlossaryTermInfoDocument setUrnDerivedFields(@Nonnull GlossaryTermUrn urn) {
    return new GlossaryTermInfoDocument()
            .setName(getTermName(urn))
            .setUrn(urn)
            .setBrowsePaths(new StringArray(Collections.singletonList(buildBrowsePath(urn))));
  }

  @Nonnull
  private GlossaryTermInfoDocument getDocumentToUpdateFromAspect(@Nonnull GlossaryTermUrn urn,
      @Nonnull GlossaryTermInfo glossaryTermInfo) {
    final String definition = glossaryTermInfo.getDefinition() == null ? "" : glossaryTermInfo.getDefinition();
    final String termSource = glossaryTermInfo.getTermSource() == null ? "" : glossaryTermInfo.getTermSource();
    return new GlossaryTermInfoDocument().setUrn(urn)
        .setDefinition(definition)
        .setTermSource(termSource)
        .setName(getTermName(urn));
  }

  @Nonnull
  private List<GlossaryTermInfoDocument> getDocumentsToUpdateFromSnapshotType(@Nonnull GlossaryTermSnapshot glossaryTermSnapshot) {
    GlossaryTermUrn urn = glossaryTermSnapshot.getUrn();
    final List<GlossaryTermInfoDocument> glossaryTermInfoDocuments = glossaryTermSnapshot.getAspects().stream().map(aspect -> {
      if (aspect.isGlossaryTermInfo()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getGlossaryTermInfo());
      } else if (aspect.isOwnership()) {
        return getDocumentToUpdateFromAspect(urn, aspect.getOwnership());
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
    glossaryTermInfoDocuments.add(setUrnDerivedFields(urn));
    return glossaryTermInfoDocuments;
  }

  @Override
  @Nonnull
  public final List<GlossaryTermInfoDocument> getDocumentsToUpdate(@Nonnull RecordTemplate genericSnapshot) {
    if (genericSnapshot instanceof GlossaryTermSnapshot) {
      return getDocumentsToUpdateFromSnapshotType((GlossaryTermSnapshot) genericSnapshot);
    }
    return Collections.emptyList();
  }

  @Override
  @Nonnull
  public Class<GlossaryTermInfoDocument> getDocumentType() {
    return GlossaryTermInfoDocument.class;
  }
}