package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;

import com.linkedin.common.Siblings;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class SiblingsUtils {

  private SiblingsUtils() {}

  public static List<Urn> getSiblingUrns(
      @Nonnull final Urn entityUrn, @Nonnull final EntityService entityService) {
    final Siblings siblingAspectOfEntity =
        (Siblings) entityService.getLatestAspect(entityUrn, SIBLINGS_ASPECT_NAME);
    if (siblingAspectOfEntity != null && siblingAspectOfEntity.hasSiblings()) {
      return siblingAspectOfEntity.getSiblings();
    }
    return new ArrayList<>();
  }

  public static Optional<Urn> getNextSiblingUrn(
      @Nonnull final List<Urn> siblingUrns, @Nonnull final HashSet<Urn> usedUrns) {
    final List<Urn> unusedSiblingUrns =
        siblingUrns.stream().filter(urn -> !usedUrns.contains(urn)).collect(Collectors.toList());
    return unusedSiblingUrns.stream().findFirst();
  }
}
