/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;

import com.linkedin.common.Siblings;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class SiblingsUtils {

  private SiblingsUtils() {}

  public static List<Urn> getSiblingUrns(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final EntityService entityService) {
    final Siblings siblingAspectOfEntity =
        (Siblings) entityService.getLatestAspect(opContext, entityUrn, SIBLINGS_ASPECT_NAME);
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
