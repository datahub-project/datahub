package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteUtils {

  private DeleteUtils() {}

  public static boolean isAuthorizedToDeleteEntity(@Nonnull QueryContext context, Urn entityUrn) {
    return AuthUtil.isAuthorizedEntityUrns(
        context.getOperationContext(), DELETE, List.of(entityUrn));
  }

  public static void updateStatusForResources(
      @Nonnull OperationContext opContext,
      boolean removed,
      List<String> urnStrs,
      Urn actor,
      EntityService<?> entityService) {
    if (urnStrs.isEmpty()) {
      return;
    }

    Set<Urn> urns =
        urnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toCollection(LinkedHashSet::new));

    // One batch read for status aspects — avoids N sequential getAspect round-trips (each borrowing
    // an Ebean connection) before the single ingestProposal write.
    Map<Urn, List<RecordTemplate>> statusByUrn =
        entityService.getLatestAspects(
            opContext, urns, Set.of(Constants.STATUS_ASPECT_NAME), false);

    // Iterate the deduped set — duplicate input URNs must not emit duplicate Status MCPs.
    final List<MetadataChangeProposal> changes = new ArrayList<>(urns.size());
    for (Urn urn : urns) {
      Status status = copyStatus(findStatusAspect(statusByUrn.get(urn)));
      status.setRemoved(removed);
      changes.add(buildMetadataChangeProposalWithUrn(urn, Constants.STATUS_ASPECT_NAME, status));
    }
    EntityUtils.ingestChangeProposals(opContext, changes, entityService, actor, false);
  }

  @Nullable
  private static Status findStatusAspect(@Nullable List<RecordTemplate> aspects) {
    if (aspects == null) {
      return null;
    }
    for (RecordTemplate aspect : aspects) {
      if (aspect instanceof Status) {
        return (Status) aspect;
      }
    }
    return null;
  }

  /** Mutable copy so we never mutate cached/shared aspect instances from the batch read. */
  @Nonnull
  private static Status copyStatus(@Nullable Status existing) {
    if (existing == null) {
      return new Status();
    }
    try {
      return new Status(existing.data().copy());
    } catch (CloneNotSupportedException e) {
      Status copy = new Status();
      if (existing.hasRemoved()) {
        copy.setRemoved(existing.isRemoved());
      }
      return copy;
    }
  }
}
