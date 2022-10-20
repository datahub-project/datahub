package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.google.common.collect.ImmutableList;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


@Slf4j
public class DeleteUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  private DeleteUtils() { }

  public static boolean isAuthorizedToDeleteEntity(@Nonnull QueryContext context, Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.DELETE_ENTITY_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        entityUrn.getEntityType(),
        entityUrn.toString(),
        orPrivilegeGroups);
  }

  public static void updateStatusForResources(
      boolean removed,
      List<String> urnStrs,
      Urn actor,
      EntityService entityService
  ) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (String urnStr : urnStrs) {
      changes.add(buildSoftDeleteProposal(removed, urnStr, actor, entityService));
    }
    ingestChangeProposals(changes, entityService, actor);
  }

  private static MetadataChangeProposal buildSoftDeleteProposal(
      boolean removed,
      String urnStr,
      Urn actor,
      EntityService entityService
  ) {
    Status status = (Status) getAspectFromEntity(
        urnStr,
        Constants.STATUS_ASPECT_NAME,
        entityService,
        new Status());
    status.setRemoved(removed);
    return buildMetadataChangeProposal(UrnUtils.getUrn(urnStr), Constants.STATUS_ASPECT_NAME, status, actor, entityService);
  }

  private static void ingestChangeProposals(List<MetadataChangeProposal> changes, EntityService entityService, Urn actor) {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      entityService.ingestProposal(change, getAuditStamp(actor), false);
    }
  }
}