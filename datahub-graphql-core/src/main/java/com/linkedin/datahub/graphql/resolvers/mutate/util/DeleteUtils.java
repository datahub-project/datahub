package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.datahub.authorization.AuthUtil.buildAPIDisjunctivePrivilegeGroup;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;

import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteUtils {

  private DeleteUtils() {}

  public static boolean isAuthorizedToDeleteEntity(@Nonnull QueryContext context, Urn entityUrn) {
    DisjunctivePrivilegeGroup orPrivilegeGroups = buildAPIDisjunctivePrivilegeGroup(ENTITY, DELETE);

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        entityUrn.getEntityType(),
        entityUrn.toString(),
        orPrivilegeGroups);
  }

  public static void updateStatusForResources(
      boolean removed, List<String> urnStrs, Urn actor, EntityService entityService) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (String urnStr : urnStrs) {
      changes.add(buildSoftDeleteProposal(removed, urnStr, actor, entityService));
    }
    EntityUtils.ingestChangeProposals(changes, entityService, actor, false);
  }

  private static MetadataChangeProposal buildSoftDeleteProposal(
      boolean removed, String urnStr, Urn actor, EntityService entityService) {
    Status status =
        (Status)
            EntityUtils.getAspectFromEntity(
                urnStr, Constants.STATUS_ASPECT_NAME, entityService, new Status());
    status.setRemoved(removed);
    return buildMetadataChangeProposalWithUrn(
        UrnUtils.getUrn(urnStr), Constants.STATUS_ASPECT_NAME, status);
  }
}
