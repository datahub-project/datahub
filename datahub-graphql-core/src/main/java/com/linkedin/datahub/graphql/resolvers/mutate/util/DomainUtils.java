package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.google.common.collect.ImmutableList;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


@Slf4j
public class DomainUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  private DomainUtils() { }

  public static boolean isAuthorizedToUpdateDomainsForEntity(@Nonnull QueryContext context, Urn entityUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOMAINS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        entityUrn.getEntityType(),
        entityUrn.toString(),
        orPrivilegeGroups);
  }

  public static void setDomainForResources(
      @Nullable Urn domainUrn,
      List<ResourceRefInput> resources,
      Urn actor,
      EntityService entityService
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceRefInput resource : resources) {
      changes.add(buildSetDomainProposal(domainUrn, resource, actor, entityService));
    }
    ingestChangeProposals(changes, entityService, actor);
  }

  private static MetadataChangeProposal buildSetDomainProposal(
      @Nullable Urn domainUrn,
      ResourceRefInput resource,
      Urn actor,
      EntityService entityService
  ) {
    Domains domains = (Domains) getAspectFromEntity(
        resource.getResourceUrn(),
        Constants.DOMAINS_ASPECT_NAME,
        entityService,
        new Domains());
    final UrnArray newDomains = new UrnArray();
    if (domainUrn != null) {
      newDomains.add(domainUrn);
    }
    domains.setDomains(newDomains);
    return buildMetadataChangeProposal(UrnUtils.getUrn(resource.getResourceUrn()), Constants.DOMAINS_ASPECT_NAME, domains, actor, entityService);
  }

  public static void validateDomain(Urn domainUrn, EntityService entityService) {
    if (!entityService.exists(domainUrn)) {
      throw new IllegalArgumentException(String.format("Failed to validate Domain with urn %s. Urn does not exist.", domainUrn));
    }
  }

  private static void ingestChangeProposals(List<MetadataChangeProposal> changes, EntityService entityService, Urn actor) {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      entityService.ingestProposal(change, getAuditStamp(actor));
    }
  }
}