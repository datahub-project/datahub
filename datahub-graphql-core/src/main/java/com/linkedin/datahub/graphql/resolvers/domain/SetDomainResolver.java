package com.linkedin.datahub.graphql.resolvers.domain;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


/**
 * Resolver used for updating the Domain associated with a Metadata Asset. Requires the EDIT_DOMAINS privilege for a particular asset.
 */
@Slf4j
@RequiredArgsConstructor
public class SetDomainResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;  // TODO: Remove this when 'exists' added to EntityClient

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final Urn entityUrn = Urn.createFromString(environment.getArgument("entityUrn"));
    final Urn domainUrn = Urn.createFromString(environment.getArgument("domainUrn"));

    return CompletableFuture.supplyAsync(() -> {

      if (!DomainUtils.isAuthorizedToUpdateDomainsForEntity(environment.getContext(), entityUrn)) {
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      }
      validateSetDomainInput(
        entityUrn,
        domainUrn,
        _entityService
      );
      try {
        Domains domains = (Domains) getAspectFromEntity(
            entityUrn.toString(),
            Constants.DOMAINS_ASPECT_NAME,
            _entityService,
            new Domains());
        setDomain(domains, domainUrn);

        // Create the Domains aspects
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityUrn(entityUrn);
        proposal.setEntityType(entityUrn.getEntityType());
        proposal.setAspectName(Constants.DOMAINS_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(domains));
        proposal.setChangeType(ChangeType.UPSERT);
        _entityClient.ingestProposal(proposal, context.getAuthentication());
        return true;
      } catch (Exception e) {
        log.error("Failed to set Domain to resource with entity urn {}, domain urn {}: {}", entityUrn, domainUrn, e.getMessage());
        throw new RuntimeException(String.format("Failed to set Domain to resource with entity urn %s, domain urn %s", entityUrn, domainUrn), e);
      }
    });
  }

  public static Boolean validateSetDomainInput(
      Urn entityUrn,
      Urn domainUrn,
      EntityService entityService
  ) {

    if (!entityService.exists(domainUrn)) {
      throw new IllegalArgumentException(
          String.format("Failed to add Entity %s to Domain %s. Domain does not exist.", entityUrn, domainUrn));
    }

    if (!entityService.exists(entityUrn)) {
      throw new IllegalArgumentException(
          String.format("Failed to add Entity %s to Domain %s. Entity does not exist.", entityUrn, domainUrn));
    }

    return true;
  }

  private static void setDomain(Domains domains, Urn domainUrn) {
    final UrnArray newDomain = new UrnArray();
    newDomain.add(domainUrn);
    domains.setDomains(newDomain);
  }
}