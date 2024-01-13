package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for removing the Domain associated with a Metadata Asset. Requires the EDIT_DOMAINS
 * privilege for a particular asset.
 */
@Slf4j
@RequiredArgsConstructor
public class UnsetDomainResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService
      _entityService; // TODO: Remove this when 'exists' added to EntityClient

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final Urn entityUrn = Urn.createFromString(environment.getArgument("entityUrn"));

    return CompletableFuture.supplyAsync(
        () -> {
          if (!DomainUtils.isAuthorizedToUpdateDomainsForEntity(
              environment.getContext(), entityUrn)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          validateUnsetDomainInput(entityUrn, _entityService);
          try {
            Domains domains =
                (Domains)
                    EntityUtils.getAspectFromEntity(
                        entityUrn.toString(), DOMAINS_ASPECT_NAME, _entityService, new Domains());
            unsetDomain(domains);

            // Create the Domains aspects
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithUrn(entityUrn, DOMAINS_ASPECT_NAME, domains);
            _entityClient.ingestProposal(proposal, context.getAuthentication(), false);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to unset Domains for resource with entity urn {}: {}",
                entityUrn,
                e.getMessage());
            throw new RuntimeException(
                String.format("Failed to unset Domains for resource with entity urn %s", entityUrn),
                e);
          }
        });
  }

  public static Boolean validateUnsetDomainInput(Urn entityUrn, EntityService entityService) {

    if (!entityService.exists(entityUrn)) {
      throw new IllegalArgumentException(
          String.format("Failed to add Entity %s to Domain %s. Entity does not exist.", entityUrn));
    }

    return true;
  }

  private static void unsetDomain(@Nonnull Domains domains) {
    if (!domains.hasDomains()) {
      domains.setDomains(new UrnArray());
    }
    domains.getDomains().clear();
  }
}
