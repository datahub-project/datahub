package com.linkedin.datahub.graphql.types.domain;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainAssociation;
import com.linkedin.datahub.graphql.generated.DomainsAssociations;
import com.linkedin.datahub.graphql.generated.EntityType;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DomainsAssociationsMapper {
  public static final DomainsAssociationsMapper INSTANCE = new DomainsAssociationsMapper();

  public static DomainsAssociations map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.domain.Domains domains,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, domains, entityUrn);
  }

  public DomainsAssociations apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.domain.Domains input,
      @Nonnull final Urn entityUrn) {
    final DomainsAssociations result = new DomainsAssociations();
    result.setDomains(
        input.getDomains().stream()
            .map(domainUrn -> mapDomainAssociation(context, domainUrn, entityUrn))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList()));
    return result;
  }

  private static Optional<DomainAssociation> mapDomainAssociation(
      @Nullable final QueryContext context,
      @Nonnull final Urn domainUrn,
      @Nonnull final Urn entityUrn) {

    if (context == null || canView(context.getOperationContext(), domainUrn)) {
      final DomainAssociation result = new DomainAssociation();
      final Domain resultDomain = new Domain();
      resultDomain.setType(EntityType.DOMAIN);
      resultDomain.setUrn(domainUrn.toString());
      result.setDomain(resultDomain);
      result.setAssociatedUrn(entityUrn.toString());
      return Optional.of(result);
    }
    return Optional.empty();
  }
}
