package com.linkedin.datahub.graphql.types.organization.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.organization.Organizations;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OrganizationsAspectMapper {

  public static List<Organization> map(
      @Nullable final QueryContext context, @Nonnull final Organizations organizations) {
    return organizations.getOrganizations().stream()
        .map(
            urn -> {
              Organization org = new Organization();
              org.setUrn(urn.toString());
              org.setType(EntityType.ORGANIZATION);
              // Note: The properties field will be populated by the GraphQL data fetcher
              // configured in GmsGraphQLEngine when the query explicitly requests properties.
              return org;
            })
        .collect(Collectors.toList());
  }

  private OrganizationsAspectMapper() {}
}
