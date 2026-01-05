package com.linkedin.datahub.graphql.types.organization.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.identity.UserOrganizations;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UserOrganizationsAspectMapper {

  public static List<Organization> map(
      @Nullable final QueryContext context, @Nonnull final UserOrganizations organizations) {
    return organizations.getOrganizations().stream()
        .map(
            urn -> {
              Organization org = new Organization();
              org.setUrn(urn.toString());
              org.setType(EntityType.ORGANIZATION);
              return org;
            })
        .collect(Collectors.toList());
  }

  private UserOrganizationsAspectMapper() {}
}
