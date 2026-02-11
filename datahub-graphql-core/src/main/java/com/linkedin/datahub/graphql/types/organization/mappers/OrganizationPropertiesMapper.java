package com.linkedin.datahub.graphql.types.organization.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.OrganizationProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link com.linkedin.organization.OrganizationProperties} to generated GraphQL {@link
 * OrganizationProperties}.
 */
public class OrganizationPropertiesMapper
    implements ModelMapper<
        com.linkedin.organization.OrganizationProperties, OrganizationProperties> {

  public static final OrganizationPropertiesMapper INSTANCE = new OrganizationPropertiesMapper();

  public static OrganizationProperties map(
      @Nonnull QueryContext context,
      @Nonnull final com.linkedin.organization.OrganizationProperties input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public OrganizationProperties apply(
      @Nonnull QueryContext context,
      @Nullable final com.linkedin.organization.OrganizationProperties input) {
    if (input == null) {
      return null;
    }

    final OrganizationProperties result = new OrganizationProperties();
    result.setName(input.getName());
    if (input.hasDescription()) {
      result.setDescription(input.getDescription());
    }
    if (input.hasCreated()) {
      result.setCreatedTime(input.getCreated().getTime());
    }
    if (input.hasLogoUrl()) {
      result.setLogoUrl(input.getLogoUrl());
    }
    return result;
  }
}
