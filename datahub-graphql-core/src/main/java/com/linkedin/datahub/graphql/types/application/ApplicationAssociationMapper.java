package com.linkedin.datahub.graphql.types.application;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Application;
import com.linkedin.datahub.graphql.generated.ApplicationAssociation;
import com.linkedin.datahub.graphql.generated.EntityType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class ApplicationAssociationMapper {

  public static final ApplicationAssociationMapper INSTANCE = new ApplicationAssociationMapper();

  public static ApplicationAssociation map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.application.Applications applications,
      @Nonnull final String entityUrn) {
    return INSTANCE.apply(context, applications, entityUrn);
  }

  public ApplicationAssociation apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.application.Applications applications,
      @Nonnull final String entityUrn) {
    if (applications.getApplications().size() > 0
        && (context == null
            || canView(context.getOperationContext(), applications.getApplications().get(0)))) {
      ApplicationAssociation association = new ApplicationAssociation();
      association.setApplication(
          Application.builder()
              .setType(EntityType.APPLICATION)
              .setUrn(applications.getApplications().get(0).toString())
              .build());
      association.setAssociatedUrn(entityUrn);
      return association;
    }
    return null;
  }
}
