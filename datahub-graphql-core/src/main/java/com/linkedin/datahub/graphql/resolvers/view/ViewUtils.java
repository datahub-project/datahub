package com.linkedin.datahub.graphql.resolvers.view;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.DataHubViewDefinitionInput;
import com.linkedin.datahub.graphql.generated.DataHubViewFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.LogicalOperator;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class ViewUtils {

  /**
   * Returns true if the authenticated actor is allowed to create a view with the given parameters.
   *
   * <p>The user can create a View if it's a personal View specific to them, or if it's a Global
   * view and they have the correct Platform privileges.
   *
   * @param type the type of the new View
   * @param context the current GraphQL {@link QueryContext}
   * @return true if the authenticator actor is allowed to change or delete the view, false
   *     otherwise.
   */
  public static boolean canCreateView(
      @Nonnull DataHubViewType type, @Nonnull QueryContext context) {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(context, "context must not be null");
    return DataHubViewType.PERSONAL.equals(type)
        || (DataHubViewType.GLOBAL.equals(type)
            && AuthorizationUtils.canManageGlobalViews(context));
  }

  /**
   * Returns true if the authenticated actor is allowed to update or delete the View with the
   * specified urn.
   *
   * @param viewService an instance of {@link ViewService}
   * @param viewUrn the urn of the View
   * @param context the current GraphQL {@link QueryContext}
   * @return true if the authenticator actor is allowed to change or delete the view, false
   *     otherwise.
   */
  public static boolean canUpdateView(
      @Nonnull ViewService viewService, @Nonnull Urn viewUrn, @Nonnull QueryContext context) {
    Objects.requireNonNull(viewService, "viewService must not be null");
    Objects.requireNonNull(viewUrn, "viewUrn must not be null");
    Objects.requireNonNull(context, "context must not be null");

    // Retrieve the view, determine it's type, and then go from there.
    final DataHubViewInfo viewInfo = viewService.getViewInfo(viewUrn, context.getAuthentication());

    if (viewInfo == null) {
      throw new IllegalArgumentException(
          String.format("Failed to modify View. View with urn %s does not exist.", viewUrn));
    }

    // If the View is Global, then the user must have ability to manage global views OR must be its
    // owner
    if (DataHubViewType.GLOBAL.equals(viewInfo.getType())
        && AuthorizationUtils.canManageGlobalViews(context)) {
      return true;
    }

    // If the View is Personal, then the current actor must be the owner.
    return isViewOwner(
        viewInfo.getCreated().getActor(),
        UrnUtils.getUrn(context.getAuthentication().getActor().toUrnStr()));
  }

  /**
   * Map a GraphQL {@link DataHubViewDefinition} to the GMS equivalent.
   *
   * @param input the GraphQL model
   * @return the GMS model
   */
  @Nonnull
  public static DataHubViewDefinition mapDefinition(
      @Nonnull final DataHubViewDefinitionInput input) {
    Objects.requireNonNull(input, "input must not be null");

    final DataHubViewDefinition result = new DataHubViewDefinition();
    if (input.getFilter() != null) {
      result.setFilter(mapFilter(input.getFilter()), SetMode.IGNORE_NULL);
    }
    result.setEntityTypes(
        new StringArray(
            input.getEntityTypes().stream()
                .map(EntityTypeMapper::getName)
                .collect(Collectors.toList())));
    return result;
  }

  /**
   * Converts an instance of {@link DataHubViewFilterInput} into the corresponding {@link Filter}
   * object, which is then persisted to the backend in an aspect.
   *
   * <p>We intentionally convert from a more rigid model to something more flexible to hedge for the
   * case in which the views feature evolves to require more advanced filter capabilities.
   *
   * <p>The risk we run is that people ingest Views through the Rest.li ingestion APIs (back door),
   * which cannot be rendered in full by the UI. We account for this on the read path by logging a
   * warning and returning an empty View in such cases.
   */
  private static Filter mapFilter(@Nonnull DataHubViewFilterInput input) {
    if (LogicalOperator.AND.equals(input.getOperator())) {
      // AND
      return buildAndFilter(input.getFilters());
    } else {
      // OR
      return buildOrFilter(input.getFilters());
    }
  }

  private static Filter buildAndFilter(@Nonnull List<FacetFilterInput> input) {
    final Filter result = new Filter();
    result.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            input.stream()
                                .map(ResolverUtils::criterionFromFilter)
                                .collect(Collectors.toList()))))));
    return result;
  }

  private static Filter buildOrFilter(@Nonnull List<FacetFilterInput> input) {
    final Filter result = new Filter();
    result.setOr(
        new ConjunctiveCriterionArray(
            input.stream()
                .map(
                    filter ->
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(ResolverUtils.criterionFromFilter(filter)))))
                .collect(Collectors.toList())));
    return result;
  }

  private static boolean isViewOwner(Urn creatorUrn, Urn actorUrn) {
    return creatorUrn.equals(actorUrn);
  }

  private ViewUtils() {}
}
