package com.linkedin.datahub.graphql.authorization;

import static com.linkedin.metadata.utils.CriterionUtils.buildConjunctiveCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Search helpers for answering as if an unviewable entity did not exist. */
public class ViewAuthorizationSearchUtils {

  private static final String URN_FIELD = "urn";

  private ViewAuthorizationSearchUtils() {}

  /**
   * Whether {@code maybeUrn} names an entity the caller cannot view. A malformed urn counts as
   * viewable so that it fails or matches nothing exactly as it did before.
   */
  public static boolean isUnviewableUrn(
      @Nonnull final OperationContext opContext, @Nullable final String maybeUrn) {
    if (maybeUrn == null) {
      return false;
    }
    try {
      return !AuthorizationUtils.canView(opContext, Urn.createFromString(maybeUrn));
    } catch (URISyntaxException e) {
      return false;
    }
  }

  /**
   * A filter no document matches. AND it into a search keyed by an unviewable urn so the search
   * still runs and returns the same empty page a urn that doesn't exist would get.
   */
  @Nonnull
  public static Filter matchNothingFilter() {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                buildConjunctiveCriterion(buildIsNullCriterion(URN_FIELD))));
  }
}
