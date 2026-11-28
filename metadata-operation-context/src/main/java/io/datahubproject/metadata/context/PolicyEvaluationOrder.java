package io.datahubproject.metadata.context;

import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCriterion;
import java.util.Comparator;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Orders the policies tried for one privilege so the cheapest are evaluated first.
 *
 * <p>The authorizer stops at the first policy that grants, so order never changes the result, only
 * the cost. Without an explicit order, the order is whatever the policy set iterates in, which
 * reshuffles whenever any policy changes: a single edit could put a policy that needs per-resource
 * lookups (a domain or glossary hierarchy walk, an ownership fetch) ahead of a broad grant, and
 * every check would then pay for those lookups.
 */
public final class PolicyEvaluationOrder {

  /** Criteria answered from the request's own resource spec, without fetching anything. */
  private static final Set<String> SPEC_ONLY_FIELDS =
      Set.of("TYPE", "RESOURCE_TYPE", "URN", "RESOURCE_URN");

  /** Cheapest first; ties keep their existing relative order when used with a stable sort. */
  public static final Comparator<DataHubPolicyInfo> CHEAPEST_FIRST =
      Comparator.comparingInt(PolicyEvaluationOrder::cost);

  private PolicyEvaluationOrder() {}

  /**
   * 0 = no resource scope; 1 = scoped only by type or URN; 2 = needs a lookup to match (any other
   * criterion, or matching through resource ownership).
   */
  static int cost(@Nonnull DataHubPolicyInfo policy) {
    final DataHubActorFilter actors = policy.getActors();
    final boolean matchesThroughOwnership =
        actors != null
            && (actors.isResourceOwners()
                || (actors.getResourceOwnersTypes() != null
                    && !actors.getResourceOwnersTypes().isEmpty()));
    return Math.max(resourceCost(policy), matchesThroughOwnership ? 2 : 0);
  }

  private static int resourceCost(@Nonnull DataHubPolicyInfo policy) {
    if (!policy.hasResources()) {
      return 0;
    }
    final DataHubResourceFilter resources = policy.getResources();
    if (resources.hasFilter() && resources.getFilter().hasCriteria()) {
      int cost = 0;
      for (PolicyMatchCriterion criterion : resources.getFilter().getCriteria()) {
        final String field = criterion.getField().toUpperCase(Locale.ROOT);
        cost = Math.max(cost, SPEC_ONLY_FIELDS.contains(field) ? 1 : 2);
      }
      return cost;
    }
    // Legacy form: a type and/or an explicit URN list, both answered from the spec.
    final boolean legacyScoped =
        resources.hasType() || (resources.hasResources() && !resources.isAllResources());
    return legacyScoped ? 1 : 0;
  }
}
