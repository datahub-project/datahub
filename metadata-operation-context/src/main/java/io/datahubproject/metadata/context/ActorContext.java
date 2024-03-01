package io.datahubproject.metadata.context;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ActorContext implements ContextInterface {

  public static ActorContext asSystem(Authentication systemAuthentication) {
    return ActorContext.builder().systemAuth(true).authentication(systemAuthentication).build();
  }

  public static ActorContext asSessionRestricted(
      Authentication authentication,
      Set<DataHubPolicyInfo> dataHubPolicySet,
      Collection<Urn> groupMembership) {
    return ActorContext.builder()
        .systemAuth(false)
        .authentication(authentication)
        .policyInfoSet(dataHubPolicySet)
        .groupMembership(groupMembership)
        .build();
  }

  private final Authentication authentication;
  @Builder.Default private final Set<DataHubPolicyInfo> policyInfoSet = Collections.emptySet();
  @Builder.Default private final Collection<Urn> groupMembership = Collections.emptyList();
  private final boolean systemAuth;

  public Urn getActorUrn() {
    return UrnUtils.getUrn(authentication.getActor().toUrnStr());
  }

  /**
   * The current implementation creates a cache entry unique for the set of policies.
   *
   * <p>We are relying on the consistent hash code implementation of String and the consistent
   * conversion of the policy into a String
   *
   * @return
   */
  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.of(
        policyInfoSet.stream()
            .filter(policy -> PoliciesConfig.ACTIVE_POLICY_STATE.equals(policy.getState()))
            .mapToInt(
                policy -> {
                  if (policy.getActors().hasResourceOwners()
                      || policy.getActors().hasResourceOwnersTypes()) {
                    // results are based on actor, distinct() added to remove duplicate sums of
                    // multiple owner policies
                    return authentication.getActor().toUrnStr().hashCode();
                  } else {
                    return policy.toString().hashCode();
                  }
                })
            .distinct()
            .sum());
  }
}
