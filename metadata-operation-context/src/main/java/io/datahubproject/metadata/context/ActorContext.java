package io.datahubproject.metadata.context;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ActorContext implements ContextInterface {

  private final Authentication authentication;
  @Builder.Default private final Set<DataHubPolicyInfo> policyInfoSet = Collections.emptySet();
  @Builder.Default private final Set<Urn> groupMembership = Collections.emptySet();
  private final boolean systemAuthentication;

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
        policyInfoSet.stream().mapToInt(policy -> policy.toString().hashCode()).sum());
  }
}
