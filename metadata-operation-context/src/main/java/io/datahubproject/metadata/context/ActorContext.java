package io.datahubproject.metadata.context;

import static com.linkedin.metadata.Constants.CORP_USER_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_STATUS_SUSPENDED;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.datahub.authentication.Authentication;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder
@Getter
@EqualsAndHashCode
public class ActorContext implements ContextInterface {

  public static ActorContext asSystem(
      Authentication systemAuthentication, boolean enforceExistenceEnabled) {
    return ActorContext.builder()
        .systemAuth(true)
        .authentication(systemAuthentication)
        .enforceExistenceEnabled(enforceExistenceEnabled)
        .build();
  }

  public static ActorContext asSessionRestricted(
      Authentication authentication,
      Set<DataHubPolicyInfo> dataHubPolicySet,
      Collection<Urn> groupMembership,
      boolean enforceExistenceEnabled) {
    return ActorContext.builder()
        .systemAuth(false)
        .authentication(authentication)
        .policyInfoSet(dataHubPolicySet)
        .groupMembership(groupMembership)
        .enforceExistenceEnabled(enforceExistenceEnabled)
        .build();
  }

  private final Authentication authentication;
  private final boolean enforceExistenceEnabled;

  @EqualsAndHashCode.Exclude @Builder.Default
  private final Set<DataHubPolicyInfo> policyInfoSet = Collections.emptySet();

  @EqualsAndHashCode.Exclude @Builder.Default
  private final Collection<Urn> groupMembership = Collections.emptyList();

  @EqualsAndHashCode.Exclude private final boolean systemAuth;

  public Urn getActorUrn() {
    return UrnUtils.getUrn(authentication.getActor().toUrnStr());
  }

  /**
   * Actor is considered active if the user is not hard-deleted, soft-deleted, and is not suspended
   *
   * @param aspectRetriever aspect retriever - ideally the SystemEntityClient backed one for caching
   * @return active status
   */
  public boolean isActive(AspectRetriever aspectRetriever) {
    // system cannot be disabled
    if (SYSTEM_ACTOR.equals(authentication.getActor().toUrnStr())) {
      return true;
    }

    Urn selfUrn = UrnUtils.getUrn(authentication.getActor().toUrnStr());
    Map<Urn, Map<String, Aspect>> urnAspectMap =
        aspectRetriever.getLatestAspectObjects(
            Set.of(selfUrn),
            Set.of(STATUS_ASPECT_NAME, CORP_USER_STATUS_ASPECT_NAME, CORP_USER_KEY_ASPECT_NAME));

    Map<String, Aspect> aspectMap = urnAspectMap.getOrDefault(selfUrn, Map.of());

    if (enforceExistenceEnabled && !aspectMap.containsKey(CORP_USER_KEY_ASPECT_NAME)) {
      // user is hard deleted
      return false;
    }

    Status status =
        Optional.ofNullable(aspectMap.get(STATUS_ASPECT_NAME))
            .map(a -> new Status(a.data()))
            .orElse(new Status().setRemoved(false));
    CorpUserStatus corpUserStatus =
        Optional.ofNullable(aspectMap.get(CORP_USER_STATUS_ASPECT_NAME))
            .map(a -> new CorpUserStatus(a.data()))
            .orElse(new CorpUserStatus().setStatus(""));

    return !status.isRemoved() && !CORP_USER_STATUS_SUSPENDED.equals(corpUserStatus.getStatus());
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
