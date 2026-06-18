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
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
        .actorPoliciesByPrivilege(indexPoliciesByPrivilege(dataHubPolicySet))
        .groupMembership(groupMembership)
        .enforceExistenceEnabled(enforceExistenceEnabled)
        .build();
  }

  private final Authentication authentication;
  private final boolean enforceExistenceEnabled;

  @EqualsAndHashCode.Exclude @Builder.Default
  private final Set<DataHubPolicyInfo> policyInfoSet = Collections.emptySet();

  /**
   * Derived once per session from {@link #policyInfoSet} so authorize() can look up candidates for
   * a privilege in O(1) without scanning the full actor-applicable set on every check.
   */
  @EqualsAndHashCode.Exclude @Builder.Default
  private final Map<String, List<RecordTemplate>> actorPoliciesByPrivilege = Collections.emptyMap();

  @EqualsAndHashCode.Exclude @Builder.Default
  private final Collection<Urn> groupMembership = Collections.emptyList();

  @EqualsAndHashCode.Exclude private final boolean systemAuth;

  public Urn getActorUrn() {
    return UrnUtils.getUrn(authentication.getActor().toUrnStr());
  }

  /**
   * Groups actor-applicable policies by privilege. Uses the same {@link DataHubPolicyInfo}
   * references as the policy cache.
   */
  @Nonnull
  public static Map<String, List<RecordTemplate>> indexPoliciesByPrivilege(
      @Nullable final Set<DataHubPolicyInfo> policyInfoSet) {
    if (policyInfoSet == null || policyInfoSet.isEmpty()) {
      return Collections.emptyMap();
    }
    final Map<String, List<RecordTemplate>> byPrivilege = new HashMap<>();
    for (final DataHubPolicyInfo policy : policyInfoSet) {
      if (policy.getPrivileges() == null) {
        continue;
      }
      for (final String privilege : policy.getPrivileges()) {
        byPrivilege.computeIfAbsent(privilege, ignored -> new ArrayList<>()).add(policy);
      }
    }
    return Collections.unmodifiableMap(byPrivilege);
  }

  /**
   * Actor is considered active if a corp user key is present when enforcement is on and the user is
   * not soft-deleted or suspended.
   *
   * @param aspectRetriever aspect retriever - ideally the SystemEntityClient backed one for caching
   * @return active status
   */
  public boolean isActive(
      OperationContext context, com.linkedin.metadata.aspect.AspectRetriever aspectRetriever) {
    // system cannot be disabled
    if (SYSTEM_ACTOR.equals(authentication.getActor().toUrnStr())) {
      return true;
    }

    Urn selfUrn = UrnUtils.getUrn(authentication.getActor().toUrnStr());
    Map<Urn, Map<String, Aspect>> urnAspectMap =
        aspectRetriever.getLatestAspectObjects(
            context,
            Set.of(selfUrn),
            Set.of(STATUS_ASPECT_NAME, CORP_USER_STATUS_ASPECT_NAME, CORP_USER_KEY_ASPECT_NAME));

    Map<String, Aspect> aspectMap = urnAspectMap.getOrDefault(selfUrn, Map.of());

    if (enforceExistenceEnabled && !aspectMap.containsKey(CORP_USER_KEY_ASPECT_NAME)) {
      // No corp user key aspect (never provisioned, purged, or inconsistent); not inferrable.
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
