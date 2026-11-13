package com.datahub.authorization;

import com.linkedin.policy.DataHubPolicyInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An immutable index of policies by privilege name, plus {@link DataHubAuthorizer#ALL}.
 *
 * <p>A new snapshot replaces the old one as a whole, so readers always see one complete, consistent
 * set of policies without a lock.
 *
 * <p>Each list is ordered newest {@code lastUpdatedTimestamp} first, policies without a timestamp
 * last, then by policy URN. This matches the order the search-based policy fetch has always
 * returned, so the policy named in an ALLOW reason does not change.
 */
final class PolicySnapshot {

  static final PolicySnapshot EMPTY = new PolicySnapshot(Collections.emptyMap(), 0);

  private static final Comparator<PolicyFetcher.Policy> ORDER =
      Comparator.comparing(
              (PolicyFetcher.Policy p) -> lastUpdated(p.getPolicyInfo()),
              Comparator.nullsLast(Comparator.reverseOrder()))
          .thenComparing(
              p -> p.getUrn() == null ? null : p.getUrn().toString(),
              Comparator.nullsLast(Comparator.naturalOrder()));

  private final Map<String, List<DataHubPolicyInfo>> byPrivilege;
  private final int size;

  private PolicySnapshot(Map<String, List<DataHubPolicyInfo>> byPrivilege, int size) {
    this.byPrivilege = byPrivilege;
    this.size = size;
  }

  @Nonnull
  static PolicySnapshot of(@Nonnull Collection<PolicyFetcher.Policy> policies) {
    // List.sort is stable, so entries that tie (no timestamp, no URN) keep their input order.
    final List<PolicyFetcher.Policy> sorted = new ArrayList<>(policies);
    sorted.sort(ORDER);

    final Map<String, List<DataHubPolicyInfo>> index = new LinkedHashMap<>();
    for (PolicyFetcher.Policy policy : sorted) {
      final DataHubPolicyInfo info = policy.getPolicyInfo();
      if (info.getPrivileges() != null) {
        for (String privilege : info.getPrivileges()) {
          index.computeIfAbsent(privilege, k -> new ArrayList<>()).add(info);
        }
      }
      index.computeIfAbsent(DataHubAuthorizer.ALL, k -> new ArrayList<>()).add(info);
    }

    index.replaceAll((k, v) -> List.copyOf(v));
    return new PolicySnapshot(Collections.unmodifiableMap(index), sorted.size());
  }

  /** Builds a snapshot from policies without URNs. Only for the test hook. */
  @Nonnull
  static PolicySnapshot ofPolicyInfos(@Nonnull Collection<DataHubPolicyInfo> policies) {
    final List<PolicyFetcher.Policy> entries = new ArrayList<>(policies.size());
    for (DataHubPolicyInfo info : policies) {
      entries.add(new PolicyFetcher.Policy(null, info));
    }
    return of(entries);
  }

  List<DataHubPolicyInfo> getOrDefault(
      @Nonnull String key, @Nullable List<DataHubPolicyInfo> defaultValue) {
    return byPrivilege.getOrDefault(key, defaultValue);
  }

  int size() {
    return size;
  }

  @Nullable
  private static Long lastUpdated(@Nonnull DataHubPolicyInfo info) {
    return info.hasLastUpdatedTimestamp() ? info.getLastUpdatedTimestamp() : null;
  }
}
