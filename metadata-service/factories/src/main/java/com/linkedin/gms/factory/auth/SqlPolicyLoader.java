package com.linkedin.gms.factory.auth;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;

import com.datahub.authorization.PolicyFetcher;
import com.datahub.authorization.PolicyLoader;
import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Loads all policies from the primary store with one query, so the result is one consistent view of
 * committed data. Unlike paged reads, a single statement cannot skip a row when other policies are
 * created or deleted during the load.
 *
 * <p>Soft deletes need no filter: the dataHubPolicy entity has no status aspect, and writes of
 * unknown aspects are dropped (metadataChangeProposal.validation.ignoreUnknown), so a policy can
 * only be hard-deleted. If a status aspect is ever added to dataHubPolicy, this loader must skip
 * policies with status.removed=true, as the search-based fetch does.
 */
@Slf4j
public class SqlPolicyLoader implements PolicyLoader {

  // Only the first few unreadable URNs go in the log line.
  private static final int MAX_LOGGED_SKIPPED = 10;

  private final AspectDao aspectDao;

  public SqlPolicyLoader(@Nonnull AspectDao aspectDao) {
    this.aspectDao = Objects.requireNonNull(aspectDao);
  }

  /**
   * Skips policies whose stored value cannot be read. If no policy can be read but some exist, the
   * load fails, so the authorizer keeps its current policies instead of an empty cache. The result
   * gives the number of skipped policies, which the authorizer reports as a metric.
   */
  @Nonnull
  @Override
  public Result loadAll() {
    final List<PolicyFetcher.Policy> policies = new ArrayList<>();
    final List<String> skipped = new ArrayList<>();
    RuntimeException firstError = null;
    try (Stream<EntityAspect> rows =
        aspectDao.streamAspects(POLICY_ENTITY_NAME, DATAHUB_POLICY_INFO_ASPECT_NAME)) {
      for (EntityAspect row : (Iterable<EntityAspect>) rows::iterator) {
        try {
          policies.add(
              new PolicyFetcher.Policy(
                  UrnUtils.getUrn(row.getUrn()),
                  RecordUtils.toRecordTemplate(DataHubPolicyInfo.class, row.getMetadata())));
        } catch (RuntimeException e) {
          skipped.add(row.getUrn());
          if (firstError == null) {
            firstError = e;
          }
        }
      }
    }
    if (!skipped.isEmpty()) {
      // One line for each load, without a stack trace: the same rows fail on every reload.
      log.warn(
          "Skipped {} policies whose stored value cannot be read: {}. First error: {}",
          skipped.size(),
          skipped.subList(0, Math.min(skipped.size(), MAX_LOGGED_SKIPPED)),
          firstError.toString());
      if (policies.isEmpty()) {
        throw new IllegalStateException(
            "No stored policy could be read (" + skipped.size() + " skipped)", firstError);
      }
    }
    return new Result(policies, skipped.size());
  }
}
