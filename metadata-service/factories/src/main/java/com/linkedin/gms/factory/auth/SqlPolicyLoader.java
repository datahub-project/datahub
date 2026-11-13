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

  private final AspectDao aspectDao;

  public SqlPolicyLoader(@Nonnull AspectDao aspectDao) {
    this.aspectDao = Objects.requireNonNull(aspectDao);
  }

  @Nonnull
  @Override
  public List<PolicyFetcher.Policy> loadAll() {
    final List<PolicyFetcher.Policy> policies = new ArrayList<>();
    try (Stream<EntityAspect> rows =
        aspectDao.streamAspects(POLICY_ENTITY_NAME, DATAHUB_POLICY_INFO_ASPECT_NAME)) {
      rows.forEach(
          row -> {
            try {
              policies.add(
                  new PolicyFetcher.Policy(
                      UrnUtils.getUrn(row.getUrn()),
                      RecordUtils.toRecordTemplate(DataHubPolicyInfo.class, row.getMetadata())));
            } catch (RuntimeException e) {
              log.warn("Skipping policy {}: its stored value cannot be read.", row.getUrn(), e);
            }
          });
    }
    return policies;
  }
}
