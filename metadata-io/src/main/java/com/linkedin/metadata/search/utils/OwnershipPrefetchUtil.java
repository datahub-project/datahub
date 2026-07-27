package com.linkedin.metadata.search.utils;

import static com.datahub.authorization.AuthUtil.VIEW_RESTRICTED_ENTITY_TYPES;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextAuthorizer;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Batch-warms resource ownership into the per-request cache shared by policy evaluation, so
 * authorization entry points that check many resources (search-result restriction, REST/OpenAPI
 * list authorization, lineage) resolve ownership in one batch fetch instead of one fetch per
 * resource.
 */
@Slf4j
public class OwnershipPrefetchUtil {
  private OwnershipPrefetchUtil() {}

  /**
   * Batch-warm ownership for a collection of result URNs into the per-request cache. Gated behind
   * the {@code ownershipPrefetchEnabled} feature flag and the presence of an owner-scoped policy;
   * best-effort, falling back to lazy per-result resolution on any failure.
   */
  public static void prefetchOwnershipForUrns(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> urns) {
    if (!opContext
        .getOperationContextConfig()
        .getViewAuthorizationConfiguration()
        .isOwnershipPrefetchEnabled()) {
      log.trace("Ownership prefetch skipped: feature flag disabled");
      return;
    }
    final Authorizer authorizer = opContext.getAuthorizationContext().getAuthorizer();
    if (!(authorizer instanceof OperationContextAuthorizer ocAuthorizer)
        || !ocAuthorizer.hasResourceOwnerPolicy()) {
      log.trace("Ownership prefetch skipped: no owner-scoped policy");
      return;
    }
    try {
      final List<Urn> restrictedUrns =
          urns.stream()
              .filter(urn -> VIEW_RESTRICTED_ENTITY_TYPES.contains(urn.getEntityType()))
              .collect(Collectors.toList());
      if (!restrictedUrns.isEmpty()) {
        ocAuthorizer.prefetchOwners(opContext, restrictedUrns);
        log.debug("Ownership prefetch warmed {} restricted result(s)", restrictedUrns.size());
      }
    } catch (Exception e) {
      log.warn("Ownership prefetch failed, falling back to per-result resolution", e);
    }
  }
}
