package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.authorization.DomainWriteAuthorizationUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Authorizes {@code domains} writes for domain-separated writers.
 *
 * <p>CREATE / UPSERT establishing domains still match Create/Edit against proposed domains. {@code
 * PATCH} always uses Edit Entity and requires the actor to be allowed for both before and after
 * domains when before membership exists (after-only when establishing first domains).
 *
 * <p>In-transaction {@code validatePreCommit} re-checks when a user session is present (sync):
 * before+after Edit when domains already exist, otherwise Create/Edit establish against proposed
 * domains. On the MCE consumer (system / no request context) user domain auth is skipped — async
 * proposals were already authorized on the API thread.
 */
@Setter
@Getter
@Accessors(chain = true)
public class DomainWriteAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    List<BatchItem> domainsItems =
        items.stream()
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());
    if (domainsItems.isEmpty()) {
      return List.of();
    }

    Set<Urn> urns = domainsItems.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
    Map<Urn, Boolean> entityExists =
        DomainWriteAuthorizationUtils.resolveEntityExists(operationContext, aspectRetriever, urns);
    Map<Urn, Boolean> domainsExists =
        DomainWriteAuthorizationUtils.resolveDomainsAspectExists(
            operationContext, aspectRetriever, urns);
    Map<Urn, Domains> persistedDomains =
        DomainWriteAuthorizationUtils.loadPersistedDomains(operationContext, aspectRetriever, urns);

    List<AspectValidationException> failures = new ArrayList<>();
    Map<Urn, Domains> proposedSoFar = new HashMap<>();
    for (BatchItem item : domainsItems) {
      Urn urn = item.getUrn();
      boolean exists = Boolean.TRUE.equals(entityExists.get(urn));
      boolean domainsAspectExists = Boolean.TRUE.equals(domainsExists.get(urn));
      Domains beforeDomains =
          proposedSoFar.containsKey(urn) ? proposedSoFar.get(urn) : persistedDomains.get(urn);

      Domains afterDomains =
          DomainWriteAuthorizationUtils.resolveAndAccumulateProposedDomains(
              item, aspectRetriever, proposedSoFar, persistedDomains.get(urn));

      if (ChangeType.PATCH.equals(item.getChangeType())) {
        // Always fail closed if the patch cannot be resolved to a Domains aspect.
        if (afterDomains == null) {
          failures.add(
              authFailure(
                  item,
                  "Unauthorized to edit domains via PATCH on entity "
                      + urn
                      + " (could not resolve proposed domains from patch)"));
          continue;
        }
        if (!DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(
            session, urn, beforeDomains, afterDomains)) {
          failures.add(
              authFailure(
                  item,
                  "Unauthorized to edit domains on entity "
                      + urn
                      + " (domain-scoped Edit Entity policy does not allow before and/or after domains)"));
        }
        continue;
      }

      boolean itemProposesDomains = afterDomains != null;
      boolean useProposed =
          DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(
              exists, domainsAspectExists, itemProposesDomains);

      ApiOperation apiOperation =
          DomainWriteAuthorizationUtils.resolveApiOperation(item.getChangeType(), exists);

      boolean allowed =
          DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
              session, urn, apiOperation, useProposed, useProposed ? afterDomains : null);

      if (!allowed) {
        failures.add(
            authFailure(
                item,
                "Unauthorized to "
                    + (exists ? "edit" : "create")
                    + " domains on entity "
                    + urn
                    + (useProposed
                        ? " (proposed domain does not match domain-scoped write policy)"
                        : "")));
      }
    }
    return failures;
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspectsWithAuth(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    if (shouldSkipUserDomainAuth(session)) {
      return Stream.empty();
    }

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    List<ChangeMCP> domainsItems =
        changeMCPs.stream()
            .filter(item -> DOMAINS_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());
    if (domainsItems.isEmpty()) {
      return Stream.empty();
    }

    Set<Urn> urns = domainsItems.stream().map(ChangeMCP::getUrn).collect(Collectors.toSet());
    Map<Urn, Boolean> entityExists =
        DomainWriteAuthorizationUtils.resolveEntityExists(operationContext, aspectRetriever, urns);

    List<AspectValidationException> failures = new ArrayList<>();
    for (ChangeMCP item : domainsItems) {
      Urn urn = item.getUrn();
      Domains before = item.getPreviousAspect(Domains.class);
      Domains after = item.getAspect(Domains.class);

      if (DomainWriteAuthorizationUtils.hasDomainMembership(before)) {
        // Existing domains: before+after Edit reconciliation (PATCH moves and similar).
        if (after == null
            || !DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(
                session, urn, before, after)) {
          failures.add(
              authFailure(
                  item,
                  "Unauthorized to edit domains on entity "
                      + urn
                      + " (domain-scoped Edit Entity policy does not allow before and/or after domains)"));
        }
        continue;
      }

      // Create / first-domains establish: keep CREATE vs EDIT privilege selection from proposed.
      boolean exists = Boolean.TRUE.equals(entityExists.get(urn));
      ApiOperation apiOperation =
          DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.UPSERT, exists);
      boolean allowed =
          DomainWriteAuthorizationUtils.isAuthorizedEntityWrite(
              session, urn, apiOperation, true, after);
      if (!allowed) {
        failures.add(
            authFailure(
                item,
                "Unauthorized to "
                    + (exists ? "edit" : "create")
                    + " domains on entity "
                    + urn
                    + " (proposed domain does not match domain-scoped write policy)"));
      }
    }
    return failures.stream();
  }

  static boolean shouldSkipUserDomainAuth(@Nullable AuthorizationSession session) {
    if (session == null) {
      return true;
    }
    if (!(session instanceof OperationContext)) {
      return false;
    }
    OperationContext opContext = (OperationContext) session;
    // Async MCE re-processing has no request context; auth already ran on the API thread.
    if (opContext.getRequestContext() == null) {
      return true;
    }
    return opContext.isSystemAuth();
  }
}
