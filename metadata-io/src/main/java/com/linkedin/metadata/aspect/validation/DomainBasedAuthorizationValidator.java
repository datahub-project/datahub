package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validator that performs domain-based authorization checks inside the transaction.
 * This ensures that domain reads from the database are consistent with the transaction state,
 * preventing race conditions where domains could change between authorization check and commit.
 *
 * <p>This validator:
 * 1. Reads domains from incoming MCPs (lightweight parsing - outside transaction)
 * 2. Reads existing entity domains from database (inside transaction via aspectRetriever)
 * 3. Performs domain-based authorization check
 * 4. Throws validation exception if unauthorized
 */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class DomainBasedAuthorizationValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    // Validation happens in validatePreCommitAspects (inside transaction)
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {

    AuthorizationSession session = retrieverContext.getAuthorizationSession();

    if (session == null) {
      log.warn("DomainBasedAuthorizationValidator: No authentication session provided");
      return Stream.of(
          AspectValidationException.forAuth(
              changeMCPs.stream().findFirst().orElse(null),
              "No authentication details found, cannot authorize change."));
    }

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    // Group changes by entity URN
    Map<Urn, List<ChangeMCP>> changesByEntity =
        changeMCPs.stream()
            .filter(
                changeMCP ->
                    !EXECUTION_REQUEST_ENTITY_NAME.equals(changeMCP.getUrn().getEntityType()))
            .collect(Collectors.groupingBy(ChangeMCP::getUrn));

    return changesByEntity.entrySet().stream()
        .flatMap(
            entry -> {
              Urn entityUrn = entry.getKey();
              List<ChangeMCP> entityChanges = entry.getValue();

              // Determine the operation type first
              ApiOperation operation =
                  entityChanges.stream()
                      .map(changeMCP -> ApiOperation.fromChangeType(changeMCP.getChangeType()))
                      .findFirst()
                      .orElse(ApiOperation.UPDATE);

              // Extract domains based on operation type
              Set<Urn> allDomains = new HashSet<>();

              if (operation == ApiOperation.DELETE) {
                allDomains.addAll(getEntityDomains(entityUrn, aspectRetriever));
              } else {
                Set<Urn> domainsFromMCPs = getDomainsFromMCPs(entityChanges);
                Set<Urn> domainsFromDB = getEntityDomains(entityUrn, aspectRetriever);
                allDomains.addAll(domainsFromMCPs);
                allDomains.addAll(domainsFromDB);
              }

              boolean isAuthorized = AuthUtil.isAPIAuthorizedEntityUrnsWithSubResources(
                  session, operation, List.of(entityUrn), allDomains);

              if (!isAuthorized) {
                log.warn("DomainBasedAuthorizationValidator: BLOCKED - Unauthorized to {} entity {} with domains {}",
                    operation, entityUrn, allDomains);
                return Stream.of(
                    AspectValidationException.forAuth(
                        entityChanges.get(0),
                        String.format(
                            "Unauthorized to %s entity %s with domains %s",
                            operation,
                            entityUrn,
                            allDomains)));
              }

              return Stream.empty();
            });
  }

  /**
   * Extract domains from MCPs
   */
  private Set<Urn> getDomainsFromMCPs(Collection<ChangeMCP> changeMCPs) {
    return changeMCPs.stream()
        .filter(changeMCP -> DOMAINS_ASPECT_NAME.equals(changeMCP.getAspectName()))
        .flatMap(
            changeMCP -> {
              try {
                Domains domains = changeMCP.getAspect(Domains.class);
                if (domains != null && domains.getDomains() != null) {
                  return domains.getDomains().stream();
                }
              } catch (Exception e) {
                log.warn(
                    "Failed to extract domains from MCP for entity {}: {}",
                    changeMCP.getUrn(),
                    e.getMessage());
              }
              return Stream.empty();
            })
        .collect(Collectors.toSet());
  }

  private Set<Urn> getEntityDomains(Urn entityUrn, AspectRetriever aspectRetriever) {
    try {
      Aspect domainsAspect = aspectRetriever.getLatestAspectObject(entityUrn, DOMAINS_ASPECT_NAME);
      if (domainsAspect != null) {
        Domains domains = RecordUtils.toRecordTemplate(Domains.class, domainsAspect.data());
        if (domains.getDomains() != null) {
          return new HashSet<>(domains.getDomains());
        }
      }
    } catch (Exception e) {
      log.warn("Failed to retrieve domains for entity {}: {}", entityUrn, e.getMessage());
    }
    return Collections.emptySet();
  }
}