package com.linkedin.metadata.aspect.validation;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.util.Pair;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Implements conditional write semantics.
 *
 * <p>1. If an aspect change request includes a SystemMetadata section which includes a value for
 * nextAspectVersion, then it must match the current SystemMetadata's value for nextAspectVersion or
 * prevent performing the write operation.
 *
 * <p>2. Headers are included with the MCP for the following time-based checks * If-Modified-Since *
 * If-Unmodified-Since
 */
@Setter
@Getter
@Accessors(chain = true)
public class ConditionalWriteValidator extends AspectPayloadValidator {
  public static final String UNVERSIONED_ASPECT_VERSION = "-1";
  public static final long DEFAULT_LAST_MODIFIED_TIME = Long.MIN_VALUE;
  public static final String HTTP_HEADER_IF_VERSION_MATCH = "If-Version-Match";
  public static final Set<ChangeType> CREATE_CHANGE_TYPES =
      ImmutableSet.of(ChangeType.CREATE, ChangeType.CREATE_ENTITY);

  @Nonnull private AspectPluginConfig config;

  private static boolean hasTimePrecondition(ChangeMCP item) {
    return item.getHeader(HttpHeaders.IF_MODIFIED_SINCE).isPresent()
        || item.getHeader(HttpHeaders.IF_UNMODIFIED_SINCE).isPresent();
  }

  private static boolean hasVersionPrecondition(ChangeMCP item) {
    return item.getHeader(HTTP_HEADER_IF_VERSION_MATCH).isPresent();
  }

  private static boolean isApplicableFilter(ChangeMCP item) {
    if (ChangeType.RESTATE.equals(item.getChangeType())) {
      return false;
    }

    return hasTimePrecondition(item) || hasVersionPrecondition(item);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    List<ChangeMCP> applicableMCPs =
        changeMCPs.stream()
            .filter(ConditionalWriteValidator::isApplicableFilter)
            .collect(Collectors.toList());

    // Batch lookup missing data
    Map<Urn, Set<String>> missingDataUrnAspects =
        applicableMCPs.stream()
            // create change types not expected to have previous data
            .filter(item -> !CREATE_CHANGE_TYPES.contains(item.getChangeType()))
            .filter(item -> item.getPreviousSystemAspect() == null)
            .collect(
                Collectors.groupingBy(
                    ChangeMCP::getUrn,
                    Collectors.mapping(ChangeMCP::getAspectName, Collectors.toSet())));
    final Map<Urn, Map<String, SystemAspect>> resolvedData =
        aspectRetriever.getLatestSystemAspects(missingDataUrnAspects);

    for (ChangeMCP item : applicableMCPs) {
      // Validate aspect version precondition
      if (hasVersionPrecondition(item)) {
        item.getHeader(HTTP_HEADER_IF_VERSION_MATCH)
            .flatMap(
                headerValue ->
                    validateVersionPrecondition(
                        item, Pair.of(HTTP_HEADER_IF_VERSION_MATCH, headerValue), resolvedData))
            .ifPresent(exceptions::addException);
      }

      // Validate modified time conditional
      if (hasTimePrecondition(item)) {
        item.getHeader(HttpHeaders.IF_MODIFIED_SINCE)
            .flatMap(
                headerValue ->
                    validateTimePrecondition(
                        item, Pair.of(HttpHeaders.IF_MODIFIED_SINCE, headerValue), resolvedData))
            .ifPresent(exceptions::addException);
        item.getHeader(HttpHeaders.IF_UNMODIFIED_SINCE)
            .flatMap(
                headerValue ->
                    validateTimePrecondition(
                        item, Pair.of(HttpHeaders.IF_UNMODIFIED_SINCE, headerValue), resolvedData))
            .ifPresent(exceptions::addException);
      }
    }

    return exceptions.streamAllExceptions();
  }

  private static Optional<AspectValidationException> validateVersionPrecondition(
      ChangeMCP item,
      Pair<String, String> header,
      Map<Urn, Map<String, SystemAspect>> resolvedData) {
    final String actualAspectVersion;
    switch (item.getChangeType()) {
      case CREATE:
      case CREATE_ENTITY:
        actualAspectVersion = UNVERSIONED_ASPECT_VERSION;
        break;
      default:
        actualAspectVersion =
            resolvePreviousSystemAspect(item, resolvedData)
                .map(
                    prevSystemAspect -> {
                      if (prevSystemAspect.getSystemMetadataVersion().isPresent()) {
                        return String.valueOf(prevSystemAspect.getSystemMetadataVersion().get());
                      } else {
                        return String.valueOf(Math.max(1, prevSystemAspect.getVersion()));
                      }
                    })
                .orElse(UNVERSIONED_ASPECT_VERSION);
        break;
    }

    if (!header.getSecond().equals(actualAspectVersion)) {
      return Optional.of(
          AspectValidationException.forPrecondition(
              item,
              String.format(
                  "Expected version %s, actual version %s",
                  header.getSecond(), actualAspectVersion)));
    }

    return Optional.empty();
  }

  private static Optional<AspectValidationException> validateTimePrecondition(
      ChangeMCP item,
      Pair<String, String> header,
      Map<Urn, Map<String, SystemAspect>> resolvedData) {

    final long lastModifiedTimeMs;
    switch (item.getChangeType()) {
      case CREATE:
      case CREATE_ENTITY:
        lastModifiedTimeMs = DEFAULT_LAST_MODIFIED_TIME;
        break;
      default:
        lastModifiedTimeMs =
            resolvePreviousSystemAspect(item, resolvedData)
                .map(systemAspect -> systemAspect.getAuditStamp().getTime())
                .orElse(DEFAULT_LAST_MODIFIED_TIME);
        break;
    }

    long headerValueEpochMs = Instant.parse(header.getValue()).toEpochMilli();

    switch (header.getKey()) {
      case HttpHeaders.IF_MODIFIED_SINCE:
        return lastModifiedTimeMs > headerValueEpochMs
            ? Optional.empty()
            : Optional.of(
                AspectValidationException.forPrecondition(
                    item,
                    String.format(
                        "Item last modified %s <= %s (epoch ms)",
                        lastModifiedTimeMs, headerValueEpochMs)));
      case HttpHeaders.IF_UNMODIFIED_SINCE:
        return lastModifiedTimeMs <= headerValueEpochMs
            ? Optional.empty()
            : Optional.of(
                AspectValidationException.forPrecondition(
                    item,
                    String.format(
                        "Item last modified %s > %s (epoch ms)",
                        lastModifiedTimeMs, headerValueEpochMs)));
      default:
        return Optional.empty();
    }
  }

  private static Optional<SystemAspect> resolvePreviousSystemAspect(
      ChangeMCP item, Map<Urn, Map<String, SystemAspect>> resolvedData) {
    if (item.getPreviousSystemAspect() != null) {
      return Optional.of(item.getPreviousSystemAspect());
    }
    if (resolvedData.getOrDefault(item.getUrn(), Collections.emptyMap()).get(item.getAspectName())
        != null) {
      return Optional.of(resolvedData.get(item.getUrn()).get(item.getAspectName()));
    }
    return Optional.empty();
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
