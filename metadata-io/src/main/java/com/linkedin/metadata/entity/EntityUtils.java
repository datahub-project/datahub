package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.google.common.base.Preconditions;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.RecordTemplateValidator;
import com.linkedin.metadata.utils.UnknownDataGuard;
import com.linkedin.metadata.utils.UnknownEntityUrnStripper;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityUtils {
  private static final UnknownDataGuard UNKNOWN_DATA =
      UnknownDataGuard.forSite(EntityUtils.class, "stored aspect of");
  private static final UnknownDataGuard UNKNOWN_REFERENCES =
      UnknownDataGuard.forSite(EntityUtils.class, "references to unknown entity types in");

  private EntityUtils() {}

  /**
   * Whether a side-effect DELETE of {@code aspectName} for {@code urn} should use hardDelete.
   *
   * <p>Key-aspect DELETE must hard-delete ({@code deleteUrn}). Soft-delete only sets {@code
   * status.removed} and leaves the key; a later status DELETE in the same cascade can remove that
   * flag and leave the entity looking active. Non-key aspect deletes stay soft.
   */
  public static boolean shouldHardDeleteAspect(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String aspectName) {
    return aspectName.equals(opContext.getKeyAspectName(urn));
  }

  @Nullable
  public static Urn getUrnFromString(String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  @Nonnull
  public static AuditStamp getAuditStamp(Urn actor) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(actor);
    return auditStamp;
  }

  public static void ingestChangeProposals(
      @Nonnull OperationContext opContext,
      @Nonnull List<MetadataChangeProposal> changes,
      @Nonnull EntityService<?> entityService,
      @Nonnull Urn actor,
      @Nonnull Boolean async) {
    entityService.ingestProposal(
        opContext,
        AspectsBatchImpl.builder()
            .mcps(changes, getAuditStamp(actor), opContext.getRetrieverContext())
            .build(opContext),
        async);
  }

  /**
   * Get aspect from entity
   *
   * @param entityUrn URN of the entity
   * @param aspectName aspect name string
   * @param entityService EntityService obj
   * @param defaultValue default value if null is found
   * @return a record template of the aspect
   */
  @Nullable
  public static RecordTemplate getAspectFromEntity(
      @Nonnull OperationContext opContext,
      String entityUrn,
      String aspectName,
      EntityService<?> entityService,
      RecordTemplate defaultValue) {
    Urn urn = getUrnFromString(entityUrn);
    if (urn == null) {
      return defaultValue;
    }
    try {
      // Callers read this aspect in order to write it back. A replica snapshot would clobber a
      // newer primary row.
      RecordTemplate aspect =
          entityService.getAspect(
              opContext.withReadPreference(ReadPreference.PRIMARY), urn, aspectName, 0);
      if (aspect == null) {
        return defaultValue;
      }
      return aspect;
    } catch (Exception e) {
      log.error(
          "Error constructing aspect from entity. Entity: {} aspect: {}. Error: {}",
          entityUrn,
          aspectName,
          e.toString());
      return null;
    }
  }

  static Entity toEntity(@Nonnull final Snapshot snapshot) {
    return new Entity().setValue(snapshot);
  }

  static Snapshot toSnapshotUnion(@Nonnull final RecordTemplate snapshotRecord) {
    final Snapshot snapshot = new Snapshot();
    RecordUtils.setSelectedRecordTemplateInUnion(snapshot, snapshotRecord);
    return snapshot;
  }

  static EnvelopedAspect getKeyEnvelopedAspect(final Urn urn, final EntityRegistry entityRegistry) {
    final EntitySpec spec = entityRegistry.getEntitySpec(PegasusUtils.urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    final com.linkedin.entity.Aspect aspect =
        new com.linkedin.entity.Aspect(EntityKeyUtils.convertUrnToEntityKey(urn, keySpec).data());

    final EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(keySpec.getName());
    envelopedAspect.setVersion(ASPECT_LATEST_VERSION);
    envelopedAspect.setValue(aspect);
    // TODO: I think we can assume this here, adding as it's a required field so object mapping
    // barfs when trying to access it,
    //    since nowhere else is using it should be safe for now at least
    envelopedAspect.setType(AspectType.VERSIONED);
    envelopedAspect.setCreated(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()));

    return envelopedAspect;
  }

  static EntityResponse toEntityResponse(
      final Urn urn, final List<EnvelopedAspect> envelopedAspects) {
    final EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(PegasusUtils.urnToEntityName(urn));
    response.setAspects(
        new EnvelopedAspectMap(
            envelopedAspects.stream()
                .collect(Collectors.toMap(EnvelopedAspect::getName, aspect -> aspect))));
    return response;
  }

  /**
   * Prefer batched interfaces
   *
   * @param operationContext operation context with fingerprint
   * @param entityAspect optional entity aspect
   * @param retrieverContext
   * @return
   */
  public static Optional<SystemAspect> toSystemAspect(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable EntityAspect entityAspect) {
    return Optional.ofNullable(entityAspect)
        .map(aspect -> toSystemAspects(operationContext, retrieverContext, List.of(aspect)))
        .filter(systemAspects -> !systemAspects.isEmpty())
        .map(systemAspects -> systemAspects.get(0));
  }

  /**
   * Given a `Map<EntityUrn, <Map<AspectName, EntityAspect>>` from the database representation,
   * translate that into our java classes
   *
   * @param operationContext operation context with fingerprint
   * @param rawAspects `Map<EntityUrn, <Map<AspectName, EntityAspect>>`
   * @param retrieverContext used for read mutations
   * @return the java map for the given database object map
   */
  @Nonnull
  public static Map<String, Map<String, SystemAspect>> toSystemAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull Map<String, Map<String, EntityAspect>> rawAspects) {
    List<SystemAspect> systemAspects =
        toSystemAspects(
            operationContext,
            retrieverContext,
            rawAspects.values().stream()
                .flatMap(m -> m.values().stream())
                .collect(Collectors.toList()));

    // map the list into the desired shape
    return systemAspects.stream()
        .collect(Collectors.groupingBy(SystemAspect::getUrn))
        .entrySet()
        .stream()
        .map(
            entry ->
                Pair.of(
                    entry.getKey(),
                    entry.getValue().stream()
                        .collect(Collectors.groupingBy(SystemAspect::getAspectName))))
        .collect(
            Collectors.toMap(
                p -> p.getFirst().toString(),
                p ->
                    p.getSecond().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)))));
  }

  @Nonnull
  public static List<SystemAspect> toSystemAspectFromEbeanAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull Collection<EbeanAspectV2> rawAspects) {
    return toSystemAspects(
        operationContext,
        retrieverContext,
        rawAspects.stream().map(EbeanAspectV2::toEntityAspect).collect(Collectors.toList()));
  }

  /**
   * Convert EntityAspect to EntitySystemAspect
   *
   * <p>This should be the 1 point that all conversions from database representations to java
   * objects happens since we need to enforce read mutations happen.
   *
   * @param operationContext operation context with fingerprint
   * @param retrieverContext retriever context to fetch with
   * @param rawAspects raw aspects to convert
   * @return map converted aspects
   */
  @Nonnull
  public static List<SystemAspect> toSystemAspects(
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext,
      @Nonnull Collection<EntityAspect> rawAspects) {
    EntityRegistry entityRegistry = retrieverContext.getAspectRetriever().getEntityRegistry();
    final Optional<MetricUtils> metrics =
        operationContext instanceof OperationContext opContext
            ? opContext.getMetricUtils()
            : Optional.empty();

    // Build — skip rows whose entity or aspect is not in the current registry
    // (can happen after a rollback when the older build encounters rows written by the newer one)
    List<SystemAspect> systemAspects =
        rawAspects.stream()
            .map(
                raw -> {
                  Urn urn = UrnUtils.getUrn(raw.getUrn());

                  // Also skips urns whose key references an unknown entity type (e.g. a schema
                  // field of an entity type only a newer version has).
                  if (!handling()
                      .admitEvent(
                          UNKNOWN_DATA,
                          entityRegistry,
                          metrics,
                          urn,
                          urn.getEntityType(),
                          raw.getAspect())) {
                    return null;
                  }
                  if (!handling().toleratesUnknownEntityAspect()) {
                    // Strict handling fails on an unknown entity type or aspect, as before.
                    final AspectSpec aspectSpec =
                        handling()
                            .entitySpecForData(entityRegistry, urn.getEntityType())
                            .map(entitySpec -> entitySpec.getAspectSpec(raw.getAspect()))
                            .orElse(null);
                    Preconditions.checkState(
                        aspectSpec != null,
                        String.format("Aspect %s could not be found", raw.getAspect()));
                  }

                  try {
                    return EntityAspect.EntitySystemAspect.builder().forUpdate(raw, entityRegistry);
                  } catch (RuntimeException conversionFailure) {
                    // One row a newer version wrote must not fail the whole read; any other
                    // conversion failure is still an error.
                    if (!handling().toleratesUnknownData()
                        || !isWrittenByNewerSchema(raw, urn, entityRegistry)) {
                      throw conversionFailure;
                    }
                    UNKNOWN_DATA.skippedBecause(
                        metrics,
                        urn.getEntityType() + "/" + raw.getAspect() + "/unreadable",
                        "it was written under a newer schema and can't be read here: "
                            + conversionFailure.getMessage(),
                        urn);
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // Read Strip: references to entity types this registry doesn't know (written by a newer version
    // before a rollback) are removed on read, so no reader (GraphQL, OpenAPI, Rest.li, hooks,
    // read-modify-write) sees them. Writes keep the stored ones (see mergeStoredReferences).
    systemAspects.forEach(
        systemAspect -> {
          if (systemAspect.getRecordTemplate() != null) {
            final UnknownEntityUrnStripper.StripResult stripped =
                handling().stripReporting(systemAspect.getRecordTemplate(), entityRegistry);
            // Decided on the stored value, before the strip removes what would show it, so steps
            // that can't validate the aspect can tell data from a newer version from a real error.
            if (systemAspect instanceof EntityAspect.EntitySystemAspect entitySystemAspect) {
              entitySystemAspect.setFromNewerVersion(
                  stripped.referencedUnknownEntityType()
                      || handling()
                          .isWrittenByNewerSchema(
                              systemAspect.getSystemMetadata(), systemAspect.getAspectSpec()));
            }
            final int removed = stripped.removed();
            if (removed > 0) {
              UNKNOWN_REFERENCES.skippedBecause(
                  metrics,
                  systemAspect.getAspectName(),
                  removed + " removed on read from " + systemAspect.getAspectName(),
                  systemAspect.getUrn());
            }
          }
        });

    // Read Mutate
    Map<Pair<EntitySpec, AspectSpec>, List<ReadItem>> grouped =
        systemAspects.stream()
            .collect(
                Collectors.groupingBy(item -> Pair.of(item.getEntitySpec(), item.getAspectSpec())));

    grouped.forEach(
        (key, value) -> {
          AspectsBatch.applyReadMutationHooks(operationContext, value, retrieverContext);
        });

    // Read Validate
    systemAspects.forEach(
        systemAspect ->
            RecordTemplateValidator.validateTrim(
                systemAspect.getRecordTemplate(),
                validationFailure -> {
                  // Enum symbols or union members a newer version added don't bump the schema
                  // version, so this failure is what shows the aspect came from that version.
                  if (systemAspect instanceof EntityAspect.EntitySystemAspect entitySystemAspect
                      && handling().failsOnlyOnValuesFromNewerVersion(validationFailure)) {
                    entitySystemAspect.setFromNewerVersion(true);
                  }
                  if (handling().toleratesUnknownData()) {
                    // After a rollback, every read of an aspect a newer version wrote can fail
                    // here: log it once a minute per aspect, without the record.
                    UNKNOWN_DATA.invalid(
                        systemAspect.getAspectName(),
                        systemAspect.getAspectName()
                            + " against its schema: "
                            + validationFailure.getMessages(),
                        systemAspect.getUrn());
                  } else {
                    log.warn(
                        String.format(
                            "Failed to validate record %s against its schema.",
                            systemAspect.getRecordTemplate()));
                  }
                }));

    // TODO consider applying write validation plugins

    return systemAspects;
  }

  /** True when the stored row's system metadata carries a newer schema version than its spec. */
  private static boolean isWrittenByNewerSchema(
      @Nonnull final EntityAspect raw,
      @Nonnull final Urn urn,
      @Nonnull final EntityRegistry entityRegistry) {
    if (raw.getSystemMetadata() == null) {
      return false;
    }
    try {
      final SystemMetadata systemMetadata =
          RecordUtils.toRecordTemplate(SystemMetadata.class, raw.getSystemMetadata());
      return handling()
          .isWrittenByNewerSchema(
              systemMetadata,
              entityRegistry.findAspectSpec(urn.getEntityType(), raw.getAspect()).orElse(null));
    } catch (RuntimeException unreadableSystemMetadata) {
      return false;
    }
  }

  /**
   * Use the precalculated next version from system metadata if it exists, otherwise lookup the next
   * version the normal way from the database
   *
   * @param txContext
   * @param aspectDao database access
   * @param latestAspects aspect version 0 with system metadata
   * @param urnAspects urn/aspects which we need next version information for
   * @return map of the urn/aspect to the next aspect version
   */
  public static <T extends SystemAspect> Map<String, Map<String, Long>> calculateNextVersions(
      @Nonnull OperationContext opContext,
      TransactionContext txContext,
      AspectDao aspectDao,
      Map<String, Map<String, T>> latestAspects,
      Map<String, Set<String>> urnAspects) {

    final Map<String, Map<String, Long>> precalculatedVersions;
    final Map<String, Set<String>> missingAspectVersions;
    if (txContext.shouldFallbackToDatabaseMaxVersion()) {
      log.warn(
          "Retry failures reached fallback threshold and last error was DuplicateKey ({}); using database max(version)+1",
          TransactionContext.DUPLICATE_KEY_MAX_VERSION_FALLBACK_AFTER_FAILURES);
      precalculatedVersions = Map.of();
      missingAspectVersions = urnAspects;
    } else {
      precalculatedVersions =
          latestAspects.entrySet().stream()
              .map(
                  entry ->
                      Map.entry(
                          entry.getKey(), convertSystemAspectToNextVersionMap(entry.getValue())))
              .filter(entry -> !entry.getValue().isEmpty())
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      missingAspectVersions =
          urnAspects.entrySet().stream()
              .flatMap(
                  entry ->
                      entry.getValue().stream()
                          .map(aspectName -> Pair.of(entry.getKey(), aspectName)))
              .filter(
                  urnAspectName ->
                      !precalculatedVersions
                          .getOrDefault(urnAspectName.getKey(), Map.of())
                          .containsKey(urnAspectName.getValue()))
              .collect(
                  Collectors.groupingBy(
                      Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toSet())));
    }

    Map<String, Map<String, Long>> databaseVersions =
        missingAspectVersions.isEmpty()
            ? Map.of()
            : aspectDao.getNextVersions(opContext, txContext, missingAspectVersions, true);

    // stitch back together the precalculated and database versions
    return Stream.concat(
            precalculatedVersions.entrySet().stream(), databaseVersions.entrySet().stream())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (m1, m2) ->
                    Stream.concat(m1.entrySet().stream(), m2.entrySet().stream())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
  }

  /**
   * Given a map of aspect name to system aspect, extract the next version if it exists
   *
   * @param aspectMap aspect name to system aspect map
   * @return aspect name to next aspect version
   */
  private static <T extends SystemAspect> Map<String, Long> convertSystemAspectToNextVersionMap(
      Map<String, T> aspectMap) {
    return aspectMap.entrySet().stream()
        .filter(entry -> entry.getValue().getVersion() == 0)
        .map(entry -> Map.entry(entry.getKey(), entry.getValue().getSystemMetadataVersion()))
        .filter(entry -> entry.getValue().isPresent())
        .map(entry -> Map.entry(entry.getKey(), entry.getValue().get()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
