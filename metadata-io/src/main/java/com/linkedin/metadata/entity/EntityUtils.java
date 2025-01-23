package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.*;

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
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.validation.RecordTemplateValidator;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityUtils {

  private EntityUtils() {}

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
            .build(),
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
      RecordTemplate aspect = entityService.getAspect(opContext, urn, aspectName, 0);
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
   * @param entityAspect optional entity aspect
   * @param retrieverContext
   * @return
   */
  public static Optional<SystemAspect> toSystemAspect(
      @Nonnull RetrieverContext retrieverContext, @Nullable EntityAspect entityAspect) {
    return Optional.ofNullable(entityAspect)
        .map(aspect -> toSystemAspects(retrieverContext, List.of(aspect)))
        .filter(systemAspects -> !systemAspects.isEmpty())
        .map(systemAspects -> systemAspects.get(0));
  }

  /**
   * Given a `Map<EntityUrn, <Map<AspectName, EntityAspect>>` from the database representation,
   * translate that into our java classes
   *
   * @param rawAspects `Map<EntityUrn, <Map<AspectName, EntityAspect>>`
   * @param retrieverContext used for read mutations
   * @return the java map for the given database object map
   */
  @Nonnull
  public static Map<String, Map<String, SystemAspect>> toSystemAspects(
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull Map<String, Map<String, EntityAspect>> rawAspects) {
    List<SystemAspect> systemAspects =
        toSystemAspects(
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
      @Nonnull RetrieverContext retrieverContext, @Nonnull Collection<EbeanAspectV2> rawAspects) {
    return toSystemAspects(
        retrieverContext,
        rawAspects.stream().map(EbeanAspectV2::toEntityAspect).collect(Collectors.toList()));
  }

  /**
   * Convert EntityAspect to EntitySystemAspect
   *
   * <p>This should be the 1 point that all conversions from database representations to java
   * objects happens since we need to enforce read mutations happen.
   *
   * @param rawAspects raw aspects to convert
   * @return map converted aspects
   */
  @Nonnull
  public static List<SystemAspect> toSystemAspects(
      @Nonnull final RetrieverContext retrieverContext,
      @Nonnull Collection<EntityAspect> rawAspects) {
    EntityRegistry entityRegistry = retrieverContext.getAspectRetriever().getEntityRegistry();

    // Build
    List<SystemAspect> systemAspects =
        rawAspects.stream()
            .map(
                raw -> {
                  Urn urn = UrnUtils.getUrn(raw.getUrn());
                  AspectSpec aspectSpec =
                      entityRegistry
                          .getEntitySpec(urn.getEntityType())
                          .getAspectSpec(raw.getAspect());

                  // TODO: aspectSpec can be null here
                  Preconditions.checkState(
                      aspectSpec != null,
                      String.format("Aspect %s could not be found", raw.getAspect()));

                  return EntityAspect.EntitySystemAspect.builder()
                      .build(entityRegistry.getEntitySpec(urn.getEntityType()), aspectSpec, raw);
                })
            .collect(Collectors.toList());

    // Read Mutate
    Map<Pair<EntitySpec, AspectSpec>, List<ReadItem>> grouped =
        systemAspects.stream()
            .collect(
                Collectors.groupingBy(item -> Pair.of(item.getEntitySpec(), item.getAspectSpec())));

    grouped.forEach(
        (key, value) -> {
          AspectsBatch.applyReadMutationHooks(value, retrieverContext);
        });

    // Read Validate
    systemAspects.forEach(
        systemAspect ->
            RecordTemplateValidator.validate(
                systemAspect.getRecordTemplate(),
                validationFailure ->
                    log.warn(
                        String.format(
                            "Failed to validate record %s against its schema.",
                            systemAspect.getRecordTemplate()))));

    // TODO consider applying write validation plugins

    return systemAspects;
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
  public static Map<String, Map<String, Long>> calculateNextVersions(
      TransactionContext txContext,
      AspectDao aspectDao,
      Map<String, Map<String, SystemAspect>> latestAspects,
      Map<String, Set<String>> urnAspects) {

    final Map<String, Map<String, Long>> precalculatedVersions;
    final Map<String, Set<String>> missingAspectVersions;
    if (txContext.getFailedAttempts() > 2 && txContext.lastExceptionIsDuplicateKey()) {
      log.warn(
          "Multiple exceptions detected, last exception detected as DuplicateKey, fallback to database max(version)+1");
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
            : aspectDao.getNextVersions(missingAspectVersions);

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
  private static Map<String, Long> convertSystemAspectToNextVersionMap(
      Map<String, SystemAspect> aspectMap) {
    return aspectMap.entrySet().stream()
        .filter(entry -> entry.getValue().getVersion() == 0)
        .map(entry -> Map.entry(entry.getKey(), entry.getValue().getSystemMetadataVersion()))
        .filter(entry -> entry.getValue().isPresent())
        .map(entry -> Map.entry(entry.getKey(), entry.getValue().get()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
