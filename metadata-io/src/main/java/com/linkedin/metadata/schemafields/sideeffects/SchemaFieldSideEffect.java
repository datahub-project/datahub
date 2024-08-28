package com.linkedin.metadata.schemafields.sideeffects;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ALIASES_ASPECT;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_KEY_ASPECT;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.key.SchemaFieldKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.eventgenerator.ChangeEventGeneratorUtils;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schemafield.SchemaFieldAliases;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class SchemaFieldSideEffect extends MCPSideEffect {
  @Nonnull private AspectPluginConfig config;
  @Nonnull private EntityChangeEventGeneratorRegistry entityChangeEventGeneratorRegistry;

  private static final Set<String> REQUIRED_ASPECTS =
      Set.of(SCHEMA_METADATA_ASPECT_NAME, STATUS_ASPECT_NAME);

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  /**
   * Handle delete of dataset schema metadata
   *
   * @param mclItems MCL items generated from committing the MCP
   * @param retrieverContext accessors for aspect and graph data
   * @return
   */
  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {

    // fetch existing aspects, if not already in the batch just committed
    Map<Urn, Map<String, Aspect>> aspectData =
        fetchRequiredAspects(mclItems, REQUIRED_ASPECTS, retrieverContext.getAspectRetriever());

    return Stream.concat(
        processUpserts(mclItems, aspectData, retrieverContext),
        processDelete(mclItems, aspectData, retrieverContext));
  }

  private static Stream<MCPItem> processDelete(
      Collection<MCLItem> mclItems,
      Map<Urn, Map<String, Aspect>> aspectData,
      @Nonnull RetrieverContext retrieverContext) {

    List<MCLItem> schemaMetadataDeletes =
        mclItems.stream()
            .filter(
                item ->
                    ChangeType.DELETE.equals(item.getChangeType())
                        && DATASET_ENTITY_NAME.equals(item.getUrn().getEntityType())
                        && SCHEMA_METADATA_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());

    Stream<MCPItem> schemaMetadataSchemaFieldMCPs =
        schemaMetadataDeletes.stream()
            .flatMap(
                item ->
                    buildSchemaMetadataSchemaFieldDeleteMCPs(
                        item, retrieverContext.getAspectRetriever()));

    List<MCLItem> statusDeletes =
        mclItems.stream()
            .filter(
                item ->
                    ChangeType.DELETE.equals(item.getChangeType())
                        && DATASET_ENTITY_NAME.equals(item.getUrn().getEntityType())
                        && STATUS_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());

    final Stream<MCPItem> statusSchemaFieldMCPs;
    if (statusDeletes.isEmpty()) {
      statusSchemaFieldMCPs = Stream.empty();
    } else {
      statusSchemaFieldMCPs =
          statusDeletes.stream()
              .flatMap(
                  item ->
                      buildStatusSchemaFieldDeleteMCPs(
                          item,
                          Optional.ofNullable(
                                  aspectData
                                      .getOrDefault(item.getUrn(), Map.of())
                                      .get(SCHEMA_METADATA_ASPECT_NAME))
                              .map(aspect -> new SchemaMetadata(aspect.data()))
                              .orElse(null),
                          retrieverContext.getAspectRetriever()));
    }

    return Stream.concat(schemaMetadataSchemaFieldMCPs, statusSchemaFieldMCPs);
  }

  private Stream<ChangeMCP> processUpserts(
      Collection<MCLItem> mclItems,
      Map<Urn, Map<String, Aspect>> aspectData,
      @Nonnull RetrieverContext retrieverContext) {

    Map<Urn, Set<String>> batchMCPAspectNames =
        mclItems.stream()
            .filter(item -> item.getChangeType() != ChangeType.DELETE)
            .collect(
                Collectors.groupingBy(
                    MCLItem::getUrn,
                    Collectors.mapping(MCLItem::getAspectName, Collectors.toSet())));

    Stream<ChangeMCP> schemaFieldSideEffects =
        mclItems.stream()
            .filter(
                changeMCP ->
                    changeMCP.getChangeType() != ChangeType.DELETE
                        && DATASET_ENTITY_NAME.equals(changeMCP.getUrn().getEntityType())
                        && Constants.SCHEMA_METADATA_ASPECT_NAME.equals(changeMCP.getAspectName()))
            .flatMap(
                // Build schemaField Keys and Aliases
                item ->
                    optimizedKeyAspectMCPsConcat(
                        buildSchemaFieldKeyMCPs(
                            item, aspectData, retrieverContext.getAspectRetriever()),
                        buildSchemaFieldAliasesMCPs(item, retrieverContext.getAspectRetriever())));

    Stream<ChangeMCP> statusSideEffects =
        mclItems.stream()
            .filter(
                changeMCP ->
                    changeMCP.getChangeType() != ChangeType.DELETE
                        && DATASET_ENTITY_NAME.equals(changeMCP.getUrn().getEntityType())
                        && Constants.STATUS_ASPECT_NAME.equals(changeMCP.getAspectName())
                        // if present, already computed above
                        && !batchMCPAspectNames
                            .getOrDefault(changeMCP.getUrn(), Set.of())
                            .contains(Constants.SCHEMA_METADATA_ASPECT_NAME))
            .flatMap(
                item ->
                    mirrorStatusAspect(item, aspectData, retrieverContext.getAspectRetriever()));

    Stream<ChangeMCP> removedFieldStatusSideEffects =
        mclItems.stream()
            .filter(
                changeMCP ->
                    changeMCP.getChangeType() != ChangeType.DELETE
                        && DATASET_ENTITY_NAME.equals(changeMCP.getUrn().getEntityType())
                        && SCHEMA_METADATA_ASPECT_NAME.equals(changeMCP.getAspectName())
                        && changeMCP.getPreviousRecordTemplate() != null)
            .flatMap(
                item ->
                    buildRemovedSchemaFieldStatusAspect(
                        item, retrieverContext.getAspectRetriever()));

    return optimizedKeyAspectMCPsConcat(
        Stream.concat(schemaFieldSideEffects, statusSideEffects), removedFieldStatusSideEffects);
  }

  /**
   * Copy aspect from dataset to schema fields
   *
   * @param parentDatasetStatusItem dataset mcp item
   * @param aspectRetriever aspectRetriever context
   * @return side effect schema field aspects
   */
  private static Stream<ChangeMCP> mirrorStatusAspect(
      BatchItem parentDatasetStatusItem,
      Map<Urn, Map<String, Aspect>> aspectData,
      @Nonnull AspectRetriever aspectRetriever) {

    SchemaMetadata schemaMetadata =
        Optional.ofNullable(
                aspectData
                    .getOrDefault(parentDatasetStatusItem.getUrn(), Map.of())
                    .getOrDefault(Constants.SCHEMA_METADATA_ASPECT_NAME, null))
            .map(aspect -> new SchemaMetadata(aspect.data()))
            .orElse(null);

    if (schemaMetadata != null) {
      return schemaMetadata.getFields().stream()
          .map(
              schemaField -> {
                Boolean removed =
                    parentDatasetStatusItem.getRecordTemplate() != null
                        ? parentDatasetStatusItem.getAspect(Status.class).isRemoved()
                        : null;
                return buildSchemaFieldStatusMCPs(
                    SchemaFieldUtils.generateSchemaFieldUrn(
                        parentDatasetStatusItem.getUrn(), schemaField),
                    removed,
                    parentDatasetStatusItem.getAuditStamp(),
                    parentDatasetStatusItem.getSystemMetadata(),
                    aspectRetriever);
              });
    }

    return Stream.empty();
  }

  /**
   * Build a schema field status MCP based on the input status from the dataset
   *
   * @param schemaFieldUrn schema fields's urn
   * @param removed removed status
   * @param datasetAuditStamp origin audit stamp
   * @param datasetSystemMetadata origin system metadata
   * @param aspectRetriever aspect retriever
   * @return stream of status aspects for schema fields
   */
  public static ChangeMCP buildSchemaFieldStatusMCPs(
      Urn schemaFieldUrn,
      @Nullable Boolean removed,
      AuditStamp datasetAuditStamp,
      SystemMetadata datasetSystemMetadata,
      @Nonnull AspectRetriever aspectRetriever) {
    return ChangeItemImpl.builder()
        .urn(schemaFieldUrn)
        .changeType(ChangeType.UPSERT)
        .aspectName(STATUS_ASPECT_NAME)
        .recordTemplate(new Status().setRemoved(removed != null ? removed : false))
        .auditStamp(datasetAuditStamp)
        .systemMetadata(datasetSystemMetadata)
        .build(aspectRetriever);
  }

  private Stream<ChangeMCP> buildRemovedSchemaFieldStatusAspect(
      MCLItem parentDatasetSchemaMetadataItem, @Nonnull AspectRetriever aspectRetriever) {

    List<ChangeEvent> changeEvents =
        ChangeEventGeneratorUtils.generateChangeEvents(
            entityChangeEventGeneratorRegistry,
            parentDatasetSchemaMetadataItem.getUrn(),
            parentDatasetSchemaMetadataItem.getEntitySpec().getName(),
            parentDatasetSchemaMetadataItem.getAspectName(),
            new com.linkedin.metadata.timeline.eventgenerator.Aspect<>(
                parentDatasetSchemaMetadataItem.getPreviousAspect(SchemaMetadata.class),
                parentDatasetSchemaMetadataItem.getPreviousSystemMetadata()),
            new com.linkedin.metadata.timeline.eventgenerator.Aspect<>(
                parentDatasetSchemaMetadataItem.getAspect(SchemaMetadata.class),
                parentDatasetSchemaMetadataItem.getSystemMetadata()),
            parentDatasetSchemaMetadataItem.getAuditStamp());

    return changeEvents.stream()
        .flatMap(
            changeEvent ->
                createRemovedStatusItem(
                    changeEvent, parentDatasetSchemaMetadataItem, aspectRetriever)
                    .stream());
  }

  private static Optional<ChangeMCP> createRemovedStatusItem(
      @Nonnull final ChangeEvent changeEvent,
      @Nonnull final MCLItem parentDatasetSchemaMetadataItem,
      @Nonnull AspectRetriever aspectRetriever) {

    if (changeEvent.getCategory().equals(ChangeCategory.TECHNICAL_SCHEMA)) {
      if (log.isDebugEnabled()) {
        // Protect against expensive toString() call
        String parameterString = "";
        if (changeEvent.getParameters() != null) {
          parameterString =
              changeEvent.getParameters().entrySet().stream()
                  .map(entry -> entry.getKey() + ":" + entry.getValue())
                  .collect(Collectors.joining(","));
        }
        log.debug(
            "Technical Schema Event: Entity: {}\nOperation: {}\nModifier: {}\nAuditStamp: {}\nParameters:{}\nCategory:{} ",
            changeEvent.getEntityUrn(),
            changeEvent.getOperation(),
            changeEvent.getModifier(),
            changeEvent.getAuditStamp(),
            parameterString,
            changeEvent.getCategory());
      }
      if (changeEvent.getOperation().equals(ChangeOperation.REMOVE)) {
        String fieldPath = changeEvent.getModifier().toString();
        log.debug("Creating status change proposal {} for field: {}", true, fieldPath);
        return Optional.of(
            buildSchemaFieldStatusMCPs(
                UrnUtils.getUrn(fieldPath),
                true,
                parentDatasetSchemaMetadataItem.getAuditStamp(),
                parentDatasetSchemaMetadataItem.getSystemMetadata(),
                aspectRetriever));
      }
    }
    return Optional.empty();
  }

  /**
   * Expand dataset schemaMetadata to schemaFields
   *
   * @param parentDatasetMetadataSchemaItem dataset mcp item
   * @param aspectRetriever aspectRetriever context
   * @return side effect schema field aspects
   */
  private static Stream<ChangeMCP> buildSchemaFieldKeyMCPs(
      MCLItem parentDatasetMetadataSchemaItem,
      Map<Urn, Map<String, Aspect>> aspectData,
      @Nonnull AspectRetriever aspectRetriever) {

    Stream<ChangeMCP> schemaFieldKeys =
        buildSchemaFieldKeyMCPs(parentDatasetMetadataSchemaItem, aspectRetriever);

    // Handle case where dataset status created before schema metadata
    final Stream<ChangeMCP> statusSideEffects;
    if (aspectData
        .getOrDefault(parentDatasetMetadataSchemaItem.getUrn(), Map.of())
        .containsKey(STATUS_ASPECT_NAME)) {
      Status status =
          new Status(
              aspectData
                  .get(parentDatasetMetadataSchemaItem.getUrn())
                  .get(STATUS_ASPECT_NAME)
                  .data());

      ChangeItemImpl datasetStatusItem =
          ChangeItemImpl.builder()
              .changeType(ChangeType.UPSERT)
              .urn(parentDatasetMetadataSchemaItem.getUrn())
              .aspectName(STATUS_ASPECT_NAME)
              .entitySpec(parentDatasetMetadataSchemaItem.getEntitySpec())
              .aspectSpec(
                  parentDatasetMetadataSchemaItem.getEntitySpec().getAspectSpec(STATUS_ASPECT_NAME))
              .recordTemplate(status)
              .auditStamp(parentDatasetMetadataSchemaItem.getAuditStamp())
              .systemMetadata(parentDatasetMetadataSchemaItem.getSystemMetadata())
              .build(aspectRetriever);
      statusSideEffects = mirrorStatusAspect(datasetStatusItem, aspectData, aspectRetriever);
    } else {
      statusSideEffects = Stream.empty();
    }

    return optimizedKeyAspectMCPsConcat(schemaFieldKeys, statusSideEffects);
  }

  /**
   * Given a dataset's metadata schema item, generate schema field key aspects
   *
   * @param parentDatasetMetadataSchemaItem dataset's metadata schema MCP
   * @param aspectRetriever retriever
   * @return stream of schema field MCPs for its key aspect
   */
  private static Stream<ChangeMCP> buildSchemaFieldKeyMCPs(
      @Nonnull MCLItem parentDatasetMetadataSchemaItem, @Nonnull AspectRetriever aspectRetriever) {

    SystemMetadata systemMetadata = parentDatasetMetadataSchemaItem.getSystemMetadata();
    SchemaMetadata schemaMetadata = parentDatasetMetadataSchemaItem.getAspect(SchemaMetadata.class);
    SchemaMetadata previousSchemaMetadata =
        parentDatasetMetadataSchemaItem.getPreviousRecordTemplate() != null
            ? parentDatasetMetadataSchemaItem.getPreviousAspect(SchemaMetadata.class)
            : null;

    return schemaMetadata.getFields().stream()
        .filter(
            schemaField ->
                ChangeType.RESTATE.equals(parentDatasetMetadataSchemaItem.getChangeType())
                    || previousSchemaMetadata == null
                    // avoid processing already existing fields
                    || !previousSchemaMetadata.getFields().contains(schemaField)
                    // system update pass through
                    || isSystemUpdate(systemMetadata))
        .map(
            schemaField ->
                ChangeItemImpl.builder()
                    .urn(
                        SchemaFieldUtils.generateSchemaFieldUrn(
                            parentDatasetMetadataSchemaItem.getUrn(), schemaField))
                    .changeType(ChangeType.UPSERT)
                    .aspectName(SCHEMA_FIELD_KEY_ASPECT)
                    .recordTemplate(
                        new SchemaFieldKey()
                            .setFieldPath(schemaField.getFieldPath())
                            .setParent(parentDatasetMetadataSchemaItem.getUrn()))
                    .auditStamp(parentDatasetMetadataSchemaItem.getAuditStamp())
                    .systemMetadata(parentDatasetMetadataSchemaItem.getSystemMetadata())
                    .build(aspectRetriever));
  }

  /**
   * Given a dataset's metadata schema item, generate schema field alias aspects
   *
   * @param parentDatasetMetadataSchemaItem dataset's metadata schema MCP
   * @param aspectRetriever retriever
   * @return stream of schema field aliases for its key aspect
   */
  private static Stream<ChangeMCP> buildSchemaFieldAliasesMCPs(
      @Nonnull MCLItem parentDatasetMetadataSchemaItem, @Nonnull AspectRetriever aspectRetriever) {

    SystemMetadata systemMetadata = parentDatasetMetadataSchemaItem.getSystemMetadata();
    SchemaMetadata schemaMetadata = parentDatasetMetadataSchemaItem.getAspect(SchemaMetadata.class);
    SchemaMetadata previousSchemaMetadata =
        parentDatasetMetadataSchemaItem.getPreviousRecordTemplate() != null
            ? parentDatasetMetadataSchemaItem.getPreviousAspect(SchemaMetadata.class)
            : null;

    return schemaMetadata.getFields().stream()
        .filter(
            schemaField ->
                ChangeType.RESTATE.equals(parentDatasetMetadataSchemaItem.getChangeType())
                    || previousSchemaMetadata == null
                    || !previousSchemaMetadata.getFields().equals(schemaMetadata.getFields())
                    // system update pass through
                    || isSystemUpdate(systemMetadata))
        .map(
            schemaField -> {
              Set<Urn> currentAliases =
                  SchemaFieldUtils.getSchemaFieldAliases(
                      parentDatasetMetadataSchemaItem.getUrn(), schemaMetadata, schemaField);
              Set<Urn> previousAliases =
                  previousSchemaMetadata == null
                      ? Set.of()
                      : SchemaFieldUtils.getSchemaFieldAliases(
                          parentDatasetMetadataSchemaItem.getUrn(),
                          previousSchemaMetadata,
                          schemaField);

              boolean forceUpdate =
                  isSystemUpdate(systemMetadata)
                      || ChangeType.RESTATE.equals(parentDatasetMetadataSchemaItem.getChangeType());
              if (!previousAliases.equals(currentAliases) || forceUpdate) {
                return ChangeItemImpl.builder()
                    .urn(
                        SchemaFieldUtils.generateSchemaFieldUrn(
                            parentDatasetMetadataSchemaItem.getUrn(), schemaField))
                    .changeType(ChangeType.UPSERT)
                    .aspectName(SCHEMA_FIELD_ALIASES_ASPECT)
                    .recordTemplate(
                        new SchemaFieldAliases().setAliases(new UrnArray(currentAliases)))
                    .auditStamp(parentDatasetMetadataSchemaItem.getAuditStamp())
                    .systemMetadata(parentDatasetMetadataSchemaItem.getSystemMetadata())
                    .build(aspectRetriever);
              }

              return (ChangeMCP) null;
            })
        .filter(Objects::nonNull);
  }

  /**
   * Given a delete MCL for a dataset's schema metadata, generate delete MCPs for the schemaField
   * aspects
   *
   * @param parentDatasetSchemaMetadataDelete the dataset's MCL from a metadata schema delete
   * @param aspectRetriever retriever context
   * @return follow-up deletes for the schema field
   */
  public static Stream<MCPItem> buildSchemaMetadataSchemaFieldDeleteMCPs(
      @Nonnull MCLItem parentDatasetSchemaMetadataDelete,
      @Nonnull AspectRetriever aspectRetriever) {

    EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();
    Urn datasetUrn = parentDatasetSchemaMetadataDelete.getUrn();
    SchemaMetadata parentDatasetSchemaMetadata =
        parentDatasetSchemaMetadataDelete.getPreviousAspect(SchemaMetadata.class);

    return parentDatasetSchemaMetadata.getFields().stream()
        .flatMap(
            schemaField ->
                entityRegistry.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME).getAspectSpecs().stream()
                    .map(
                        aspectSpec ->
                            DeleteItemImpl.builder()
                                .urn(
                                    SchemaFieldUtils.generateSchemaFieldUrn(
                                        datasetUrn, schemaField))
                                .aspectName(aspectSpec.getName())
                                .auditStamp(parentDatasetSchemaMetadataDelete.getAuditStamp())
                                .entitySpec(entityRegistry.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                                .aspectSpec(aspectSpec)
                                .build(aspectRetriever)));
  }

  public static Stream<MCPItem> buildStatusSchemaFieldDeleteMCPs(
      @Nonnull MCLItem parentDatasetStatusDelete,
      @Nullable SchemaMetadata parentDatasetSchemaMetadata,
      @Nonnull AspectRetriever aspectRetriever) {

    if (parentDatasetSchemaMetadata == null) {
      return Stream.empty();
    } else {
      EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();
      return parentDatasetSchemaMetadata.getFields().stream()
          .map(
              schemaField ->
                  DeleteItemImpl.builder()
                      .urn(
                          SchemaFieldUtils.generateSchemaFieldUrn(
                              parentDatasetStatusDelete.getUrn(), schemaField))
                      .aspectName(STATUS_ASPECT_NAME)
                      .auditStamp(parentDatasetStatusDelete.getAuditStamp())
                      .entitySpec(entityRegistry.getEntitySpec(SCHEMA_FIELD_ENTITY_NAME))
                      .aspectSpec(parentDatasetStatusDelete.getAspectSpec())
                      .build(aspectRetriever));
    }
  }

  /**
   * Fetch missing aspects if not already in the batch
   *
   * @param mclItems batch mcps
   * @param aspectRetriever aspect retriever
   * @return required aspects for the side effect processing
   */
  private static Map<Urn, Map<String, Aspect>> fetchRequiredAspects(
      Collection<MCLItem> mclItems,
      Set<String> requiredAspectNames,
      AspectRetriever aspectRetriever) {
    Map<Urn, Map<String, Aspect>> aspectData = new HashMap<>();

    // Aspects included data in batch
    mclItems.stream()
        .filter(item -> item.getRecordTemplate() != null)
        .forEach(
            item ->
                aspectData
                    .computeIfAbsent(item.getUrn(), k -> new HashMap<>())
                    .put(item.getAspectName(), new Aspect(item.getRecordTemplate().data())));

    // Aspect to fetch
    Map<Urn, Set<String>> missingAspectData =
        mclItems.stream()
            .flatMap(
                item ->
                    requiredAspectNames.stream()
                        .filter(
                            aspectName ->
                                !aspectData
                                    .getOrDefault(item.getUrn(), Map.of())
                                    .containsKey(aspectName))
                        .map(aspectName -> Pair.of(item.getUrn(), aspectName)))
            .collect(
                Collectors.groupingBy(
                    Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toSet())));

    // Fetch missing
    missingAspectData.forEach(
        (urn, aspectNames) -> {
          Map<String, Aspect> fetchedData =
              aspectRetriever.getLatestAspectObjects(Set.of(urn), aspectNames).get(urn);
          if (fetchedData != null) {
            fetchedData.forEach(
                (aspectName, aspectValue) ->
                    aspectData
                        .computeIfAbsent(urn, k -> new HashMap<>())
                        .put(aspectName, aspectValue));
          }
        });

    return aspectData;
  }

  /**
   * We can reduce the number of MCPs sent via kafka by 1/2 to 1/3 because key aspects are
   * automatically created if they don't exist. The only case where there needs to be an explicit
   * key aspect is when there are no other aspects being generated.
   *
   * @param keyMCPs stream of MCPs which *may* contain key aspects
   * @param otherMCPs stream of MCPs which are not expected to contain key aspects
   * @return reduced stream of MCPs
   */
  private static <T extends MCPItem> Stream<T> optimizedKeyAspectMCPsConcat(
      Stream<T> keyMCPs, Stream<T> otherMCPs) {
    List<T> other = otherMCPs.collect(Collectors.toList());
    Set<Urn> otherUrns = other.stream().map(T::getUrn).collect(Collectors.toSet());
    return Stream.concat(
        keyMCPs.filter(
            item ->
                !item.getAspectName().equals(item.getEntitySpec().getKeyAspectName())
                    || !otherUrns.contains(item.getUrn())),
        other.stream());
  }

  private static boolean isSystemUpdate(@Nullable SystemMetadata systemMetadata) {
    return systemMetadata != null
        && systemMetadata.getProperties() != null
        && SYSTEM_UPDATE_SOURCE.equals(systemMetadata.getProperties().get(APP_SOURCE));
  }
}
