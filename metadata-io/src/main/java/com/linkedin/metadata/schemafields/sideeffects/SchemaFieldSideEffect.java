package com.linkedin.metadata.schemafields.sideeffects;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_KEY_ASPECT;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
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
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private static final Set<String> REQUIRED_ASPECTS =
      Set.of(Constants.SCHEMA_METADATA_ASPECT_NAME, Constants.STATUS_ASPECT_NAME);

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {

    // fetch existing aspects
    Map<Urn, Map<String, Aspect>> aspectData =
        fetchRequiredAspects(
            changeMCPs.stream().map(i -> (BatchItem) i).collect(Collectors.toList()),
            REQUIRED_ASPECTS,
            retrieverContext.getAspectRetriever());

    Map<Urn, Set<String>> batchMCPAspectNames =
        changeMCPs.stream()
            .collect(
                Collectors.groupingBy(
                    ChangeMCP::getUrn,
                    Collectors.mapping(ChangeMCP::getAspectName, Collectors.toSet())));

    Stream<ChangeMCP> schemaFieldSideEffects =
        changeMCPs.stream()
            .filter(
                changeMCP ->
                    DATASET_ENTITY_NAME.equals(changeMCP.getUrn().getEntityType())
                        && Constants.SCHEMA_METADATA_ASPECT_NAME.equals(changeMCP.getAspectName()))
            .flatMap(
                item ->
                    buildSchemaFieldKeyMCPs(
                        item, aspectData, retrieverContext.getAspectRetriever()));

    Stream<ChangeMCP> statusSideEffects =
        changeMCPs.stream()
            .filter(
                changeMCP ->
                    DATASET_ENTITY_NAME.equals(changeMCP.getUrn().getEntityType())
                        && Constants.STATUS_ASPECT_NAME.equals(changeMCP.getAspectName())
                        // if present, already computed above
                        && !batchMCPAspectNames
                            .getOrDefault(changeMCP.getUrn(), Set.of())
                            .contains(Constants.SCHEMA_METADATA_ASPECT_NAME))
            .flatMap(
                item ->
                    mirrorStatusAspect(item, aspectData, retrieverContext.getAspectRetriever()));

    return Stream.concat(statusSideEffects, schemaFieldSideEffects);
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
      Map<Urn, Map<String, Aspect>> aspectData =
          fetchRequiredAspects(
              statusDeletes.stream().map(item -> (BatchItem) item).collect(Collectors.toList()),
              Set.of(SCHEMA_METADATA_ASPECT_NAME),
              retrieverContext.getAspectRetriever());
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

    return buildSchemaFieldStatusMCPs(parentDatasetStatusItem, schemaMetadata, aspectRetriever);
  }

  /**
   * Build a schema field status MCP based on the input status from the dataset
   *
   * @param parentDatasetStatusItem dataset's status aspect
   * @param parentDatasetSchemaMetadata dataset's schema metadata
   * @param aspectRetriever aspect retriever
   * @return stream of status aspects for schema fields
   */
  public static Stream<ChangeMCP> buildSchemaFieldStatusMCPs(
      @Nonnull BatchItem parentDatasetStatusItem,
      @Nullable SchemaMetadata parentDatasetSchemaMetadata,
      @Nonnull AspectRetriever aspectRetriever) {
    if (parentDatasetSchemaMetadata == null) {
      return Stream.empty();
    } else {
      return parentDatasetSchemaMetadata.getFields().stream()
          .map(
              schemaField ->
                  ChangeItemImpl.builder()
                      .urn(getSchemaFieldUrn(parentDatasetStatusItem.getUrn(), schemaField))
                      .changeType(ChangeType.UPSERT)
                      .aspectName(parentDatasetStatusItem.getAspectName())
                      .recordTemplate(parentDatasetStatusItem.getRecordTemplate())
                      .auditStamp(parentDatasetStatusItem.getAuditStamp())
                      .systemMetadata(parentDatasetStatusItem.getSystemMetadata())
                      .build(aspectRetriever));
    }
  }

  /**
   * Expand dataset schemaMetadata to schemaFields
   *
   * @param parentDatasetMetadataSchemaItem dataset mcp item
   * @param aspectRetriever aspectRetriever context
   * @return side effect schema field aspects
   */
  private static Stream<ChangeMCP> buildSchemaFieldKeyMCPs(
      BatchItem parentDatasetMetadataSchemaItem,
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

    return Stream.concat(schemaFieldKeys, statusSideEffects);
  }

  /**
   * Given a dataset's metadata schema item, generate schema field key aspects
   *
   * @param parentDatasetMetadataSchemaItem dataset's metadata schema MCP
   * @param aspectRetriever retriever
   * @return stream of schema field MCPs for its key aspect
   */
  private static Stream<ChangeMCP> buildSchemaFieldKeyMCPs(
      @Nonnull BatchItem parentDatasetMetadataSchemaItem,
      @Nonnull AspectRetriever aspectRetriever) {

    SchemaMetadata schemaMetadata = parentDatasetMetadataSchemaItem.getAspect(SchemaMetadata.class);

    return schemaMetadata.getFields().stream()
        .map(
            schemaField ->
                ChangeItemImpl.builder()
                    .urn(getSchemaFieldUrn(parentDatasetMetadataSchemaItem.getUrn(), schemaField))
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
                                .urn(getSchemaFieldUrn(datasetUrn, schemaField))
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
                      .urn(getSchemaFieldUrn(parentDatasetStatusDelete.getUrn(), schemaField))
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
   * @param batchItems batch mcps
   * @param aspectRetriever aspect retriever
   * @return required aspects for the side effect processing
   */
  private static Map<Urn, Map<String, Aspect>> fetchRequiredAspects(
      Collection<BatchItem> batchItems,
      Set<String> requiredAspectNames,
      AspectRetriever aspectRetriever) {
    Map<Urn, Map<String, Aspect>> aspectData = new HashMap<>();

    // Aspects included data in batch
    batchItems.stream()
        .filter(item -> item.getRecordTemplate() != null)
        .forEach(
            item ->
                aspectData
                    .computeIfAbsent(item.getUrn(), k -> new HashMap<>())
                    .put(item.getAspectName(), new Aspect(item.getRecordTemplate().data())));

    // Aspect to fetch
    Map<Urn, Set<String>> missingAspectData =
        batchItems.stream()
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

  private static Urn getSchemaFieldUrn(Urn parentUrn, SchemaField field) {
    return Urn.createFromTuple(
        SCHEMA_FIELD_ENTITY_NAME, parentUrn.toString(), field.getFieldPath());
  }
}
