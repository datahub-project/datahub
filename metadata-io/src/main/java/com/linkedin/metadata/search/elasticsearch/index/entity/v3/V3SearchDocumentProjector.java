package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Projects transformed searchable aspect fields into the consolidated V3 entity document. */
public class V3SearchDocumentProjector {

  public static final String URN_FIELD = MappingConstants.URN_FIELD;
  public static final String ENTITY_TYPE_FIELD = MappingConstants.ENTITY_TYPE_FIELD;
  public static final String SYSTEM_METADATA_FIELD = "_systemMetadata";

  /**
   * Aspect JSON stored in {@code _source} and not indexed. This is the same serialization written
   * to SQL. A string, so a partial document update replaces it.
   */
  public static final String ASPECT_JSON_FIELD = "_aspectJson";

  private static final String SEMANTIC_CONTENT_ASPECT_NAME = "semanticContent";

  private final SearchDocumentTransformer searchDocumentTransformer;

  public V3SearchDocumentProjector(
      @Nonnull final SearchDocumentTransformer searchDocumentTransformer) {
    this.searchDocumentTransformer = searchDocumentTransformer;
  }

  public ObjectNode newEntityDocument(
      @Nonnull final Urn urn, @Nonnull final EntitySpec entitySpec) {
    return newEntityDocument(urn, entitySpec, null);
  }

  public ObjectNode newEntityDocument(
      @Nonnull final Urn urn,
      @Nonnull final EntitySpec entitySpec,
      @Nullable final EntityRegistry entityRegistry) {
    final ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put(URN_FIELD, urn.toString());
    document.put(ENTITY_TYPE_FIELD, entitySpec.getName());
    if (entityRegistry != null) {
      putParsedUrnName(document, urn, entityRegistry);
    }
    return document;
  }

  public Optional<ProjectedAspect> projectAspect(
      @Nonnull final OperationContext opContext, @Nonnull final MCLItem event) {
    return projectAspect(opContext, event, event.getPreviousRecordTemplate());
  }

  public Optional<ProjectedAspect> projectAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final MCLItem event,
      @Nullable final RecordTemplate previousAspect) {
    final Urn urn = event.getUrn();
    final AspectSpec aspectSpec = event.getAspectSpec();

    final Optional<ObjectNode> searchDocument =
        transformCurrentAspect(opContext, event, aspectSpec);
    if (searchDocument.isEmpty()) {
      return Optional.empty();
    }

    final Optional<ObjectNode> previousSearchDocument =
        transformPreviousAspect(opContext, urn, aspectSpec, previousAspect, event);
    if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName())) {
      searchDocument.ifPresent(
          document ->
              StructuredPropertyUrnIdentity.project(
                  opContext, document, event.getRecordTemplate()));
      if (previousAspect != null) {
        previousSearchDocument.ifPresent(
            document -> StructuredPropertyUrnIdentity.project(opContext, document, previousAspect));
      }
    }
    final ObjectNode projectedFields =
        SearchDocumentTransformer.handleRemoveFields(
            searchDocument.get().deepCopy(), previousSearchDocument.orElse(null));
    omitV3ExcludedFields(aspectSpec, projectedFields);
    storeAspectJson(event, aspectSpec, projectedFields);
    final Set<String> sharedHasValues = placeSharedHasValues(aspectSpec, projectedFields);
    final EntityRegistry entityRegistry = opContext.getEntityRegistry();
    final List<EntitySpec> indexGroup =
        V3SearchFields.indexGroupSpecs(entityRegistry, List.of(event.getEntitySpec()));
    final ParsedUrnNames parsedUrnNames =
        parsedUrnNames(entityRegistry, aspectSpec, projectedFields, indexGroup);

    return Optional.of(
        buildProjectedAspect(
            aspectSpec.getName(),
            projectedFields,
            event.getSystemMetadata(),
            STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName()),
            sharedHasValues,
            parsedUrnNames,
            event.getUrn(),
            opContext));
  }

  public void applyProjection(
      @Nonnull final ObjectNode document, @Nonnull final ProjectedAspect projectedAspect) {
    copyFields(projectedAspect.rootFields(), document);
    if (projectedAspect.rootOnly()) {
      // Searchable values stay at the root. Parsed-name keywords still live under the aspect so
      // copy_to can rebuild a label after another aspect is updated.
      final ObjectNode sources = JsonNodeFactory.instance.objectNode();
      projectedAspect
          .aspectFields()
          .fieldNames()
          .forEachRemaining(
              name -> {
                if (name.endsWith(V3SearchFields.PARSED_URN_SUFFIX)) {
                  sources.set(name, projectedAspect.aspectFields().get(name).deepCopy());
                }
              });
      if (!sources.isEmpty()) {
        getOrCreateObject(document, MappingConstants.ASPECTS_FIELD_NAME)
            .set(projectedAspect.aspectName(), sources);
      }
      return;
    }

    final ObjectNode aspectsNode = getOrCreateObject(document, MappingConstants.ASPECTS_FIELD_NAME);
    aspectsNode.set(projectedAspect.aspectName(), projectedAspect.aspectFields().deepCopy());
  }

  private Optional<ObjectNode> transformCurrentAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final MCLItem event,
      @Nonnull final AspectSpec aspectSpec) {
    try {
      final boolean isDelete = ChangeType.DELETE.equals(event.getChangeType());
      Optional<ObjectNode> transformed =
          searchDocumentTransformer
              .transformAspect(
                  opContext,
                  event.getUrn(),
                  event.getRecordTemplate(),
                  aspectSpec,
                  isDelete,
                  event.getAuditStamp())
              .map(
                  objectNode ->
                      event.getAuditStamp() == null
                          ? objectNode
                          : SearchDocumentTransformer.withSystemCreated(
                              objectNode,
                              changeTypeForSystemCreated(event),
                              event.getEntitySpec(),
                              aspectSpec,
                              event.getAuditStamp()));
      return transformed;
    } catch (Exception e) {
      throw propagate(
          "Failed to transform V3 aspect " + aspectSpec.getName() + " for " + event.getUrn(), e);
    }
  }

  /**
   * A backfill restates what primary storage holds, so its key aspect carries the row's creation
   * time the way the upsert that created the entity did, and the document records it. A live
   * restate records nothing, as before.
   */
  private static ChangeType changeTypeForSystemCreated(@Nonnull final MCLItem event) {
    if (event.getChangeType() != ChangeType.RESTATE) {
      return event.getChangeType();
    }
    final MetadataChangeLog changeLog = event.getMetadataChangeLog();
    final boolean backfill =
        changeLog != null
            && changeLog.getHeaders() != null
            && changeLog.getHeaders().containsKey(BackfillHeaders.MODE_HEADER_NAME);
    return backfill ? ChangeType.UPSERT : ChangeType.RESTATE;
  }

  private Optional<ObjectNode> transformPreviousAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate previousAspect,
      @Nonnull final MCLItem event) {
    if (previousAspect == null) {
      return Optional.empty();
    }
    try {
      Optional<ObjectNode> transformed =
          searchDocumentTransformer.transformAspect(
              opContext, urn, previousAspect, aspectSpec, false, event.getAuditStamp());
      return transformed;
    } catch (Exception e) {
      throw propagate(
          "Failed to transform previous V3 aspect " + aspectSpec.getName() + " for " + urn, e);
    }
  }

  private static void omitV3ExcludedFields(
      @Nonnull final AspectSpec aspectSpec, @Nonnull final ObjectNode projectedFields) {
    for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
      final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
      if (!annotation.isV3Searchable()) {
        projectedFields.remove(annotation.getFieldName());
        annotation.getHasValuesFieldName().ifPresent(projectedFields::remove);
        annotation.getNumValuesFieldName().ifPresent(projectedFields::remove);
      }
      if (!annotation.resolvedForV3().isIncludeSystemModifiedAt()) {
        ESUtils.getSystemModifiedAtFieldName(fieldSpec).ifPresent(projectedFields::remove);
      }
    }
  }

  /**
   * A shared-label hasValues boolean is written under the aspect, at the declared name, only when a
   * source in this aspect is populated. {@code copy_to} sends that true to the degree's root name
   * ({@code hasDescription} or {@code hasDescription_1}). The declared name is cleared on the root
   * so a column value is not indexed as the entity's own boolean.
   */
  @Nonnull
  private static Set<String> placeSharedHasValues(
      @Nonnull final AspectSpec aspectSpec, @Nonnull final ObjectNode projectedFields) {
    final Set<String> names = new LinkedHashSet<>();
    final Set<String> populated = new LinkedHashSet<>();
    for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
      final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
      final SearchableAnnotation resolved = annotation.resolvedForV3();
      if (!resolved.isV3Searchable()) {
        continue;
      }
      final Optional<String> label = resolved.indexedFieldName();
      if (label.isEmpty()) {
        continue;
      }
      resolved
          .getHasValuesFieldName()
          .ifPresent(
              booleanName -> {
                names.add(booleanName);
                if (sourcePopulated(annotation, projectedFields)) {
                  populated.add(booleanName);
                }
              });
    }
    for (String name : names) {
      if (populated.contains(name)) {
        projectedFields.put(name, true);
      } else {
        projectedFields.remove(name);
      }
    }
    return names;
  }

  /**
   * Parsed name of each URN field on this aspect. The document's own URN is not included. An empty
   * array clears a name the field no longer contributes. Fields whose parsed name occupies the root
   * label are listed in {@link ParsedUrnNames#rootFields()}.
   */
  @Nonnull
  private static ParsedUrnNames parsedUrnNames(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final ObjectNode projectedFields,
      @Nonnull final List<EntitySpec> indexGroup) {
    final Map<String, ArrayNode> names = new LinkedHashMap<>();
    final Set<String> rootFields = new LinkedHashSet<>();
    for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
      if (!V3SearchFields.contributesParsedUrn(fieldSpec)) {
        continue;
      }
      final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation().resolvedForV3();
      final Set<String> found = new LinkedHashSet<>();
      collectUrnText(projectedFields.get(annotation.getFieldName()), entityRegistry, found);
      names.put(V3SearchFields.parsedUrnField(annotation.getFieldName()), arrayOf(found));
      if (V3SearchFields.clearsRootForUrnLabel(fieldSpec, indexGroup)) {
        rootFields.add(annotation.getFieldName());
      }
    }
    return new ParsedUrnNames(names, rootFields);
  }

  @Nonnull
  private static ArrayNode arrayOf(@Nonnull final Set<String> values) {
    final ArrayNode array = JsonNodeFactory.instance.arrayNode();
    values.forEach(array::add);
    return array;
  }

  private static void collectUrnText(
      @Nullable final JsonNode value,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Set<String> identities) {
    if (value == null || value.isNull()) {
      return;
    }
    if (value.isArray()) {
      value.forEach(item -> collectUrnText(item, entityRegistry, identities));
      return;
    }
    if (value.isObject()) {
      collectUrnText(value.get("urn"), entityRegistry, identities);
      return;
    }
    if (!value.isTextual() || !value.asText().startsWith("urn:")) {
      return;
    }
    try {
      final String identity =
          UrnSearchIdentity.identity(Urn.createFromString(value.asText()), entityRegistry);
      if (!identity.isEmpty()) {
        identities.add(identity);
      }
    } catch (URISyntaxException exception) {
      // A value that only looks like a URN stays exact and contributes no full-text identity.
    }
  }

  private static boolean sourcePopulated(
      @Nonnull final SearchableAnnotation annotation, @Nonnull final ObjectNode projectedFields) {
    final Optional<String> declared = annotation.getHasValuesFieldName();
    if (declared.isPresent()) {
      final JsonNode flag = projectedFields.get(declared.get());
      return flag != null && flag.isBoolean() && flag.booleanValue();
    }
    final JsonNode value = projectedFields.get(annotation.getFieldName());
    if (value == null || value.isNull()) {
      return false;
    }
    if (value.isBoolean()) {
      return value.booleanValue();
    }
    if (value.isTextual()) {
      return !value.asText().isEmpty();
    }
    return !value.isArray() || !value.isEmpty();
  }

  private static void storeAspectJson(
      @Nonnull final MCLItem event,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final ObjectNode projectedFields) {
    if (aspectSpec.getAspectAnnotation() == null
        || !aspectSpec.getAspectAnnotation().isAspectJson()) {
      return;
    }
    if (ChangeType.DELETE.equals(event.getChangeType()) || event.getRecordTemplate() == null) {
      projectedFields.putNull(ASPECT_JSON_FIELD);
      return;
    }
    projectedFields.put(ASPECT_JSON_FIELD, RecordUtils.toJsonString(event.getRecordTemplate()));
  }

  /**
   * Aspect JSON from an {@code _aspects.<aspect>} object. A string is the stored value. An object
   * is accepted so a caller can still parse a value that was written as JSON.
   */
  @Nullable
  public static String aspectJson(@Nullable final JsonNode aspectNode) {
    if (aspectNode == null || aspectNode.isNull() || aspectNode.isMissingNode()) {
      return null;
    }
    JsonNode stored = aspectNode.get(ASPECT_JSON_FIELD);
    if (stored == null || stored.isNull() || stored.isMissingNode()) {
      return null;
    }
    return stored.isTextual() ? stored.asText() : stored.toString();
  }

  private static ProjectedAspect buildProjectedAspect(
      @Nonnull final String aspectName,
      @Nonnull final ObjectNode projectedFields,
      @Nullable final SystemMetadata systemMetadata,
      final boolean rootOnly,
      @Nonnull final Set<String> sharedHasValues,
      @Nonnull final ParsedUrnNames parsedUrns,
      @Nonnull final Urn urn,
      @Nonnull final OperationContext opContext) {
    final ObjectNode rootFields = projectedFields.deepCopy();
    rootFields.remove(URN_FIELD);
    rootFields.remove(ASPECT_JSON_FIELD);

    final ObjectNode aspectFields = projectedFields.deepCopy();
    aspectFields.remove(URN_FIELD);
    for (String name : sharedHasValues) {
      // Drop a stored root true or false. The indexed term comes from copy_to.
      rootFields.putNull(name);
      final JsonNode aspectValue = aspectFields.get(name);
      if (aspectValue == null || !aspectValue.isBoolean() || !aspectValue.booleanValue()) {
        aspectFields.remove(name);
      }
    }
    addSystemMetadata(opContext, aspectFields, systemMetadata);
    // Kept on the aspect so a later partial update of another aspect does not replace it.
    // An empty array clears a name this field no longer contributes.
    parsedUrns
        .names()
        .forEach(
            (fieldName, values) -> {
              aspectFields.set(fieldName, values);
            });
    // A URN label is an object filled by copy_to. Clear a previously stored string.
    for (String fieldName : parsedUrns.rootFields()) {
      rootFields.putNull(fieldName);
    }

    if (SEMANTIC_CONTENT_ASPECT_NAME.equals(aspectName)) {
      // Vectors stay under _aspects; UpdateIndicesV3Strategy lifts them to the root only for
      // entities with semantic search enabled, whose V3 index maps a root embeddings field.
      final ObjectNode semanticRoot = JsonNodeFactory.instance.objectNode();
      putParsedUrnName(semanticRoot, urn, opContext.getEntityRegistry());
      return new ProjectedAspect(aspectName, semanticRoot, aspectFields, false);
    }

    putParsedUrnName(rootFields, urn, opContext.getEntityRegistry());
    return new ProjectedAspect(aspectName, rootFields, aspectFields, rootOnly);
  }

  private static void putParsedUrnName(
      @Nonnull final ObjectNode rootFields,
      @Nonnull final Urn urn,
      @Nonnull final EntityRegistry entityRegistry) {
    final String identity = UrnSearchIdentity.identity(urn, entityRegistry);
    if (!identity.isEmpty()) {
      rootFields.put(MappingConstants.URN_NAME_FIELD, identity);
    }
  }

  private static void addSystemMetadata(
      @Nonnull final OperationContext opContext,
      @Nonnull final ObjectNode aspectFields,
      @Nullable final SystemMetadata systemMetadata) {
    if (systemMetadata == null) {
      return;
    }
    try {
      aspectFields.set(
          SYSTEM_METADATA_FIELD,
          opContext.getObjectMapper().readTree(RecordUtils.toJsonString(systemMetadata)));
    } catch (Exception e) {
      throw propagate("Failed to serialize system metadata for V3 projected aspect", e);
    }
  }

  private record ParsedUrnNames(
      @Nonnull Map<String, ArrayNode> names, @Nonnull Set<String> rootFields) {}

  private static RuntimeException propagate(
      @Nonnull final String message, @Nonnull final Exception exception) {
    if (exception instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    return new IllegalStateException(message, exception);
  }

  private static ObjectNode getOrCreateObject(
      @Nonnull final ObjectNode parent, @Nonnull final String fieldName) {
    final JsonNode existing = parent.get(fieldName);
    if (existing instanceof ObjectNode) {
      return (ObjectNode) existing;
    }
    final ObjectNode created = JsonNodeFactory.instance.objectNode();
    parent.set(fieldName, created);
    return created;
  }

  private static void copyFields(
      @Nonnull final ObjectNode source, @Nonnull final ObjectNode target) {
    final Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
    fields.forEachRemaining(entry -> target.set(entry.getKey(), entry.getValue().deepCopy()));
  }

  public record ProjectedAspect(
      @Nonnull String aspectName,
      @Nonnull ObjectNode rootFields,
      @Nonnull ObjectNode aspectFields,
      boolean rootOnly) {}
}
