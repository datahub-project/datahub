package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
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
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
  private static final String SEMANTIC_CONTENT_ASPECT_NAME = "semanticContent";

  private final SearchDocumentTransformer searchDocumentTransformer;

  public V3SearchDocumentProjector(
      @Nonnull final SearchDocumentTransformer searchDocumentTransformer) {
    this.searchDocumentTransformer = searchDocumentTransformer;
  }

  public ObjectNode newEntityDocument(
      @Nonnull final Urn urn, @Nonnull final EntitySpec entitySpec) {
    final ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put(URN_FIELD, urn.toString());
    document.put(ENTITY_TYPE_FIELD, entitySpec.getName());
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
    final ObjectNode projectedFields =
        SearchDocumentTransformer.handleRemoveFields(
            searchDocument.get().deepCopy(), previousSearchDocument.orElse(null));
    omitV3ExcludedFields(aspectSpec, projectedFields);
    final Set<String> sharedHasValues =
        placeSharedHasValues(event.getEntitySpec(), aspectSpec, projectedFields);

    return Optional.of(
        buildProjectedAspect(
            aspectSpec.getName(),
            projectedFields,
            event.getSystemMetadata(),
            STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName()),
            sharedHasValues,
            opContext));
  }

  public void applyProjection(
      @Nonnull final ObjectNode document, @Nonnull final ProjectedAspect projectedAspect) {
    copyFields(projectedAspect.rootFields(), document);
    if (projectedAspect.rootOnly()) {
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
   * A shared-label hasValues boolean is written under the aspect only when a source in this aspect
   * is populated. {@code copy_to} ORs those trues onto the root name. The root value is cleared so
   * a stored false is not indexed beside a copied true.
   */
  @Nonnull
  private static Set<String> placeSharedHasValues(
      @Nonnull final EntitySpec entitySpec,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final ObjectNode projectedFields) {
    final Set<String> names = new LinkedHashSet<>();
    final Set<String> populated = new LinkedHashSet<>();
    for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
      final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
      final SearchableAnnotation resolved = annotation.resolvedForV3();
      if (!resolved.isV3Searchable()) {
        continue;
      }
      final Optional<String> label = resolved.getSearchLabel().filter(name -> !name.isEmpty());
      if (label.isEmpty()) {
        continue;
      }
      V3SearchFields.hasValuesFieldName(entitySpec, label.get())
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
   * Uses the boolean {@code setSearchableValue} already wrote when this field declares one.
   * Otherwise the source field it wrote is populated when that value is present.
   */
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

  private static ProjectedAspect buildProjectedAspect(
      @Nonnull final String aspectName,
      @Nonnull final ObjectNode projectedFields,
      @Nullable final SystemMetadata systemMetadata,
      final boolean rootOnly,
      @Nonnull final Set<String> sharedHasValues,
      @Nonnull final OperationContext opContext) {
    final ObjectNode rootFields = projectedFields.deepCopy();
    rootFields.remove(URN_FIELD);

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

    if (SEMANTIC_CONTENT_ASPECT_NAME.equals(aspectName)) {
      // Vectors stay under _aspects; UpdateIndicesV3Strategy lifts them to the root only for
      // entities with semantic search enabled, whose V3 index maps a root embeddings field.
      return new ProjectedAspect(
          aspectName, JsonNodeFactory.instance.objectNode(), aspectFields, false);
    }

    return new ProjectedAspect(aspectName, rootFields, aspectFields, rootOnly);
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
