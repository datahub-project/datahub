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
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Projects transformed searchable aspect fields into the consolidated V3 entity document. */
public class V3SearchDocumentProjector {

  public static final String URN_FIELD = "urn";
  public static final String ENTITY_TYPE_FIELD = "_entityType";
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

    return Optional.of(
        buildProjectedAspect(
            aspectSpec.getName(),
            projectedFields,
            event.getSystemMetadata(),
            STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName()),
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
      return searchDocumentTransformer
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
                          event.getChangeType(),
                          event.getEntitySpec(),
                          aspectSpec,
                          event.getAuditStamp()));
    } catch (Exception e) {
      throw propagate(
          "Failed to transform V3 aspect " + aspectSpec.getName() + " for " + event.getUrn(), e);
    }
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
      return searchDocumentTransformer.transformAspect(
          opContext, urn, previousAspect, aspectSpec, false, event.getAuditStamp());
    } catch (Exception e) {
      throw propagate(
          "Failed to transform previous V3 aspect " + aspectSpec.getName() + " for " + urn, e);
    }
  }

  private static ProjectedAspect buildProjectedAspect(
      @Nonnull final String aspectName,
      @Nonnull final ObjectNode projectedFields,
      @Nullable final SystemMetadata systemMetadata,
      final boolean rootOnly,
      @Nonnull final OperationContext opContext) {
    final ObjectNode rootFields = projectedFields.deepCopy();
    rootFields.remove(URN_FIELD);

    final ObjectNode aspectFields = projectedFields.deepCopy();
    aspectFields.remove(URN_FIELD);
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
