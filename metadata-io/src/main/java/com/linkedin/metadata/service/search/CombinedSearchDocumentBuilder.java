package com.linkedin.metadata.service.search;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds a single combined entity search document (aspects under {@link
 * com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants#ASPECTS_FIELD_NAME}).
 * Used by {@link com.linkedin.metadata.service.UpdateIndicesV3Strategy} and {@link
 * com.linkedin.metadata.service.PostgresEntitySearchStrategy}.
 */
@Slf4j
@RequiredArgsConstructor
public class CombinedSearchDocumentBuilder {

  @Nonnull private final SearchDocumentTransformer searchDocumentTransformer;

  /**
   * Combines multiple aspect events into one document with the standard _aspects (and related)
   * structure.
   *
   * @return the combined document or null if no aspects to process
   */
  public ObjectNode buildCombinedDocument(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull List<MCLItem> events) {

    ObjectNode combinedDocument = JsonNodeFactory.instance.objectNode();
    combinedDocument.put("urn", urn.toString());

    String entityType = events.get(0).getEntitySpec().getName();
    combinedDocument.put("_entityType", entityType);

    ObjectNode aspectsNode = JsonNodeFactory.instance.objectNode();

    boolean hasAnyAspects = false;

    for (MCLItem event : events) {
      try {
        AspectSpec aspectSpec = event.getAspectSpec();
        String aspectName = aspectSpec.getName();

        if (Constants.STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectName)) {
          processStructuredPropertiesAspect(opContext, urn, event, combinedDocument);
          hasAnyAspects = true;
          continue;
        }

        ObjectNode aspectDocument = processAspectForCombinedDocument(opContext, urn, event);
        if (aspectDocument != null) {
          aspectsNode.set(aspectName, aspectDocument);
          hasAnyAspects = true;

          if (aspectSpec.isTimeseries()) {
            log.debug(
                "Included timeseries aspect {} in combined search document for URN: {}",
                aspectName,
                urn);
          } else {
            log.debug(
                "Included versioned aspect {} in combined search document for URN: {}",
                aspectName,
                urn);
          }
        }

      } catch (Exception e) {
        log.error(
            "Error processing aspect {} for URN {}: {}",
            event.getAspectName(),
            urn,
            e.getMessage(),
            e);
      }
    }

    if (hasAnyAspects && aspectsNode.size() > 0) {
      combinedDocument.set(MappingConstants.ASPECTS_FIELD_NAME, aspectsNode);
    }

    return hasAnyAspects ? combinedDocument : null;
  }

  private ObjectNode processAspectForCombinedDocument(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull MCLItem event) {

    AspectSpec aspectSpec = event.getAspectSpec();
    RecordTemplate aspect = event.getRecordTemplate();
    SystemMetadata systemMetadata = event.getSystemMetadata();

    try {
      boolean isDelete = event.getChangeType() == ChangeType.DELETE;
      Optional<ObjectNode> searchDocument =
          searchDocumentTransformer
              .transformAspect(opContext, urn, aspect, aspectSpec, isDelete, event.getAuditStamp())
              .map(
                  objectNode ->
                      SearchDocumentTransformer.withSystemCreated(
                          objectNode,
                          event.getChangeType(),
                          event.getEntitySpec(),
                          aspectSpec,
                          event.getAuditStamp()));

      if (searchDocument.isEmpty()) {
        return null;
      }

      ObjectNode aspectNode = searchDocument.get();

      aspectNode.remove("urn");

      if (systemMetadata != null) {
        String systemMetadataJson = RecordUtils.toJsonString(systemMetadata);
        ObjectMapper mapper = opContext.getObjectMapper();
        JsonNode systemMetadataNode = mapper.readTree(systemMetadataJson);
        aspectNode.set("_systemmetadata", systemMetadataNode);
      }

      return aspectNode;

    } catch (Exception e) {
      log.error(
          "Error transforming aspect {} for URN {}: {}",
          aspectSpec.getName(),
          urn,
          e.getMessage(),
          e);
      return null;
    }
  }

  private void processStructuredPropertiesAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull MCLItem event,
      @Nonnull ObjectNode combinedDocument) {

    try {
      AspectSpec aspectSpec = event.getAspectSpec();
      RecordTemplate aspect = event.getRecordTemplate();

      Optional<ObjectNode> searchDocument =
          searchDocumentTransformer.transformAspect(
              opContext, urn, aspect, aspectSpec, false, event.getAuditStamp());

      if (searchDocument.isPresent()) {
        ObjectNode structuredPropsDoc = searchDocument.get();

        Iterator<String> fieldNames = structuredPropsDoc.fieldNames();
        if (fieldNames != null) {
          fieldNames.forEachRemaining(
              fieldName -> {
                if (!"urn".equals(fieldName)) {
                  combinedDocument.set(fieldName, structuredPropsDoc.get(fieldName));
                }
              });
        }
      }

    } catch (Exception e) {
      log.error("Error processing structured properties for URN {}: {}", urn, e.getMessage(), e);
    }
  }
}
