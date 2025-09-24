package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ArrayPrimaryKeyInput;
import com.linkedin.datahub.graphql.generated.PatchOperationInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.SystemMetadataInput;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.SystemMetadata;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import jakarta.json.JsonPatchBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PatchResolverUtils {

  // Entity types that support auto-generated URNs (simple string URNs)
  public static final Set<String> AUTO_GENERATE_ALLOWED_ENTITY_TYPES =
      Set.of("glossaryTerm", "glossaryNode", "container", "notebook", "domain", "dataProduct");

  /**
   * Resolves entity URN from input - either provided URN or auto-generated for supported entity
   * types
   */
  @Nonnull
  public static Urn resolveEntityUrn(@Nonnull String urn, @Nullable String entityType)
      throws Exception {
    if (urn != null && !urn.isEmpty()) {
      // Use provided URN
      return UrnUtils.getUrn(urn);
    } else if (entityType != null && !entityType.isEmpty()) {
      // Auto-generate URN for the specified entity type

      // Only allow auto-generation for safe entity types
      if (!AUTO_GENERATE_ALLOWED_ENTITY_TYPES.contains(entityType)) {
        throw new IllegalArgumentException(
            "Auto-generated URNs are only supported for entity types: "
                + AUTO_GENERATE_ALLOWED_ENTITY_TYPES
                + ". Entity type '"
                + entityType
                + "' requires a structured URN. "
                + "Please provide a specific URN for this entity type.");
      }

      // Generate GUID for the entity
      String guid = UUID.randomUUID().toString();
      String newUrn = String.format("urn:li:%s:%s", entityType, guid);

      return UrnUtils.getUrn(newUrn);
    } else {
      throw new IllegalArgumentException(
          "Either 'urn' or 'entityType' must be provided. "
              + "Use 'urn' for existing entities or 'entityType' to auto-generate a URN for new entities.");
    }
  }

  /** Creates a patch aspect from patch operations (legacy method for backward compatibility) */
  @Nonnull
  public static GenericAspect createPatchAspect(
      @Nonnull List<PatchOperationInput> patchOperations, @Nonnull QueryContext context) {
    return createPatchAspect(patchOperations, null, null, context);
  }

  /**
   * Creates a patch aspect from patch operations with optional array primary keys and force generic
   * patch
   */
  @Nonnull
  public static GenericAspect createPatchAspect(
      @Nonnull List<PatchOperationInput> patchOperations,
      @Nullable List<ArrayPrimaryKeyInput> arrayPrimaryKeys,
      @Nullable Boolean forceGenericPatch,
      @Nonnull QueryContext context) {
    try {
      // Check if we should use GenericJsonPatch (like OpenAPI does)
      boolean useGenericPatch =
          (forceGenericPatch != null && forceGenericPatch)
              || (arrayPrimaryKeys != null && !arrayPrimaryKeys.isEmpty());

      if (useGenericPatch) {
        // Convert to GenericJsonPatch format (same as OpenAPI)
        List<GenericJsonPatch.PatchOp> patchOps =
            patchOperations.stream()
                .map(
                    op -> {
                      GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
                      patchOp.setOp(op.getOp().toString().toLowerCase());
                      patchOp.setPath(op.getPath());
                      if (op.getValue() != null) {
                        // Try to parse as JSON, fallback to string
                        try {
                          ObjectMapper mapper = context.getOperationContext().getObjectMapper();
                          JsonNode valueNode = mapper.readTree(op.getValue());
                          patchOp.setValue(mapper.treeToValue(valueNode, Object.class));
                        } catch (JsonProcessingException e) {
                          patchOp.setValue(op.getValue());
                        }
                      }
                      return patchOp;
                    })
                .collect(Collectors.toList());

        // Convert arrayPrimaryKeys if provided
        Map<String, List<String>> arrayPrimaryKeysMap = new HashMap<>();
        if (arrayPrimaryKeys != null) {
          arrayPrimaryKeys.forEach(
              key -> arrayPrimaryKeysMap.put(key.getArrayField(), key.getKeys()));
        }

        // Create GenericJsonPatch
        GenericJsonPatch genericJsonPatch =
            GenericJsonPatch.builder()
                .patch(patchOps)
                .arrayPrimaryKeys(arrayPrimaryKeysMap)
                .forceGenericPatch(forceGenericPatch != null ? forceGenericPatch : false)
                .build();

        // Convert to JsonNode and serialize (same as OpenAPI does)
        ObjectMapper mapper = context.getOperationContext().getObjectMapper();
        JsonNode patchNode = mapper.valueToTree(genericJsonPatch);
        return GenericRecordUtils.serializePatch(patchNode);
      } else {
        // Use traditional JsonPatch approach for backward compatibility
        return createLegacyPatchAspect(patchOperations, context);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create patch aspect", e);
    }
  }

  /** Legacy patch aspect creation method using traditional JsonPatch */
  @Nonnull
  private static GenericAspect createLegacyPatchAspect(
      @Nonnull List<PatchOperationInput> patchOperations, @Nonnull QueryContext context) {
    try {
      // Convert patch operations to JSON patch format
      List<Map<String, Object>> patchOps =
          patchOperations.stream()
              .map(op -> convertPatchOperation(op, context))
              .collect(Collectors.toList());

      // Create JsonPatch using jakarta.json
      JsonPatchBuilder patchBuilder = Json.createPatchBuilder();
      for (Map<String, Object> op : patchOps) {
        String opType = (String) op.get("op");
        String path = (String) op.get("path");
        Object value = op.get("value");

        switch (opType.toLowerCase()) {
          case "add":
            patchBuilder.add(path, convertToJsonValue(value, context));
            break;
          case "remove":
            patchBuilder.remove(path);
            break;
          case "replace":
            patchBuilder.replace(path, convertToJsonValue(value, context));
            break;
          case "move":
            patchBuilder.move(path, (String) op.get("from"));
            break;
          case "copy":
            patchBuilder.copy((String) op.get("from"), path);
            break;
          case "test":
            patchBuilder.test(path, convertToJsonValue(value, context));
            break;
          default:
            throw new IllegalArgumentException("Unsupported patch operation: " + opType);
        }
      }

      JsonPatch jsonPatch = patchBuilder.build();
      return GenericRecordUtils.serializePatch(jsonPatch);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create legacy patch aspect", e);
    }
  }

  /** Converts a patch operation to Map format */
  @Nonnull
  private static Map<String, Object> convertPatchOperation(
      @Nonnull PatchOperationInput operation, @Nonnull QueryContext context) {
    Map<String, Object> patchOp = new HashMap<>();
    patchOp.put("op", operation.getOp().toString().toLowerCase());
    patchOp.put("path", operation.getPath());

    if (operation.getValue() != null) {
      // Try to parse as JSON, fallback to string
      try {
        ObjectMapper mapper = context.getOperationContext().getObjectMapper();
        JsonNode valueNode = mapper.readTree(operation.getValue());
        patchOp.put("value", mapper.treeToValue(valueNode, Object.class));
      } catch (JsonProcessingException e) {
        patchOp.put("value", operation.getValue());
      }
    }

    if (operation.getFrom() != null) {
      patchOp.put("from", operation.getFrom());
    }

    return patchOp;
  }

  /** Converts an object to jakarta.json.JsonValue */
  @Nonnull
  private static jakarta.json.JsonValue convertToJsonValue(
      @Nullable Object value, @Nonnull QueryContext context) {
    if (value == null) {
      return jakarta.json.JsonValue.NULL;
    } else if (value instanceof String) {
      return jakarta.json.Json.createValue((String) value);
    } else if (value instanceof Integer) {
      return jakarta.json.Json.createValue((Integer) value);
    } else if (value instanceof Long) {
      return jakarta.json.Json.createValue((Long) value);
    } else if (value instanceof Boolean) {
      return ((Boolean) value) ? jakarta.json.JsonValue.TRUE : jakarta.json.JsonValue.FALSE;
    } else if (value instanceof Double) {
      return jakarta.json.Json.createValue((Double) value);
    } else {
      // For complex objects, convert to JSON string first
      try {
        ObjectMapper mapper = context.getOperationContext().getObjectMapper();
        String jsonString = mapper.writeValueAsString(value);
        return jakarta.json.Json.createReader(new java.io.StringReader(jsonString)).readValue();
      } catch (Exception e) {
        // Fallback to string representation
        return jakarta.json.Json.createValue(value.toString());
      }
    }
  }

  /** Creates a MetadataChangeProposal */
  @Nonnull
  public static com.linkedin.mxe.MetadataChangeProposal createMetadataChangeProposal(
      @Nonnull Urn entityUrn,
      @Nonnull String aspectName,
      @Nonnull GenericAspect patchAspect,
      @Nullable SystemMetadataInput systemMetadataInput,
      @Nullable List<StringMapEntryInput> headers)
      throws JsonProcessingException {

    final com.linkedin.mxe.MetadataChangeProposal mcp =
        new com.linkedin.mxe.MetadataChangeProposal();
    mcp.setEntityUrn(entityUrn);
    mcp.setAspectName(aspectName);
    mcp.setEntityType(entityUrn.getEntityType());
    mcp.setChangeType(com.linkedin.events.metadata.ChangeType.PATCH);
    mcp.setAspect(patchAspect);

    // Set system metadata - create default if not provided (matching OpenAPI behavior)
    SystemMetadata systemMetadata;
    if (systemMetadataInput != null) {
      systemMetadata = new SystemMetadata();
      if (systemMetadataInput.getLastObserved() != null) {
        systemMetadata.setLastObserved(systemMetadataInput.getLastObserved());
      }
      if (systemMetadataInput.getRunId() != null) {
        systemMetadata.setRunId(systemMetadataInput.getRunId());
      }
      if (systemMetadataInput.getProperties() != null) {
        final Map<String, String> properties = new HashMap<>();
        for (StringMapEntryInput entry : systemMetadataInput.getProperties()) {
          properties.put(entry.getKey(), entry.getValue());
        }
        systemMetadata.setProperties(new StringMap(properties));
      }
    } else {
      // Create default system metadata like OpenAPI does
      systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();
    }
    mcp.setSystemMetadata(systemMetadata);

    // Set headers if provided
    if (headers != null) {
      final Map<String, String> headerMap = new HashMap<>();
      for (StringMapEntryInput header : headers) {
        headerMap.put(header.getKey(), header.getValue());
      }
      mcp.setHeaders(new StringMap(headerMap));
    }

    return mcp;
  }

  /** Extracts entity name from patch operations */
  @Nullable
  public static String extractEntityName(@Nonnull List<PatchOperationInput> patchOperations) {
    // Look for name field in patch operations
    for (PatchOperationInput operation : patchOperations) {
      if ("/name".equals(operation.getPath()) && operation.getValue() != null) {
        return operation.getValue();
      }
    }
    return null;
  }

  /** Validates name for entity type (logs warning for glossary entities without names) */
  public static void validateNameForEntityType(
      @Nullable String entityType, @Nullable String entityName) {
    if (("glossaryTerm".equals(entityType) || "glossaryNode".equals(entityType))
        && entityName == null) {
      log.warn(
          "Creating {} without a name - consider adding a name field to the patch operations for better UX",
          entityType);
    }
  }
}
