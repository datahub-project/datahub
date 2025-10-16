package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ArrayPrimaryKeyInput;
import com.linkedin.datahub.graphql.generated.PatchEntityInput;
import com.linkedin.datahub.graphql.generated.PatchOperationInput;
import com.linkedin.datahub.graphql.generated.PatchOperationType;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.SystemMetadataInput;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import jakarta.json.JsonPatchBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PatchResolverUtils {

  /** Resolves entity URN from input - requires a valid URN to be provided */
  @Nonnull
  public static Urn resolveEntityUrn(@Nonnull String urn, @Nullable String entityType)
      throws Exception {
    if (urn != null && !urn.isEmpty()) {
      // Use provided URN
      return UrnUtils.getUrn(urn);
    } else {
      throw new IllegalArgumentException(
          "URN must be provided for patch operations. "
              + "Please provide a valid URN for the entity you want to patch.");
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
      // Validate and transform patch operations for entity-specific rules
      List<PatchOperationInput> validatedOperations =
          validateAndTransformPatchOperations(patchOperations, context);

      // Check if we should use GenericJsonPatch (like OpenAPI does)
      boolean useGenericPatch =
          (forceGenericPatch != null && forceGenericPatch)
              || (arrayPrimaryKeys != null && !arrayPrimaryKeys.isEmpty());

      if (useGenericPatch) {
        return createGenericJsonPatchAspect(validatedOperations, arrayPrimaryKeys, forceGenericPatch, context);
      } else {
        // Use traditional JsonPatch approach for backward compatibility
        return createLegacyPatchAspect(validatedOperations, context);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create patch aspect", e);
    }
  }

  /** Legacy patch aspect creation method using traditional JsonPatch */
  @Nonnull
  private static GenericAspect createLegacyPatchAspect(
      @Nonnull List<PatchOperationInput> validatedOperations, @Nonnull QueryContext context) {
    try {
      // Convert patch operations to JSON patch format
      List<Map<String, Object>> patchOps =
          validatedOperations.stream()
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
      patchOp.put("value", processPatchValue(operation.getValue(), context));
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
      String strValue = (String) value;
      // Explicitly handle empty strings to ensure they're preserved
      return jakarta.json.Json.createValue(strValue);
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
  public static MetadataChangeProposal createMetadataChangeProposal(
      @Nonnull Urn entityUrn,
      @Nonnull String aspectName,
      @Nonnull GenericAspect patchAspect,
      @Nullable SystemMetadataInput systemMetadataInput,
      @Nullable List<StringMapEntryInput> headers)
      throws JsonProcessingException {

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(entityUrn);
    mcp.setAspectName(aspectName);
    mcp.setEntityType(entityUrn.getEntityType());
    mcp.setChangeType(ChangeType.PATCH);
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

  /**
   * Creates MetadataChangeProposals for patch operations
   *
   * @param inputs List of patch entity inputs
   * @param context Query context
   * @param entityRegistry Entity registry for validation
   * @return List of MetadataChangeProposals
   * @throws Exception if validation or MCP creation fails
   */
  @Nonnull
  public static List<MetadataChangeProposal> createPatchEntitiesMcps(
      @Nonnull List<PatchEntityInput> inputs,
      @Nonnull QueryContext context,
      @Nonnull com.linkedin.metadata.models.registry.EntityRegistry entityRegistry)
      throws Exception {

    final List<MetadataChangeProposal> mcps = new ArrayList<>();

    for (int i = 0; i < inputs.size(); i++) {
      final PatchEntityInput input = inputs.get(i);

      try {
        // Resolve URN
        final Urn entityUrn = resolveEntityUrn(input.getUrn(), input.getEntityType());

        // Validate aspect exists
        final var aspectSpec =
            entityRegistry
                .getEntitySpec(entityUrn.getEntityType())
                .getAspectSpec(input.getAspectName());
        if (aspectSpec == null) {
          throw new IllegalArgumentException(
              "Aspect "
                  + input.getAspectName()
                  + " not found for entity type "
                  + entityUrn.getEntityType());
        }

        // Create patch aspect
        GenericAspect aspect =
            createPatchAspect(
                input.getPatch(),
                input.getArrayPrimaryKeys(),
                input.getForceGenericPatch(),
                context);

        // Create MetadataChangeProposal
        final MetadataChangeProposal mcp =
            createMetadataChangeProposal(
                entityUrn,
                input.getAspectName(),
                aspect,
                input.getSystemMetadata(),
                input.getHeaders());

        mcps.add(mcp);

        log.debug("Created MCP for input {}: {}", i, entityUrn);
      } catch (Exception e) {
        log.error(
            "Failed to create MCP for input {} ({}): {}", i, input.getUrn(), e.getMessage(), e);
        throw new RuntimeException(
            "Failed to create MCP for input " + i + ": " + e.getMessage(), e);
      }
    }

    return mcps;
  }

  /**
   * Checks authorization for patch operations
   *
   * @param input Patch entity input
   * @param context Query context
   * @return true if authorized, false otherwise
   */
  public static boolean isAuthorizedForPatch(
      @Nonnull PatchEntityInput input, @Nonnull QueryContext context) {

    // For patch operations, we need EDIT_ENTITY_PRIVILEGE
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()))));

    // Use entity type from URN if not provided in input
    String entityType = input.getEntityType();
    if (entityType == null && input.getUrn() != null) {
      try {
        entityType = UrnUtils.getUrn(input.getUrn()).getEntityType();
      } catch (Exception e) {
        log.warn("Failed to extract entity type from URN: {}", input.getUrn(), e);
      }
    }

    return AuthorizationUtils.isAuthorized(context, entityType, input.getUrn(), orPrivilegeGroups);
  }

  /**
   * Checks authorization for all entities in a batch
   *
   * @param inputs List of patch entity inputs
   * @param context Query context
   * @throws AuthorizationException if any entity is not authorized
   */
  public static void checkBatchAuthorization(
      @Nonnull List<PatchEntityInput> inputs, @Nonnull QueryContext context) {

    for (int i = 0; i < inputs.size(); i++) {
      PatchEntityInput input = inputs.get(i);
      if (!isAuthorizedForPatch(input, context)) {
        throw new AuthorizationException(
            context.getAuthentication().getActor().toUrnStr()
                + " is unauthorized to update entity "
                + input.getUrn());
      }
    }
  }

  /**
   * Validates and transforms patch operations for entity-specific rules
   *
   * @param operations List of patch operations to validate
   * @param context Query context
   * @return List of validated and transformed patch operations
   */
  @Nonnull
  private static List<PatchOperationInput> validateAndTransformPatchOperations(
      @Nonnull List<PatchOperationInput> operations, @Nonnull QueryContext context) {

    return operations.stream()
        .map(
            op -> {
              // Handle glossary-specific validation
              if (isGlossaryDefinitionOperation(op)) {
                return transformGlossaryDefinitionOperation(op);
              }
              return op;
            })
        .collect(Collectors.toList());
  }

  /**
   * Checks if a patch operation is targeting a glossary definition field
   *
   * @param op Patch operation to check
   * @return true if this is a glossary definition operation
   */
  private static boolean isGlossaryDefinitionOperation(@Nonnull PatchOperationInput op) {
    return "/definition".equals(op.getPath())
        && (op.getOp() == PatchOperationType.ADD || op.getOp() == PatchOperationType.REPLACE);
  }

  /**
   * Transforms a glossary definition operation to handle null values properly
   *
   * @param op Original patch operation
   * @return Transformed patch operation
   */
  @Nonnull
  private static PatchOperationInput transformGlossaryDefinitionOperation(
      @Nonnull PatchOperationInput op) {
    PatchOperationInput transformed = new PatchOperationInput();
    transformed.setOp(op.getOp());
    transformed.setPath(op.getPath());
    transformed.setFrom(op.getFrom());

    // Convert null to empty string for glossary definitions
    String value = op.getValue();
    if (value == null || "null".equals(value) || "\"null\"".equals(value)) {
      transformed.setValue("\"\""); // Empty string
    } else {
      transformed.setValue(value);
    }

    return transformed;
  }

  /**
   * Creates a GenericJsonPatch aspect from validated operations
   *
   * @param validatedOperations List of validated patch operations
   * @param arrayPrimaryKeys Optional array primary keys
   * @param forceGenericPatch Whether to force generic patch
   * @param context Query context
   * @return GenericAspect for GenericJsonPatch
   */
  @Nonnull
  private static GenericAspect createGenericJsonPatchAspect(
      @Nonnull List<PatchOperationInput> validatedOperations,
      @Nullable List<ArrayPrimaryKeyInput> arrayPrimaryKeys,
      @Nullable Boolean forceGenericPatch,
      @Nonnull QueryContext context) {
    try {
      // Convert to GenericJsonPatch format (same as OpenAPI)
      List<GenericJsonPatch.PatchOp> patchOps =
          validatedOperations.stream()
              .map(
                  op -> {
                    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
                    patchOp.setOp(op.getOp().toString().toLowerCase());
                    patchOp.setPath(op.getPath());
                    // Always set value for ADD operations (Jakarta JSON Patch requirement)
                    if (op.getOp().toString().equals("ADD") || op.getValue() != null) {
                      patchOp.setValue(processPatchValue(op.getValue(), context));
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
    } catch (Exception e) {
      throw new RuntimeException("Failed to create GenericJsonPatch aspect", e);
    }
  }

  /**
   * Processes patch operation values with consistent JSON parsing and empty string handling
   *
   * @param value Raw value from patch operation
   * @param context Query context for object mapper
   * @return Processed value object
   */
  @Nullable
  private static Object processPatchValue(@Nullable String value, @Nonnull QueryContext context) {
    if (value == null) {
      return null;
    }

    // Handle empty string specially to preserve it
    if (value.equals("\"\"") || value.equals("")) {
      return ""; // Preserve empty string
    }

    // Try to parse as JSON, fallback to string
    try {
      ObjectMapper mapper = context.getOperationContext().getObjectMapper();
      JsonNode valueNode = mapper.readTree(value);
      return mapper.treeToValue(valueNode, Object.class);
    } catch (JsonProcessingException e) {
      return value; // Fallback to string
    }
  }
}
