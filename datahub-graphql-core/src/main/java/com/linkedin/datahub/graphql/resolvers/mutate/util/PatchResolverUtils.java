package com.linkedin.datahub.graphql.resolvers.mutate.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ArrayPrimaryKeyInput;
import com.linkedin.datahub.graphql.generated.PatchEntityInput;
import com.linkedin.datahub.graphql.generated.PatchOperationInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.SystemMetadataInput;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
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
      // Validate patch operations
      List<PatchOperationInput> validatedOperations =
          validateAndTransformPatchOperations(patchOperations, context);

      // Always use GenericJsonPatch format (matches OpenAPI exactly)
      return createGenericJsonPatchAspect(
          validatedOperations, arrayPrimaryKeys, forceGenericPatch, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create patch aspect", e);
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
      if (!AuthorizationUtils.isAuthorizedForPatch(input, context)) {
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
    return operations; // No transformation needed
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

      // Serialize GenericJsonPatch directly (fixes ClassCastException)
      ObjectMapper mapper = context.getOperationContext().getObjectMapper();
      return GenericRecordUtils.serializePatch(genericJsonPatch, mapper);
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
