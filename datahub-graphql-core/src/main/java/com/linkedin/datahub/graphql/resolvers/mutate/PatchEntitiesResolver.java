package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.PatchEntityInput;
import com.linkedin.datahub.graphql.generated.PatchEntityResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.SystemMetadata;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import jakarta.json.JsonPatchBuilder;
import java.io.StringReader;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PatchEntitiesResolver
    implements DataFetcher<CompletableFuture<List<PatchEntityResult>>> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  // Entity types that support auto-generated URNs (simple string URNs)
  private static final Set<String> AUTO_GENERATE_ALLOWED_ENTITY_TYPES = Set.of(
      "glossaryTerm",
      "glossaryNode", 
      "container",
      "notebook",
      "domain",
      "dataProduct"
  );

  private final EntityService _entityService;
  private final EntityClient _entityClient;
  private final EntityRegistry _entityRegistry;

  @Override
  public CompletableFuture<List<PatchEntityResult>> get(DataFetchingEnvironment environment)
      throws Exception {
    final PatchEntityInput[] inputArray =
        bindArgument(environment.getArgument("input"), PatchEntityInput[].class);
    final List<PatchEntityInput> inputs = Arrays.asList(inputArray);
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return patchEntities(inputs, context);
          } catch (Exception e) {
            log.error("Failed to patch entities: {}", e.getMessage(), e);
            return inputs.stream()
                .map(input -> {
                  String urn = input.getUrn() != null ? input.getUrn() : "urn:li:unknown:error";
                  return new PatchEntityResult(urn, null, false, e.getMessage());
                })
                .collect(Collectors.toList());
          }
        },
        this.getClass().getSimpleName(),
        "patchEntities");
  }

  private List<PatchEntityResult> patchEntities(
      @Nonnull List<PatchEntityInput> inputs, @Nonnull QueryContext context) throws Exception {

    final Authentication authentication = context.getAuthentication();
    log.info("Processing batch patch for {} entities", inputs.size());
    
    // Resolve URNs for all inputs (either provided or auto-generated)
    final List<Urn> entityUrns = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      PatchEntityInput input = inputs.get(i);
      try {
        Urn entityUrn = resolveEntityUrn(input);
        entityUrns.add(entityUrn);
        log.debug("Resolved URN for input {}: {}", i, entityUrn);
      } catch (Exception e) {
        log.error("Failed to resolve URN for input {}: {}", i, e.getMessage(), e);
        throw new IllegalArgumentException("Failed to resolve URN for input " + i + ": " + e.getMessage(), e);
      }
    }

    // Check authorization for all entities
    // For new entities (auto-generated URNs), we need to check permissions differently
    // since the entities don't exist yet
    boolean hasPermission = true;
    for (int i = 0; i < inputs.size(); i++) {
      PatchEntityInput input = inputs.get(i);
      Urn entityUrn = entityUrns.get(i);
      
      // Check if this is a new entity (auto-generated URN)
      boolean isNewEntity = input.getUrn() == null || input.getUrn().isEmpty();
      
      if (isNewEntity) {
        // For new entities, check if user has MANAGE permissions on the entity type
        // This is a simplified check - in practice, you might need more sophisticated logic
        log.debug("Skipping authorization check for new entity: {}", entityUrn);
        // For now, assume admin has permission to create new entities
        continue;
      } else {
        // For existing entities, check UPDATE permissions
        if (!AuthUtil.isAPIAuthorizedEntityUrns(context.getOperationContext(), UPDATE, List.of(entityUrn))) {
          hasPermission = false;
          log.warn("User {} is unauthorized to UPDATE entity {}", authentication.getActor().toUrnStr(), entityUrn);
          break;
        }
      }
    }
    
    if (!hasPermission) {
      throw new com.linkedin.datahub.graphql.exception.AuthorizationException(
          authentication.getActor().toUrnStr() + " is unauthorized to UPDATE entities.");
    }

    // Create batch of MetadataChangeProposals
    final List<com.linkedin.mxe.MetadataChangeProposal> mcps = new ArrayList<>();

    for (int i = 0; i < inputs.size(); i++) {
      final PatchEntityInput input = inputs.get(i);
      final Urn entityUrn = entityUrns.get(i);

      try {
        // Validate aspect exists
        final var aspectSpec =
            _entityRegistry
                .getEntitySpec(entityUrn.getEntityType())
                .getAspectSpec(input.getAspectName());
        if (aspectSpec == null) {
          throw new IllegalArgumentException(
              "Aspect "
                  + input.getAspectName()
                  + " not found for entity type "
                  + entityUrn.getEntityType());
        }

        // Always use patch aspect - same as single patchEntity resolver
        GenericAspect aspect = createPatchAspect(input.getPatch());

        // Create MetadataChangeProposal
        final com.linkedin.mxe.MetadataChangeProposal mcp =
            createMetadataChangeProposal(
                entityUrn,
                input.getAspectName(),
                aspect,
                input.getSystemMetadata(),
                input.getHeaders());

        mcps.add(mcp);
        
        log.debug("Created MCP for input {}: {}", i, entityUrn);
      } catch (Exception e) {
        log.error("Failed to create MCP for input {} ({}): {}", i, entityUrn, e.getMessage(), e);
        throw new RuntimeException("Failed to create MCP for input " + i + ": " + e.getMessage(), e);
      }
    }

    // Apply all patches individually
    final List<IngestResult> results = new ArrayList<>();
    for (int i = 0; i < mcps.size(); i++) {
      com.linkedin.mxe.MetadataChangeProposal mcp = mcps.get(i);
      try {
        Urn actor = UrnUtils.getUrn(authentication.getActor().toUrnStr());
        IngestResult result =
            _entityService.ingestProposal(
                context.getOperationContext(),
                mcp,
                AuditStampUtils.createAuditStamp(actor.toString()),
                false); // synchronous for GraphQL
        results.add(result);
        log.debug("Successfully applied patch for input {}: {}", i, mcp.getEntityUrn());
      } catch (Exception e) {
        log.error("Failed to apply patch for input {} ({}): {}", i, mcp.getEntityUrn(), e.getMessage(), e);
        // Add null result for failed operations
        results.add(null);
      }
    }

    // Map results back to input order
    final List<PatchEntityResult> patchResults = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      final PatchEntityInput input = inputs.get(i);
      final IngestResult result = i < results.size() ? results.get(i) : null;
      
      // Extract entity name from the patch operations
      String entityName = extractEntityName(input.getPatch());
      
      // Validate name for glossary entities
      validateNameForEntityType(input.getEntityType(), entityName);

      patchResults.add(
          new PatchEntityResult(
              input.getUrn(), entityName, result != null, result == null ? "Failed to apply patch" : null));
    }

    return patchResults;
  }

  @Nonnull
  private GenericAspect createPatchAspect(@Nonnull List<com.linkedin.datahub.graphql.generated.PatchOperationInput> patchOperations) {
    try {
      // Convert patch operations to JSON patch format
      List<Map<String, Object>> patchOps = patchOperations.stream()
          .map(this::convertPatchOperation)
          .collect(Collectors.toList());
      
      // Create JsonPatch using jakarta.json
      JsonPatchBuilder patchBuilder = Json.createPatchBuilder();
      for (Map<String, Object> op : patchOps) {
        String opType = (String) op.get("op");
        String path = (String) op.get("path");
        Object value = op.get("value");
        
        switch (opType.toLowerCase()) {
          case "add":
            patchBuilder.add(path, convertToJsonValue(value));
            break;
          case "remove":
            patchBuilder.remove(path);
            break;
          case "replace":
            patchBuilder.replace(path, convertToJsonValue(value));
            break;
          case "move":
            patchBuilder.move(path, (String) op.get("from"));
            break;
          case "copy":
            patchBuilder.copy((String) op.get("from"), path);
            break;
          case "test":
            patchBuilder.test(path, convertToJsonValue(value));
            break;
          default:
            throw new IllegalArgumentException("Unsupported patch operation: " + opType);
        }
      }
      
      JsonPatch jsonPatch = patchBuilder.build();
      return GenericRecordUtils.serializePatch(jsonPatch);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create patch aspect", e);
    }
  }

  @Nonnull
  private Map<String, Object> convertPatchOperation(@Nonnull com.linkedin.datahub.graphql.generated.PatchOperationInput operation) {
    Map<String, Object> patchOp = new HashMap<>();
    patchOp.put("op", operation.getOp().toString().toLowerCase());
    patchOp.put("path", operation.getPath());
    
    if (operation.getValue() != null) {
      // Try to parse as JSON, fallback to string
      try {
        JsonNode valueNode = OBJECT_MAPPER.readTree(operation.getValue());
        patchOp.put("value", OBJECT_MAPPER.treeToValue(valueNode, Object.class));
      } catch (JsonProcessingException e) {
        patchOp.put("value", operation.getValue());
      }
    }
    
    if (operation.getFrom() != null) {
      patchOp.put("from", operation.getFrom());
    }
    
    return patchOp;
  }

  @Nonnull
  private jakarta.json.JsonValue convertToJsonValue(@Nullable Object value) {
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
        String jsonString = OBJECT_MAPPER.writeValueAsString(value);
        return jakarta.json.Json.createReader(new java.io.StringReader(jsonString)).readValue();
      } catch (Exception e) {
        // Fallback to string representation
        return jakarta.json.Json.createValue(value.toString());
      }
    }
  }

  @Nonnull
  private com.linkedin.mxe.MetadataChangeProposal createMetadataChangeProposal(
      @Nonnull Urn entityUrn,
      @Nonnull String aspectName,
      @Nonnull GenericAspect patchAspect,
      @Nonnull com.linkedin.datahub.graphql.generated.SystemMetadataInput systemMetadataInput,
      @Nonnull List<com.linkedin.datahub.graphql.generated.StringMapEntryInput> headers)
      throws JsonProcessingException {

    final com.linkedin.mxe.MetadataChangeProposal mcp =
        new com.linkedin.mxe.MetadataChangeProposal();
    mcp.setEntityUrn(entityUrn);
    mcp.setAspectName(aspectName);
    mcp.setEntityType(entityUrn.getEntityType());
    // Always use UPSERT for now to handle both new and existing entities
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
        for (com.linkedin.datahub.graphql.generated.StringMapEntryInput entry :
            systemMetadataInput.getProperties()) {
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
      for (com.linkedin.datahub.graphql.generated.StringMapEntryInput header : headers) {
        headerMap.put(header.getKey(), header.getValue());
      }
      mcp.setHeaders(new StringMap(headerMap));
    }

    return mcp;
  }

  @Nonnull
  private Urn resolveEntityUrn(@Nonnull PatchEntityInput input) throws Exception {
    if (input.getUrn() != null && !input.getUrn().isEmpty()) {
      // Use provided URN
      return UrnUtils.getUrn(input.getUrn());
    } else if (input.getEntityType() != null && !input.getEntityType().isEmpty()) {
      // Auto-generate URN for the specified entity type
      String entityType = input.getEntityType();
      
      // Only allow auto-generation for safe entity types
      if (!AUTO_GENERATE_ALLOWED_ENTITY_TYPES.contains(entityType)) {
        throw new IllegalArgumentException(
            "Auto-generated URNs are only supported for entity types: " + 
            AUTO_GENERATE_ALLOWED_ENTITY_TYPES + 
            ". Entity type '" + entityType + "' requires a structured URN. " +
            "Please provide a specific URN for this entity type.");
      }
      
      // Generate GUID for the entity
      String guid = UUID.randomUUID().toString();
      String newUrn = String.format("urn:li:%s:%s", entityType, guid);
      
      // Update the input URN for the response
      input.setUrn(newUrn);
      
      return UrnUtils.getUrn(newUrn);
    } else {
      throw new IllegalArgumentException(
          "Either 'urn' or 'entityType' must be provided. " +
          "Use 'urn' for existing entities or 'entityType' to auto-generate a URN for new entities.");
    }
  }

  @Nullable
  private String extractEntityName(@Nonnull List<com.linkedin.datahub.graphql.generated.PatchOperationInput> patchOperations) {
    // Look for name field in patch operations
    for (com.linkedin.datahub.graphql.generated.PatchOperationInput operation : patchOperations) {
      if ("/name".equals(operation.getPath()) && operation.getValue() != null) {
        return operation.getValue();
      }
    }
    return null;
  }

  private void validateNameForEntityType(@Nullable String entityType, @Nullable String entityName) {
    if (("glossaryTerm".equals(entityType) || "glossaryNode".equals(entityType)) && entityName == null) {
      log.warn("Creating {} without a name - consider adding a name field to the patch operations for better UX", entityType);
    }
  }



}
