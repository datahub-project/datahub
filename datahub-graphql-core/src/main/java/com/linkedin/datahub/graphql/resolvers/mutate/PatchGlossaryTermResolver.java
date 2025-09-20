package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.PatchEntityResult;
import com.linkedin.datahub.graphql.generated.PatchGlossaryTermInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.entity.EntityService;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PatchGlossaryTermResolver
    implements DataFetcher<CompletableFuture<PatchEntityResult>> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final EntityService _entityService;
  private final EntityClient _entityClient;
  private final EntityRegistry _entityRegistry;

  @Override
  public CompletableFuture<PatchEntityResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final PatchGlossaryTermInput input =
        bindArgument(environment.getArgument("input"), PatchGlossaryTermInput.class);
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return patchGlossaryTerm(input, context);
          } catch (Exception e) {
            log.error("Failed to patch glossary term: {}", e.getMessage(), e);
            return new PatchEntityResult(input.getUrn(), false, e.getMessage());
          }
        },
        this.getClass().getSimpleName(),
        "patchGlossaryTerm");
  }

  private PatchEntityResult patchGlossaryTerm(
      @Nonnull PatchGlossaryTermInput input, @Nonnull QueryContext context) throws Exception {
    final Urn entityUrn = UrnUtils.getUrn(input.getUrn());
    final Authentication authentication = context.getAuthentication();

    // Validate that this is a glossary term
    if (!"glossaryTerm".equals(entityUrn.getEntityType())) {
      throw new IllegalArgumentException("Entity must be a glossary term");
    }

    // Check authorization using glossary-specific authorization
    if (!GlossaryUtils.canUpdateGlossaryEntity(entityUrn, context, _entityClient)) {
      throw new AuthorizationException(
          authentication.getActor().toUrnStr() + " is unauthorized to update glossary terms.");
    }

    // Validate aspect exists for glossary terms
    final var aspectSpec =
        _entityRegistry
            .getEntitySpec(entityUrn.getEntityType())
            .getAspectSpec(input.getAspectName());
    if (aspectSpec == null) {
      throw new IllegalArgumentException(
          "Aspect " + input.getAspectName() + " not found for glossary terms");
    }

    // Create patch aspect using the same pattern as OpenAPI
    final GenericAspect patchAspect = createPatchAspect(input.getPatch());

    // Create MetadataChangeProposal
    final com.linkedin.mxe.MetadataChangeProposal mcp =
        createMetadataChangeProposal(
            entityUrn,
            input.getAspectName(),
            patchAspect,
            input.getSystemMetadata(),
            input.getHeaders());

    // Apply the patch
    try {
      Urn actor = UrnUtils.getUrn(authentication.getActor().toUrnStr());
      _entityService.ingestProposal(
          context.getOperationContext(),
          mcp,
          AuditStampUtils.createAuditStamp(actor.toString()),
          false); // synchronous for GraphQL

      return new PatchEntityResult(input.getUrn(), true, null);
    } catch (Exception e) {
      return new PatchEntityResult(
          input.getUrn(), false, "Failed to apply patch to glossary term: " + e.getMessage());
    }
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
}
