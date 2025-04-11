package com.linkedin.metadata.entity.ebean.batch;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.batch.PatchMCP;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.aspect.patch.template.common.GenericPatchTemplate;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatch;
import jakarta.json.JsonReader;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class PatchItemImpl implements PatchMCP {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  // urn an urn associated with the new aspect
  private final Urn urn;
  // aspectName name of the aspect being inserted
  private final String aspectName;
  private SystemMetadata systemMetadata;
  private final AuditStamp auditStamp;

  @Nonnull private final JsonPatch patch;
  @Nullable private final GenericJsonPatch genericJsonPatch;

  private final MetadataChangeProposal metadataChangeProposal;

  // derived
  private final EntitySpec entitySpec;
  private final AspectSpec aspectSpec;

  @Nonnull
  @Override
  public ChangeType getChangeType() {
    return ChangeType.PATCH;
  }

  @Nullable
  @Override
  public RecordTemplate getRecordTemplate() {
    return null;
  }

  @Nonnull
  public MetadataChangeProposal getMetadataChangeProposal() {
    if (metadataChangeProposal != null) {
      return metadataChangeProposal;
    } else {
      final MetadataChangeProposal mcp = new MetadataChangeProposal();
      mcp.setEntityUrn(getUrn());
      mcp.setChangeType(getChangeType());
      mcp.setEntityType(getEntitySpec().getName());
      mcp.setAspectName(getAspectName());
      mcp.setAspect(GenericRecordUtils.serializePatch(getPatch()));
      mcp.setSystemMetadata(getSystemMetadata());
      mcp.setEntityKeyAspect(
          GenericRecordUtils.serializeAspect(
              EntityKeyUtils.convertUrnToEntityKey(getUrn(), entitySpec.getKeyAspectSpec())));
      return mcp;
    }
  }

  @Override
  public void setSystemMetadata(@Nonnull SystemMetadata systemMetadata) {
    this.systemMetadata = systemMetadata;
    if (this.metadataChangeProposal != null) {
      this.metadataChangeProposal.setSystemMetadata(systemMetadata);
    }
  }

  public ChangeItemImpl applyPatch(RecordTemplate recordTemplate, AspectRetriever aspectRetriever) {
    if (genericJsonPatch != null) {
      if (!genericJsonPatch.getArrayPrimaryKeys().isEmpty()
          || genericJsonPatch.isForceGenericPatch()) {
        return applyGenericPatch(recordTemplate, aspectRetriever);
      }
    }
    return applyTemplatePatch(recordTemplate, aspectRetriever);
  }

  private ChangeItemImpl applyGenericPatch(
      RecordTemplate recordTemplate, AspectRetriever aspectRetriever) {
    try {
      GenericPatchTemplate<? extends RecordTemplate> genericPatchTemplate =
          GenericPatchTemplate.builder()
              .genericJsonPatch(genericJsonPatch)
              .templateType(aspectSpec.getDataTemplateClass())
              .templateDefault(
                  aspectSpec.getDataTemplateClass().getDeclaredConstructor().newInstance())
              .build();
      return ChangeItemImpl.fromPatch(
          urn, aspectSpec, recordTemplate, genericPatchTemplate, auditStamp, aspectRetriever);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ChangeItemImpl applyTemplatePatch(
      RecordTemplate recordTemplate, AspectRetriever aspectRetriever) {
    ChangeItemImpl.ChangeItemImplBuilder builder =
        ChangeItemImpl.builder()
            .urn(getUrn())
            .aspectName(getAspectName())
            .metadataChangeProposal(getMetadataChangeProposal())
            .auditStamp(auditStamp)
            .systemMetadata(getSystemMetadata());

    AspectTemplateEngine aspectTemplateEngine =
        aspectRetriever.getEntityRegistry().getAspectTemplateEngine();

    RecordTemplate currentValue =
        recordTemplate != null
            ? recordTemplate
            : aspectTemplateEngine.getDefaultTemplate(getAspectName());

    if (currentValue == null) {
      // Attempting to patch a value to an aspect which has no default value and no existing value.
      throw new UnsupportedOperationException(
          String.format(
              "Patch not supported for aspect with name %s. "
                  + "Default aspect is required because no aspect currently exists for urn %s.",
              getAspectName(), getUrn()));
    }

    try {
      builder.recordTemplate(
          aspectTemplateEngine.applyPatch(currentValue, getPatch(), getAspectSpec()));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return builder.build(aspectRetriever);
  }

  public static class PatchItemImplBuilder {

    // Ensure use of other builders
    private PatchItemImpl build() {
      return null;
    }

    public PatchItemImpl.PatchItemImplBuilder systemMetadata(SystemMetadata systemMetadata) {
      this.systemMetadata = SystemMetadataUtils.generateSystemMetadataIfEmpty(systemMetadata);
      return this;
    }

    public PatchItemImpl.PatchItemImplBuilder aspectSpec(AspectSpec aspectSpec) {
      if (!MCPItem.isValidChangeType(ChangeType.PATCH, aspectSpec)) {
        throw new UnsupportedOperationException(
            "ChangeType not supported: " + ChangeType.PATCH + " for aspect " + this.aspectName);
      }
      this.aspectSpec = aspectSpec;
      return this;
    }

    public PatchItemImpl.PatchItemImplBuilder patch(JsonPatch patch) {
      if (patch == null) {
        throw new IllegalArgumentException(String.format("Missing patch to apply. Item: %s", this));
      }
      this.patch = patch;
      return this;
    }

    public PatchItemImpl build(EntityRegistry entityRegistry) {
      urn(ValidationApiUtils.validateUrn(entityRegistry, this.urn));
      log.debug("entity type = {}", this.urn.getEntityType());

      entitySpec(ValidationApiUtils.validateEntity(entityRegistry, this.urn.getEntityType()));
      log.debug("entity spec = {}", this.entitySpec);

      aspectSpec(ValidationApiUtils.validateAspect(this.entitySpec, this.aspectName));
      log.debug("aspect spec = {}", this.aspectSpec);

      if (this.systemMetadata == null) {
        // generate default
        systemMetadata(null);
      }

      return new PatchItemImpl(
          this.urn,
          this.aspectName,
          this.systemMetadata,
          this.auditStamp,
          Objects.requireNonNull(this.patch),
          this.genericJsonPatch,
          this.metadataChangeProposal,
          this.entitySpec,
          this.aspectSpec);
    }

    public PatchItemImpl build(
        MetadataChangeProposal mcp, AuditStamp auditStamp, EntityRegistry entityRegistry) {

      // Validation includes: Urn, Entity, Aspect
      this.metadataChangeProposal = ValidationApiUtils.validateMCP(entityRegistry, mcp);
      this.urn = this.metadataChangeProposal.getEntityUrn(); // validation ensures existence
      this.auditStamp = auditStamp;
      this.aspectName = mcp.getAspectName();
      systemMetadata(mcp.getSystemMetadata());
      Pair<JsonPatch, Optional<GenericJsonPatch>> parsedJson = convertToJsonPatch(mcp);
      patch(parsedJson.getFirst());
      parsedJson.getSecond().ifPresent(generic -> this.genericJsonPatch = generic);

      entitySpec(entityRegistry.getEntitySpec(this.urn.getEntityType())); // prior validation
      aspectSpec(entitySpec.getAspectSpec(this.aspectName)); // prior validation

      return new PatchItemImpl(
          this.urn,
          this.aspectName,
          this.systemMetadata,
          this.auditStamp,
          this.patch,
          this.genericJsonPatch,
          this.metadataChangeProposal,
          this.entitySpec,
          this.aspectSpec);
    }

    private static Pair<JsonPatch, Optional<GenericJsonPatch>> convertToJsonPatch(
        MetadataChangeProposal mcp) {
      try {
        String jsonString = mcp.getAspect().getValue().asString(StandardCharsets.UTF_8);
        JsonReader reader = Json.createReader(new StringReader(jsonString));

        // Check if the JSON contains a "patch" key
        JsonStructure jsonStructure = reader.read();
        JsonPatch jsonPatch;
        Optional<GenericJsonPatch> genericJsonPatch = Optional.empty();

        if (jsonStructure.getValueType() == JsonValue.ValueType.OBJECT) {
          JsonObject jsonObject = (JsonObject) jsonStructure;

          if (jsonObject.containsKey("patch")) {
            // If "patch" key exists, read the array from this key
            jsonPatch = Json.createPatch(jsonObject.getJsonArray("patch"));

            // Convert to GenericJsonPatch
            genericJsonPatch =
                Optional.of(OBJECT_MAPPER.readValue(jsonString, GenericJsonPatch.class));

            return Pair.of(jsonPatch, genericJsonPatch);
          }
        }

        // If no "patch" key or not an object, fallback to original behavior
        jsonPatch = Json.createPatch(Json.createReader(new StringReader(jsonString)).readArray());

        return Pair.of(jsonPatch, genericJsonPatch);
      } catch (RuntimeException e) {
        throw new IllegalArgumentException(
            "Invalid JSON Patch: " + mcp.getAspect().getValue().asString(StandardCharsets.UTF_8),
            e);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean isDatabaseDuplicateOf(BatchItem other) {
    return equals(other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PatchItemImpl that = (PatchItemImpl) o;
    return urn.equals(that.urn)
        && aspectName.equals(that.aspectName)
        && Objects.equals(systemMetadata, that.systemMetadata)
        && auditStamp.equals(that.auditStamp)
        && patch.equals(that.patch)
        && Objects.equals(genericJsonPatch, that.genericJsonPatch);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn, aspectName, systemMetadata, auditStamp, patch, genericJsonPatch);
  }

  @Override
  public String toString() {
    return "PatchBatchItem{"
        + "urn="
        + urn
        + ", aspectName='"
        + aspectName
        + '\''
        + ", systemMetadata="
        + systemMetadata
        + ", patch="
        + patch
        + '}';
  }
}
