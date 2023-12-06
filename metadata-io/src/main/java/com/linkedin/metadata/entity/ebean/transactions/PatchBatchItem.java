package com.linkedin.metadata.entity.ebean.transactions;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.github.fge.jsonpatch.Patch;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.transactions.AbstractBatchItem;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.template.AspectTemplateEngine;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class PatchBatchItem extends AbstractBatchItem {
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
  private final SystemMetadata systemMetadata;

  private final Patch patch;

  private final MetadataChangeProposal metadataChangeProposal;

  // derived
  private final EntitySpec entitySpec;
  private final AspectSpec aspectSpec;

  @Override
  public ChangeType getChangeType() {
    return ChangeType.PATCH;
  }

  @Override
  public void validateUrn(EntityRegistry entityRegistry, Urn urn) {
    EntityUtils.validateUrn(entityRegistry, urn);
  }

  public UpsertBatchItem applyPatch(EntityRegistry entityRegistry, RecordTemplate recordTemplate) {
    UpsertBatchItem.UpsertBatchItemBuilder builder =
        UpsertBatchItem.builder()
            .urn(getUrn())
            .aspectName(getAspectName())
            .metadataChangeProposal(getMetadataChangeProposal())
            .systemMetadata(getSystemMetadata());

    AspectTemplateEngine aspectTemplateEngine = entityRegistry.getAspectTemplateEngine();

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
      builder.aspect(aspectTemplateEngine.applyPatch(currentValue, getPatch(), getAspectSpec()));
    } catch (JsonProcessingException | JsonPatchException e) {
      throw new RuntimeException(e);
    }

    return builder.build(entityRegistry);
  }

  public static class PatchBatchItemBuilder {

    public PatchBatchItem build(EntityRegistry entityRegistry) {
      EntityUtils.validateUrn(entityRegistry, this.urn);
      log.debug("entity type = {}", this.urn.getEntityType());

      entitySpec(entityRegistry.getEntitySpec(this.urn.getEntityType()));
      log.debug("entity spec = {}", this.entitySpec);

      aspectSpec(ValidationUtils.validate(this.entitySpec, this.aspectName));
      log.debug("aspect spec = {}", this.aspectSpec);

      if (this.patch == null) {
        throw new IllegalArgumentException(
            String.format("Missing patch to apply. Aspect: %s", this.aspectSpec.getName()));
      }

      return new PatchBatchItem(
          this.urn,
          this.aspectName,
          generateSystemMetadataIfEmpty(this.systemMetadata),
          this.patch,
          this.metadataChangeProposal,
          this.entitySpec,
          this.aspectSpec);
    }

    public static PatchBatchItem build(MetadataChangeProposal mcp, EntityRegistry entityRegistry) {
      log.debug("entity type = {}", mcp.getEntityType());
      EntitySpec entitySpec = entityRegistry.getEntitySpec(mcp.getEntityType());
      AspectSpec aspectSpec = validateAspect(mcp, entitySpec);

      if (!isValidChangeType(ChangeType.PATCH, aspectSpec)) {
        throw new UnsupportedOperationException(
            "ChangeType not supported: "
                + mcp.getChangeType()
                + " for aspect "
                + mcp.getAspectName());
      }

      Urn urn = mcp.getEntityUrn();
      if (urn == null) {
        urn = EntityKeyUtils.getUrnFromProposal(mcp, entitySpec.getKeyAspectSpec());
      }

      PatchBatchItemBuilder builder =
          PatchBatchItem.builder()
              .urn(urn)
              .aspectName(mcp.getAspectName())
              .systemMetadata(mcp.getSystemMetadata())
              .metadataChangeProposal(mcp)
              .patch(convertToJsonPatch(mcp));

      return builder.build(entityRegistry);
    }

    private PatchBatchItemBuilder entitySpec(EntitySpec entitySpec) {
      this.entitySpec = entitySpec;
      return this;
    }

    private PatchBatchItemBuilder aspectSpec(AspectSpec aspectSpec) {
      this.aspectSpec = aspectSpec;
      return this;
    }

    private static Patch convertToJsonPatch(MetadataChangeProposal mcp) {
      JsonNode json;
      try {
        json = OBJECT_MAPPER.readTree(mcp.getAspect().getValue().asString(StandardCharsets.UTF_8));
        return JsonPatch.fromJson(json);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid JSON Patch: " + mcp.getAspect().getValue(), e);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PatchBatchItem that = (PatchBatchItem) o;
    return urn.equals(that.urn)
        && aspectName.equals(that.aspectName)
        && Objects.equals(systemMetadata, that.systemMetadata)
        && patch.equals(that.patch);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn, aspectName, systemMetadata, patch);
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
