package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityApiUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.EntityRegistryUrnValidator;
import com.linkedin.metadata.utils.RecordTemplateValidator;
import com.linkedin.metadata.utils.UrnValidationUtil;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValidationApiUtils {
  public static final String STRICT_URN_VALIDATION_ENABLED = "STRICT_URN_VALIDATION_ENABLED";

  /**
   * Validates a {@link RecordTemplate} and throws {@link ValidationException} if validation fails.
   *
   * @param record record to be validated.
   */
  public static void validateOrThrow(RecordTemplate record) {
    RecordTemplateValidator.validate(
        record,
        validationResult -> {
          throw new ValidationException(
              String.format(
                  "Failed to validate record with class %s: %s",
                  record.getClass().getName(), validationResult.getMessages().toString()));
        });
  }

  public static void validateTrimOrThrow(RecordTemplate record) {
    RecordTemplateValidator.validateTrim(
        record,
        validationResult -> {
          throw new ValidationException(
              String.format(
                  "Failed to validate record with class %s: %s",
                  record.getClass().getName(), validationResult.getMessages().toString()));
        });
  }

  @Nonnull
  public static Urn validateUrn(@Nonnull EntityRegistry entityRegistry, final Urn urn) {
    if (urn == null) {
      throw new ValidationException("Cannot validate null URN.");
    }

    UrnValidationUtil.validateUrn(
        entityRegistry,
        urn,
        Boolean.TRUE.equals(
            Boolean.parseBoolean(
                System.getenv().getOrDefault(STRICT_URN_VALIDATION_ENABLED, "false"))));
    return urn;
  }

  @Nonnull
  public static EntitySpec validateEntity(
      @Nonnull EntityRegistry entityRegistry, String entityType) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityType);
    if (entitySpec == null) {
      throw new ValidationException("Unknown entity: " + entityType);
    }
    return entitySpec;
  }

  @Nonnull
  public static AspectSpec validateAspect(@Nonnull EntitySpec entitySpec, String aspectName) {
    if (aspectName == null || aspectName.isEmpty()) {
      throw new UnsupportedOperationException(
          "Aspect name is required for create and update operations");
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);

    if (aspectSpec == null) {
      throw new ValidationException(
          String.format("Unknown aspect %s for entity %s", aspectName, entitySpec.getName()));
    }

    return aspectSpec;
  }

  public static void validateRecordTemplate(
      EntitySpec entitySpec,
      Urn urn,
      @Nullable RecordTemplate aspect,
      @Nonnull AspectRetriever aspectRetriever) {
    EntityRegistry entityRegistry = aspectRetriever.getEntityRegistry();
    EntityRegistryUrnValidator validator = new EntityRegistryUrnValidator(entityRegistry);
    validator.setCurrentEntitySpec(entitySpec);
    Consumer<ValidationResult> resultFunction =
        validationResult -> {
          throw new ValidationException(
              "Invalid format for aspect: "
                  + entitySpec.getName()
                  + "\n Cause: "
                  + validationResult.getMessages());
        };

    RecordTemplateValidator.validate(
        EntityApiUtils.buildKeyAspect(entityRegistry, urn), resultFunction, validator);

    if (aspect != null) {
      RecordTemplateValidator.validateTrim(aspect, resultFunction, validator);
    }
  }

  /**
   * Given an MCP validate 3 primary components: URN, Entity, Aspect validate against the
   * EntityRegistry
   *
   * @param entityRegistry the entity registry
   * @param mcp the MetadataChangeProposal to validate
   * @return the validated MetadataChangeProposal
   */
  public static MetadataChangeProposal validateMCP(
      @Nonnull EntityRegistry entityRegistry, MetadataChangeProposal mcp) {
    if (mcp == null) {
      throw new UnsupportedOperationException("MetadataChangeProposal is required.");
    }

    final EntitySpec entitySpec;
    final Urn urn;
    if (mcp.getEntityUrn() != null) {
      urn = mcp.getEntityUrn();
      entitySpec = validateEntity(entityRegistry, urn.getEntityType());
    } else {
      entitySpec = validateEntity(entityRegistry, mcp.getEntityType());
      urn = EntityKeyUtils.getUrnFromProposal(mcp, entitySpec.getKeyAspectSpec());
      mcp.setEntityUrn(urn);
    }

    if (!Objects.equals(mcp.getEntityType(), urn.getEntityType())) {
      throw new ValidationException(
          String.format(
              "URN entity type does not match MCP entity type. %s != %s",
              urn.getEntityType(), mcp.getEntityType()));
    }

    validateUrn(entityRegistry, urn);
    validateAspect(entitySpec, mcp.getAspectName());

    return mcp;
  }
}
