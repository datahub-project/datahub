package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
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
import java.util.function.Consumer;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValidationApiUtils {
  public static final String STRICT_URN_VALIDATION_ENABLED = "STRICT_URN_VALIDATION_ENABLED";
  private static final Set<String> AUDIT_STAMP_FIELD_NAMES =
          Set.of("auditStamp", "created", "lastModified");
  private static final String TIME_FIELD_NAME = "time";

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

  /**
   * Compares two RecordTemplates for equality while tolerating Integer vs Long mismatches on known
   * AuditStamp default timestamp fields.
   *
   * <p>This resolves Integer vs Long DataMap type mismatches that occur when an AuditStamp field
   * has the default value of {@code time=0}: Jackson parses {@code 0} from raw JSON as {@code
   * Integer(0)}, but the incoming MCP path coerces it to {@code Long(0)}. The resulting {@code
   * Integer(0).equals(Long(0)) == false} causes spurious MCL production on every re-ingestion of
   * aspects that contain {@code auditStamp.time}, {@code created.time}, or {@code
   * lastModified.time} defaults.
   */
  public static boolean normalizedEqual(
          @Nullable RecordTemplate oldAspect, @Nullable RecordTemplate newAspect) {
    if (oldAspect == newAspect) return true;
    if (oldAspect == null || newAspect == null) return false;
    if (oldAspect.equals(newAspect)) {
      return true;
    }

    return normalizedDataEquals(oldAspect.data(), newAspect.data(), false, null);
  }

  private static boolean normalizedDataEquals(
          @Nullable Object oldValue,
          @Nullable Object newValue,
          boolean insideAuditStamp,
          @Nullable String currentFieldName) {
    if (oldValue == newValue) {
      return true;
    }
    if (oldValue == null || newValue == null) {
      return false;
    }
    if (oldValue instanceof DataMap && newValue instanceof DataMap) {
      return normalizedMapEquals((DataMap) oldValue, (DataMap) newValue, insideAuditStamp);
    }
    if (oldValue instanceof DataList && newValue instanceof DataList) {
      return normalizedListEquals((DataList) oldValue, (DataList) newValue, insideAuditStamp);
    }
    if (oldValue instanceof Number && newValue instanceof Number) {
      return numbersEqual((Number) oldValue, (Number) newValue, insideAuditStamp, currentFieldName);
    }
    return Objects.equals(oldValue, newValue);
  }

  private static boolean numbersEqual(
          Number oldValue,
          Number newValue,
          boolean insideAuditStamp,
          @Nullable String currentFieldName) {
    if (insideAuditStamp && TIME_FIELD_NAME.equals(currentFieldName)) {
      return oldValue.longValue() == newValue.longValue();
    }
    if (isIntegralNumber(oldValue) && isIntegralNumber(newValue)) {
      return oldValue.longValue() == newValue.longValue();
    }
    return Objects.equals(oldValue, newValue);
  }

  private static boolean isIntegralNumber(Number value) {
    return value instanceof Byte
            || value instanceof Short
            || value instanceof Integer
            || value instanceof Long;
  }

  private static boolean normalizedMapEquals(
          DataMap oldMap, DataMap newMap, boolean insideAuditStamp) {
    if (oldMap.size() != newMap.size()) {
      return false;
    }

    for (String key : oldMap.keySet()) {
      if (!newMap.containsKey(key)) {
        return false;
      }
      Object oldValue = oldMap.get(key);
      Object newValue = newMap.get(key);
      boolean childInsideAuditStamp = insideAuditStamp || AUDIT_STAMP_FIELD_NAMES.contains(key);
      if (!normalizedDataEquals(oldValue, newValue, childInsideAuditStamp, key)) {
        return false;
      }
    }

    return true;
  }

  private static boolean normalizedListEquals(
          DataList oldList, DataList newList, boolean insideAuditStamp) {
    if (oldList.size() != newList.size()) {
      return false;
    }

    for (int i = 0; i < oldList.size(); i++) {
      if (!normalizedDataEquals(oldList.get(i), newList.get(i), insideAuditStamp, null)) {
        return false;
      }
    }
    return true;
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
    try {
      EntitySpec entitySpec = entityRegistry.getEntitySpec(entityType);
      if (entitySpec == null) {
        throw new ValidationException("Unknown entity: " + entityType);
      }
      return entitySpec;
    } catch (IllegalArgumentException e) {
      throw new ValidationException(e.getMessage());
    }
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

    if (mcp.getEntityType().equalsIgnoreCase(urn.getEntityType())) {
      mcp.setEntityType(urn.getEntityType());
    } else {
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
