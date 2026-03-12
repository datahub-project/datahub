package com.linkedin.metadata.aspect.consistency.check;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.validation.CoercionMode;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Consistency check that detects invalid enum values in aspects using schema-based validation.
 *
 * <p>This check validates raw DataMap data against the aspect's Pegasus schema to detect enum
 * fields with values that are not valid enum symbols. Invalid enums typically occur when:
 *
 * <ul>
 *   <li>Schema evolution removed enum values that still exist in data
 *   <li>Data was written with an incorrect or misspelled enum value
 *   <li>External systems wrote invalid data directly to storage
 * </ul>
 *
 * <p><b>Detection:</b> Uses {@link ValidateDataAgainstSchema#validate} with the aspect's {@link
 * RecordDataSchema} to validate raw DataMap. Checks validation messages for "is not an enum symbol"
 * pattern.
 *
 * <p><b>Fix:</b> Returns {@link ConsistencyFixType#DELETE_ASPECT} to delete only the specific
 * aspect with the invalid enum value. Subclasses (like AssertionInvalidTypeCheck) may override to
 * use HARD_DELETE when the entire entity should be removed.
 *
 * <p><b>Unrecognized Fields:</b> Uses {@link UnrecognizedFieldMode#TRIM} so that unknown fields do
 * not trigger this check. Unknown fields are handled separately by {@link
 * AspectSchemaValidationCheck} with TRIM_UPSERT fix.
 *
 * <p><b>On-Demand Only:</b> This base check is on-demand only. Subclasses can override {@link
 * #isOnDemandOnly()} to enable by default for specific entity types.
 *
 * <p><b>Wildcard Entity Type:</b> Returns "*" as entity type, meaning it can process any entity
 * type when invoked explicitly.
 */
@Slf4j
public class InvalidEnumCheck extends AbstractEntityCheck {

  // Use TRIM mode for unrecognized fields - we only care about enum validation errors.
  // If enum is invalid, the aspect is deleted anyway (trimming doesn't matter).
  // Unrecognized fields without enum errors are handled by AspectSchemaValidationCheck.
  private static final ValidationOptions VALIDATION_OPTIONS =
      new ValidationOptions(
          RequiredMode.CAN_BE_ABSENT_IF_HAS_DEFAULT,
          CoercionMode.NORMAL,
          UnrecognizedFieldMode.TRIM);

  /** Pattern in validation messages that indicates an invalid enum value */
  private static final String ENUM_ERROR_PATTERN = "is not an enum symbol";

  @Override
  public boolean isOnDemandOnly() {
    return true; // On-demand by default, subclasses can override
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE;
  }

  @Override
  @Nonnull
  public String getName() {
    return "Invalid Enum Check";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Detects entities with invalid/unknown enum values in aspect fields";
  }

  @Override
  @Nonnull
  public Optional<Set<String>> getRequiredAspects() {
    // Subclasses can restrict to specific aspects via getTargetAspects()
    return Optional.empty();
  }

  /**
   * Get the set of aspect names to validate.
   *
   * <p>Subclasses can override to restrict validation to specific aspects. An empty set means all
   * aspects will be validated.
   *
   * @return set of aspect names to validate, or empty to validate all
   */
  @Override
  @Nonnull
  public Set<String> getTargetAspects() {
    return Collections.emptySet(); // Empty = check all aspects
  }

  /**
   * Get the fix type for invalid enum issues.
   *
   * <p>Subclasses can override to use HARD_DELETE when the entire entity should be removed (e.g.,
   * for assertions where an invalid type makes the entity unusable).
   *
   * @return the fix type to use (default: DELETE_ASPECT)
   */
  @Nonnull
  protected ConsistencyFixType getFixType() {
    return ConsistencyFixType.DELETE_ASPECT;
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkEntity(
      @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response) {

    EntityRegistry entityRegistry =
        ctx.getOperationContext().getRetrieverContext().getAspectRetriever().getEntityRegistry();

    EntitySpec entitySpec;
    try {
      entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
    } catch (IllegalArgumentException e) {
      log.warn("Unknown entity type {} for URN {}, skipping", urn.getEntityType(), urn);
      return Collections.emptyList();
    }

    Set<String> targetAspects = getTargetAspects();
    List<ConsistencyIssue> issues = new ArrayList<>();

    for (Map.Entry<String, EnvelopedAspect> entry : response.getAspects().entrySet()) {
      String aspectName = entry.getKey();

      // Skip if not in target aspects (unless target is empty = check all)
      if (!targetAspects.isEmpty() && !targetAspects.contains(aspectName)) {
        continue;
      }

      AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
      if (aspectSpec == null) {
        log.warn("Unknown aspect {} on entity {}, skipping", aspectName, urn);
        continue;
      }

      DataMap dataMap = entry.getValue().getValue().data();
      RecordDataSchema schema = aspectSpec.getPegasusSchema();

      // Validate raw DataMap against schema
      ValidationResult result =
          ValidateDataAgainstSchema.validate(dataMap, schema, VALIDATION_OPTIONS);

      if (!result.isValid()) {
        // Check if any error is an enum validation error
        String enumError = findEnumValidationError(result);
        if (enumError != null) {
          log.debug("Invalid enum in aspect {} on entity {}: {}", aspectName, urn, enumError);

          ConsistencyFixType fixType = getFixType();
          ConsistencyIssue.ConsistencyIssueBuilder issueBuilder =
              createIssueBuilder(urn, fixType)
                  .description(
                      String.format("Aspect %s has invalid enum value: %s", aspectName, enumError));

          if (fixType == ConsistencyFixType.HARD_DELETE) {
            issueBuilder.hardDeleteUrns(List.of(urn));
          } else {
            // DELETE_ASPECT - create a DeleteItemImpl for the specific aspect
            AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
            BatchItem deleteItem =
                DeleteItemImpl.builder()
                    .urn(urn)
                    .aspectName(aspectName)
                    .auditStamp(auditStamp)
                    .build(ctx.getOperationContext().getAspectRetriever());
            issueBuilder.batchItems(List.of(deleteItem));
          }

          issues.add(issueBuilder.build());
          // One issue per entity is enough - stop checking other aspects
          break;
        }
      }
    }

    return issues;
  }

  /**
   * Find an enum validation error in the validation result.
   *
   * @param result the validation result
   * @return the error message if an enum error is found, null otherwise
   */
  private String findEnumValidationError(ValidationResult result) {
    if (result.getMessages() != null) {
      for (Object msg : result.getMessages()) {
        String msgText = msg.toString();
        if (msgText.contains(ENUM_ERROR_PATTERN)) {
          return msgText;
        }
      }
    }
    return null;
  }
}
