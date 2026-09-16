package com.linkedin.metadata.aspect.consistency.check;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.RecordTemplateValidator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * On-demand consistency check that validates RecordTemplates against aspect schemas.
 *
 * <p>This check detects aspects with unknown/unrecognized fields that need trimming. It works with
 * any entity type and aspect, making it useful for schema migration cleanup.
 *
 * <p><b>Detection:</b> Uses {@link RecordTemplateValidator#validate} with {@code
 * UnrecognizedFieldMode.DISALLOW} for efficient detection without modifying data.
 *
 * <p><b>Fix:</b> Returns {@link ConsistencyFixType#TRIM_UPSERT} which uses {@link
 * com.linkedin.metadata.aspect.consistency.fix.TrimBatchItemsFix} to trim unknown fields before
 * upserting.
 *
 * <p><b>On-Demand Only:</b> This check is excluded from default/wildcard runs and must be invoked
 * explicitly by check ID.
 *
 * <p><b>Wildcard Entity Type:</b> Returns "*" as entity type, meaning it can process any entity
 * type when invoked explicitly.
 */
@Slf4j
@Component
public class AspectSchemaValidationCheck extends AbstractEntityCheck {

  @Override
  @Nonnull
  public String getName() {
    return "Aspect Schema Validation";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Validates aspects against their schemas and identifies those with unknown fields that need trimming";
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE;
  }

  @Override
  @Nonnull
  public Optional<Set<String>> getRequiredAspects() {
    // Empty optional = fetch all aspects (we validate all aspects present on the entity)
    return Optional.empty();
  }

  @Override
  public boolean isOnDemandOnly() {
    return true;
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkEntity(
      @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response) {

    EntityRegistry entityRegistry =
        ctx.getOperationContext().getRetrieverContext().getAspectRetriever().getEntityRegistry();

    String entityTypeName = urn.getEntityType();
    EntitySpec entitySpec;
    try {
      entitySpec = entityRegistry.getEntitySpec(entityTypeName);
    } catch (IllegalArgumentException e) {
      log.warn("Unknown entity type {} for URN {}, skipping", entityTypeName, urn);
      return Collections.emptyList();
    }

    List<ConsistencyIssue> issues = new ArrayList<>();

    // Check each aspect in the response
    for (Map.Entry<String, EnvelopedAspect> aspectEntry : response.getAspects().entrySet()) {
      String aspectName = aspectEntry.getKey();
      EnvelopedAspect envelopedAspect = aspectEntry.getValue();

      AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
      if (aspectSpec == null) {
        // Unknown aspect - should not happen but skip if it does
        log.warn("Unknown aspect {} on entity {}, skipping", aspectName, urn);
        continue;
      }

      try {
        // Deserialize the aspect to a RecordTemplate
        // EnvelopedAspect (com.linkedin.entity) has getValue() returning Aspect
        // which holds the DataMap containing the aspect data
        RecordTemplate recordTemplate =
            RecordUtils.toRecordTemplate(
                aspectSpec.getDataTemplateClass(), envelopedAspect.getValue().data());

        // Check if the aspect has unknown fields using strict validation
        if (hasUnknownFields(recordTemplate)) {
          log.debug(
              "Aspect {} on entity {} has unknown fields that need trimming", aspectName, urn);

          // Create issue with TRIM_UPSERT fix type
          // Pass envelopedAspect for conditional write header
          ConsistencyIssue issue =
              createIssueBuilder(urn, ConsistencyFixType.TRIM_UPSERT)
                  .description(
                      String.format(
                          "Aspect %s has unknown fields that do not conform to the schema",
                          aspectName))
                  .details(aspectName)
                  .batchItems(
                      List.of(
                          createUpsertItem(ctx, urn, aspectName, recordTemplate, envelopedAspect)))
                  .build();

          issues.add(issue);
        }
      } catch (Exception e) {
        log.warn(
            "Failed to validate aspect {} on entity {}: {}. Skipping.",
            aspectName,
            urn,
            e.getMessage());
      }
    }

    return issues;
  }

  /**
   * Check if a RecordTemplate has unknown fields.
   *
   * <p>Uses {@link RecordTemplateValidator#validate} which applies {@code
   * UnrecognizedFieldMode.DISALLOW}. This is efficient because it fails fast on unknown fields
   * without modifying the data.
   *
   * @param recordTemplate the record to validate
   * @return true if the record has unknown fields (validation failed)
   */
  private boolean hasUnknownFields(@Nonnull RecordTemplate recordTemplate) {
    final boolean[] hasUnknown = {false};
    RecordTemplateValidator.validate(
        recordTemplate,
        result -> {
          // Validation failed - has unknown fields or other validation issues
          hasUnknown[0] = true;
        });
    return hasUnknown[0];
  }
}
