package com.linkedin.metadata.aspect.consistency.check;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.UNVERSIONED_ASPECT_VERSION;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.UrnValidationFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract base class for entity consistency checks.
 *
 * <p>Provides common functionality for:
 *
 * <ul>
 *   <li>Iterating over a batch of entities
 *   <li>Skipping soft-deleted entities (by default)
 *   <li>Creating Issue builders with common fields
 *   <li>Extracting aspects from entity responses
 *   <li>Exception handling - errors checking individual entities are logged and processing
 *       continues
 *   <li>Default ID derived from class name (e.g., AssertionEntityUrnMissingCheck →
 *       assertion-entity-urn-missing)
 * </ul>
 *
 * <p>Subclasses must implement:
 *
 * <ul>
 *   <li>{@link #getEntityType()} - Entity type this check applies to
 *   <li>{@link #getRequiredAspects()} - Aspects to fetch for checking
 *   <li>{@link #checkEntity} - The actual check logic
 * </ul>
 *
 * <p>Subclasses may optionally override:
 *
 * <ul>
 *   <li>{@link #getTargetAspects()} - Aspects to filter entities by
 * </ul>
 */
@Slf4j
public abstract class AbstractEntityCheck implements ConsistencyCheck {

  /** Cached ID derived from class name */
  private String cachedId;

  /**
   * Get the unique identifier for this check.
   *
   * <p>By default, derives the ID from the class name by removing the "Check" suffix and converting
   * from PascalCase to kebab-case. For example:
   *
   * <ul>
   *   <li>AssertionEntityUrnMissingCheck → assertion-entity-urn-missing
   *   <li>MonitorAssertionsEmptyCheck → monitor-assertions-empty
   * </ul>
   *
   * <p>Subclasses can override this method to provide a custom ID.
   *
   * @return unique check identifier
   */
  @Override
  @Nonnull
  public String getId() {
    if (cachedId == null) {
      cachedId = deriveIdFromClassName();
    }
    return cachedId;
  }

  /**
   * Derive the check ID from the class name.
   *
   * @return kebab-case ID
   */
  private String deriveIdFromClassName() {
    String className = getClass().getSimpleName();

    // Remove "Check" suffix if present
    if (className.endsWith("Check")) {
      className = className.substring(0, className.length() - 5);
    }

    // Convert PascalCase to kebab-case
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < className.length(); i++) {
      char c = className.charAt(i);
      if (Character.isUpperCase(c)) {
        if (i > 0) {
          result.append('-');
        }
        result.append(Character.toLowerCase(c));
      } else {
        result.append(c);
      }
    }
    return result.toString();
  }

  @Override
  @Nonnull
  public final List<ConsistencyIssue> check(
      @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
    List<ConsistencyIssue> allIssues = new ArrayList<>();

    for (Map.Entry<Urn, EntityResponse> entry : entityResponses.entrySet()) {
      Urn urn = entry.getKey();
      EntityResponse response = entry.getValue();

      try {
        // Skip soft-deleted entities by default (subclasses can override)
        if (skipSoftDeleted() && isEntitySoftDeleted(response)) {
          continue;
        }

        List<ConsistencyIssue> issues = checkEntity(ctx, urn, response);
        allIssues.addAll(issues);
      } catch (Exception e) {
        // Log the error and continue processing other entities
        log.error(
            "Error running check {} on entity {}: {}. Continuing with remaining entities.",
            getId(),
            urn,
            e.getMessage(),
            e);
      }
    }

    return allIssues;
  }

  /**
   * Check a single entity for consistency issues.
   *
   * @param ctx check context
   * @param urn URN of the entity
   * @param response entity response with aspects
   * @return list of issues found (may be empty)
   */
  @Nonnull
  protected abstract List<ConsistencyIssue> checkEntity(
      @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response);

  /**
   * Whether to skip soft-deleted entities.
   *
   * <p>Override to return false if the check should also examine soft-deleted entities.
   *
   * @return true to skip soft-deleted entities (default)
   */
  protected boolean skipSoftDeleted() {
    return true;
  }

  /**
   * Check if the entity is soft-deleted.
   *
   * @param response entity response
   * @return true if soft-deleted
   */
  protected boolean isEntitySoftDeleted(@Nonnull EntityResponse response) {
    EnvelopedAspect statusAspect = response.getAspects().get(STATUS_ASPECT_NAME);
    if (statusAspect != null) {
      Status status = new Status(statusAspect.getValue().data());
      return status.isRemoved();
    }
    return false;
  }

  /**
   * Get an aspect from the entity response.
   *
   * @param response entity response
   * @param aspectName aspect name
   * @return enveloped aspect, or null if not present
   */
  @Nullable
  protected EnvelopedAspect getAspect(
      @Nonnull EntityResponse response, @Nonnull String aspectName) {
    return response.getAspects().get(aspectName);
  }

  /**
   * Create an Issue builder pre-populated with common fields.
   *
   * <p>The fixType must be specified for each issue - there is no default.
   *
   * @param urn the entity URN
   * @param fixType the fix type for this issue
   * @return issue builder
   */
  protected ConsistencyIssue.ConsistencyIssueBuilder createIssueBuilder(
      @Nonnull Urn urn, @Nonnull ConsistencyFixType fixType) {
    return ConsistencyIssue.builder()
        .entityUrn(urn)
        .entityType(getEntityType())
        .checkId(getId())
        .fixType(fixType);
  }

  // ============================================================================
  // Helper methods for creating BatchItems
  // ============================================================================

  /**
   * Create a ChangeItemImpl for upserting an aspect with conditional write support.
   *
   * <p>The original aspect's system metadata version is used for optimistic concurrency control via
   * the {@code If-Version-Match} header.
   *
   * @param ctx check context (provides AspectRetriever)
   * @param urn URN of the entity
   * @param aspectName name of the aspect
   * @param aspect the aspect value
   * @param originalAspect the original aspect from EntityResponse (for conditional write version)
   * @return ChangeItemImpl for upsert with conditional write header
   */
  protected BatchItem createUpsertItem(
      @Nonnull CheckContext ctx,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect,
      @Nullable EnvelopedAspect originalAspect) {
    return createUpsertItem(ctx, urn, aspectName, aspect, ChangeType.UPSERT, originalAspect);
  }

  /**
   * Create a ChangeItemImpl for creating or upserting an aspect without conditional write.
   *
   * <p><b>Note:</b> Prefer using the overload with {@code originalAspect} to enable conditional
   * writes.
   *
   * @param ctx check context (provides AspectRetriever)
   * @param urn URN of the entity
   * @param aspectName name of the aspect
   * @param aspect the aspect value
   * @param changeType the change type (UPSERT or CREATE)
   * @return ChangeItemImpl
   */
  protected BatchItem createUpsertItem(
      @Nonnull CheckContext ctx,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect,
      @Nonnull ChangeType changeType) {
    return createUpsertItem(ctx, urn, aspectName, aspect, changeType, null);
  }

  /**
   * Create a ChangeItemImpl for creating or upserting an aspect with conditional write support.
   *
   * <p>The original aspect's system metadata version is used for optimistic concurrency control via
   * the {@code If-Version-Match} header.
   *
   * @param ctx check context (provides AspectRetriever)
   * @param urn URN of the entity
   * @param aspectName name of the aspect
   * @param aspect the aspect value
   * @param changeType the change type (UPSERT or CREATE)
   * @param originalAspect the original aspect from EntityResponse (for conditional write version),
   *     null for CREATE or if conditional write is not needed
   * @return ChangeItemImpl with conditional write header if originalAspect is provided
   */
  protected BatchItem createUpsertItem(
      @Nonnull CheckContext ctx,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect,
      @Nonnull ChangeType changeType,
      @Nullable EnvelopedAspect originalAspect) {
    AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

    // Extract version for conditional write
    Map<String, String> headers = buildConditionalWriteHeaders(originalAspect);

    return ChangeItemImpl.builder()
        .changeType(changeType)
        .urn(urn)
        .aspectName(aspectName)
        .recordTemplate(aspect)
        .auditStamp(auditStamp)
        .headers(headers)
        .build(ctx.getOperationContext().getAspectRetriever());
  }

  /**
   * Build headers map with conditional write version from the original aspect.
   *
   * <p>Package-private for testing.
   *
   * @param originalAspect the original aspect (may be null)
   * @return headers map with If-Version-Match header, or empty map if no version
   */
  Map<String, String> buildConditionalWriteHeaders(@Nullable EnvelopedAspect originalAspect) {
    if (originalAspect != null && originalAspect.hasSystemMetadata()) {
      String version = originalAspect.getSystemMetadata().getVersion();
      if (version != null) {
        return Map.of(HTTP_HEADER_IF_VERSION_MATCH, version);
      }
    }
    // For CREATE or missing version, use unversioned marker
    return Map.of(HTTP_HEADER_IF_VERSION_MATCH, UNVERSIONED_ASPECT_VERSION);
  }

  /**
   * Create a ChangeItemImpl for soft-deleting an entity with conditional write support.
   *
   * <p>Uses the status aspect's version from the entity response for optimistic concurrency
   * control.
   *
   * @param ctx check context (provides AspectRetriever)
   * @param urn URN of the entity to soft-delete
   * @param response entity response containing the current status aspect
   * @return ChangeItemImpl that sets status.removed=true with conditional write header
   */
  protected BatchItem createSoftDeleteItem(
      @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response) {
    Status removedStatus = new Status().setRemoved(true);
    EnvelopedAspect statusAspect = getAspect(response, STATUS_ASPECT_NAME);
    return createUpsertItem(ctx, urn, STATUS_ASPECT_NAME, removedStatus, statusAspect);
  }

  /**
   * Create an Issue with batch items for soft-delete fixes.
   *
   * @param urn the entity URN
   * @param description issue description
   * @param batchItems the batch items (typically soft-delete status update)
   * @return issue with batch items
   */
  protected ConsistencyIssue createSoftDeleteIssue(
      @Nonnull Urn urn, @Nonnull String description, @Nonnull List<BatchItem> batchItems) {
    return createIssueBuilder(urn, ConsistencyFixType.SOFT_DELETE)
        .description(description)
        .batchItems(batchItems)
        .build();
  }

  // ============================================================================
  // Referenced Entity Helpers
  // ============================================================================

  /**
   * Check if a referenced entity is soft-deleted by querying the entity service directly.
   *
   * <p>This queries the entity's existence both with and without soft-deleted entities included. If
   * the entity exists when including soft-deleted but not when excluding them, it is soft-deleted.
   *
   * @param ctx check context
   * @param urn URN of the entity to check
   * @return true if the entity exists and is soft-deleted
   */
  protected boolean isReferencedEntitySoftDeleted(@Nonnull CheckContext ctx, @Nonnull Urn urn) {
    // Check if entity exists including soft-deleted
    boolean existsIncludingSoftDeleted =
        ctx.getEntityService().exists(ctx.getOperationContext(), urn, true);

    if (!existsIncludingSoftDeleted) {
      // Entity doesn't exist at all (hard-deleted or never existed)
      return false;
    }

    // Check if entity exists excluding soft-deleted
    boolean existsNotSoftDeleted =
        ctx.getEntityService().exists(ctx.getOperationContext(), urn, false);

    // If it exists including soft-deleted but not when excluding, it's soft-deleted
    return !existsNotSoftDeleted;
  }

  /**
   * Check if a referenced entity exists (including soft-deleted).
   *
   * <p>Use this to check if an entity has been hard-deleted.
   *
   * @param ctx check context
   * @param urn URN of the entity to check
   * @return true if the entity exists (even if soft-deleted)
   */
  protected boolean referencedEntityExists(@Nonnull CheckContext ctx, @Nonnull Urn urn) {
    return ctx.getEntityService().exists(ctx.getOperationContext(), urn, true);
  }

  // ============================================================================
  // Entity Type Validation from PDL Annotations
  // ============================================================================

  /**
   * Compute valid entity types from @UrnValidation annotations in an aspect's PDL.
   *
   * <p>This utility method extracts all entity types declared in @UrnValidation annotations on URN
   * fields within the specified aspect. It can be used by subclasses to determine valid entity
   * types at construction time rather than hardcoding them.
   *
   * <p>Example usage in a subclass constructor:
   *
   * <pre>{@code
   * this.validEntityTypes = Collections.unmodifiableSet(
   *     computeValidEntityTypesFromAspect(
   *         entityRegistry,
   *         ASSERTION_ENTITY_NAME,
   *         ASSERTION_INFO_ASPECT_NAME,
   *         Set.of(SCHEMA_FIELD_ENTITY_NAME)  // exclusions
   *     ));
   * }</pre>
   *
   * @param entityRegistry the entity registry
   * @param entityName the entity type name (e.g., "assertion", "monitor")
   * @param aspectName the aspect name containing @UrnValidation annotations
   * @param exclusions entity types to exclude from the result (may be empty)
   * @return set of valid entity type names
   * @throws IllegalStateException if the aspect spec is not found or has no valid types
   */
  @Nonnull
  protected static Set<String> computeValidEntityTypesFromAspect(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Set<String> exclusions) {

    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);

    if (aspectSpec == null) {
      throw new IllegalStateException(
          String.format(
              "%s aspect spec not found in entity registry for %s entity", aspectName, entityName));
    }

    Set<String> validTypes = new HashSet<>();
    for (UrnValidationFieldSpec fieldSpec : aspectSpec.getUrnValidationFieldSpecMap().values()) {
      List<String> entityTypes = fieldSpec.getUrnValidationAnnotation().getEntityTypes();
      for (String type : entityTypes) {
        if (!exclusions.contains(type)) {
          validTypes.add(type);
        }
      }
    }

    if (validTypes.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "No valid entity types found in %s @UrnValidation annotations for %s",
              aspectName, entityName));
    }

    return validTypes;
  }

  /**
   * Compute valid entity types from @UrnValidation annotations in an aspect's PDL.
   *
   * <p>Convenience overload with no exclusions.
   *
   * @param entityRegistry the entity registry
   * @param entityName the entity type name (e.g., "assertion", "monitor")
   * @param aspectName the aspect name containing @UrnValidation annotations
   * @return set of valid entity type names
   * @throws IllegalStateException if the aspect spec is not found or has no valid types
   */
  @Nonnull
  protected static Set<String> computeValidEntityTypesFromAspect(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull String entityName,
      @Nonnull String aspectName) {
    return computeValidEntityTypesFromAspect(entityRegistry, entityName, aspectName, Set.of());
  }
}
