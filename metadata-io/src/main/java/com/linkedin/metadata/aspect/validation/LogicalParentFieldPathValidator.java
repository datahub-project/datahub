package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.PatchOperationUtils;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates that a column-level {@code logicalParent} edge references field paths that exist on
 * both the child and parent dataset schemas. This runs across all write APIs (GraphQL, OpenAPI,
 * RestLI, SDK) so a logical-model link can never create a {@code logicalParent} edge pointing at a
 * non-existent schema field — the GraphQL resolver previously skipped this check that the OpenAPI
 * controller performed, so the guarantee now lives at the aspect layer instead of per-API.
 *
 * <p>Unlink writes (a {@code logicalParent} whose parent edge is cleared) and dataset-level links
 * (which carry no field paths) are ignored.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class LogicalParentFieldPathValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    final ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    // A multi-column link emits one item per mapped column, all referencing the same two datasets;
    // cache field paths per dataset so each schema is read at most once per batch.
    final Map<Urn, Set<String>> fieldPathCache = new HashMap<>();

    mcpItems.forEach(
        item -> {
          if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof MCPItem) {
            validatePatchItem(
                (MCPItem) item, operationContext, retrieverContext, fieldPathCache, exceptions);
            return;
          }
          validateLogicalParent(
              item,
              item.getAspect(LogicalParent.class),
              operationContext,
              retrieverContext,
              fieldPathCache,
              exceptions);
        });

    return exceptions.streamAllExceptions();
  }

  /**
   * A patch item carries only its delta; rebuild a partial aspect from each add/replace operation
   * (a value at {@code /parent} becomes {@code {"parent":<value>}}) and run the same edge check —
   * the child field path comes from the item's own urn. Unparseable values are left to schema
   * validation at merge time.
   */
  private void validatePatchItem(
      @Nonnull final MCPItem item,
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext,
      @Nonnull final Map<Urn, Set<String>> fieldPathCache,
      @Nonnull final ValidationExceptionCollection exceptions) {
    PatchOperationUtils.addAndReplaceValues(item)
        .forEach(
            op ->
                PatchOperationUtils.nestValueAtObjectPath(op.getFirst(), op.getSecond())
                    .ifPresent(
                        nested -> {
                          try {
                            validateLogicalParent(
                                item,
                                RecordUtils.toRecordTemplate(
                                    LogicalParent.class, nested.toString()),
                                operationContext,
                                retrieverContext,
                                fieldPathCache,
                                exceptions);
                          } catch (RuntimeException e) {
                            // unparseable delta — schema validation rejects it at merge time
                          }
                        }));
  }

  private void validateLogicalParent(
      @Nonnull final BatchItem item,
      final LogicalParent logicalParent,
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext,
      @Nonnull final Map<Urn, Set<String>> fieldPathCache,
      @Nonnull final ValidationExceptionCollection exceptions) {
    if (logicalParent == null
        || logicalParent.getParent() == null
        || logicalParent.getParent().getDestinationUrn() == null) {
      // Unlink (parent cleared) — nothing to validate.
      return;
    }

    final Optional<Pair<Urn, String>> child = SchemaFieldUtils.parseSchemaFieldUrn(item.getUrn());
    final Optional<Pair<Urn, String>> parent =
        SchemaFieldUtils.parseSchemaFieldUrn(logicalParent.getParent().getDestinationUrn());
    if (child.isEmpty() || parent.isEmpty()) {
      // Not a column-level (schemaField -> schemaField) edge; dataset-level links carry no
      // field paths to validate.
      return;
    }

    validateFieldPresent(
        item,
        child.get().getFirst(),
        child.get().getSecond(),
        "child",
        operationContext,
        retrieverContext,
        fieldPathCache,
        exceptions);
    validateFieldPresent(
        item,
        parent.get().getFirst(),
        parent.get().getSecond(),
        "parent",
        operationContext,
        retrieverContext,
        fieldPathCache,
        exceptions);
  }

  private void validateFieldPresent(
      @Nonnull final BatchItem item,
      @Nonnull final Urn datasetUrn,
      @Nonnull final String fieldPath,
      @Nonnull final String role,
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext,
      @Nonnull final Map<Urn, Set<String>> fieldPathCache,
      @Nonnull final ValidationExceptionCollection exceptions) {
    final Set<String> fieldPaths =
        fieldPathCache.computeIfAbsent(
            datasetUrn, urn -> readFieldPaths(urn, operationContext, retrieverContext));
    if (!fieldPaths.contains(fieldPath)) {
      exceptions.addException(
          item,
          String.format("Field path '%s' not found on %s dataset %s", fieldPath, role, datasetUrn));
    }
  }

  @Nonnull
  private Set<String> readFieldPaths(
      @Nonnull final Urn datasetUrn,
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext) {
    final Aspect aspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(operationContext, datasetUrn, SCHEMA_METADATA_ASPECT_NAME);
    if (aspect == null) {
      return Set.of();
    }
    final SchemaMetadata schema = new SchemaMetadata(aspect.data());
    if (!schema.hasFields()) {
      return Set.of();
    }
    return schema.getFields().stream().map(SchemaField::getFieldPath).collect(Collectors.toSet());
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
