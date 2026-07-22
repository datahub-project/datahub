package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
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
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    // Validate the merged aspect at pre-commit so a value written via PATCH (whose delta skips the
    // proposed hook because PATCH is not in supportedOperations) is still checked: a patch-applied
    // item arrives here as an UPSERT ChangeMCP carrying the merged LogicalParent.
    return validateFieldPaths(
        operationContext,
        changeMCPs.stream()
            .filter(item -> LOGICAL_PARENT_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  public static Stream<AspectValidationException> validateFieldPaths(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    final ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    // A multi-column link emits one item per mapped column, all referencing the same two datasets;
    // cache field paths per dataset so each schema is read at most once per batch.
    final Map<Urn, Set<String>> fieldPathCache = new HashMap<>();

    changeMCPs.forEach(
        item -> {
          final LogicalParent logicalParent = item.getAspect(LogicalParent.class);
          if (logicalParent == null
              || logicalParent.getParent() == null
              || logicalParent.getParent().getDestinationUrn() == null) {
            // Unlink (parent cleared) — nothing to validate.
            return;
          }

          final Optional<Pair<Urn, String>> child =
              SchemaFieldUtils.parseSchemaFieldUrn(item.getUrn());
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
        });

    return exceptions.streamAllExceptions();
  }

  private static void validateFieldPresent(
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
  private static Set<String> readFieldPaths(
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
}
