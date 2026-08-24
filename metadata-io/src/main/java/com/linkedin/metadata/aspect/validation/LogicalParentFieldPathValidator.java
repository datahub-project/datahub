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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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

  private record FieldCheck(BatchItem item, Urn datasetUrn, String fieldPath, String role) {}

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    final ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    // Collect the checks for the whole batch before reading anything: a multi-column link emits one
    // item per mapped column, all referencing the same two datasets, so every schema referenced by
    // the batch is read in one batch fetch and each dataset at most once.
    final List<FieldCheck> fieldChecks = new ArrayList<>();

    mcpItems.forEach(
        item -> {
          if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof MCPItem) {
            collectPatchItemChecks((MCPItem) item, fieldChecks);
            return;
          }
          collectChecks(item, item.getAspect(LogicalParent.class), fieldChecks);
        });

    if (fieldChecks.isEmpty()) {
      return exceptions.streamAllExceptions();
    }

    final Map<Urn, Set<String>> fieldPathsByDataset =
        readFieldPaths(
            fieldChecks.stream().map(FieldCheck::datasetUrn).collect(Collectors.toSet()),
            operationContext,
            retrieverContext);

    fieldChecks.forEach(
        check -> {
          if (!fieldPathsByDataset
              .getOrDefault(check.datasetUrn(), Set.of())
              .contains(check.fieldPath())) {
            exceptions.addException(
                check.item(),
                String.format(
                    "Field path '%s' not found on %s dataset %s",
                    check.fieldPath(), check.role(), check.datasetUrn()));
          }
        });

    return exceptions.streamAllExceptions();
  }

  /**
   * A patch item carries only its delta; rebuild a partial aspect from each add/replace operation
   * (a value at {@code /parent} becomes {@code {"parent":<value>}}) and run the same edge check —
   * the child field path comes from the item's own urn. Unparseable values are left to schema
   * validation at merge time.
   */
  private void collectPatchItemChecks(
      @Nonnull final MCPItem item, @Nonnull final List<FieldCheck> fieldChecks) {
    PatchOperationUtils.addAndReplaceValues(item)
        .forEach(
            op ->
                PatchOperationUtils.nestValueAtObjectPath(op.getFirst(), op.getSecond())
                    .ifPresent(
                        nested -> {
                          try {
                            collectChecks(
                                item,
                                RecordUtils.toRecordTemplate(
                                    LogicalParent.class, nested.toString()),
                                fieldChecks);
                          } catch (RuntimeException e) {
                            // unparseable delta — schema validation rejects it at merge time
                          }
                        }));
  }

  private void collectChecks(
      @Nonnull final BatchItem item,
      final LogicalParent logicalParent,
      @Nonnull final List<FieldCheck> fieldChecks) {
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

    fieldChecks.add(new FieldCheck(item, child.get().getFirst(), child.get().getSecond(), "child"));
    fieldChecks.add(
        new FieldCheck(item, parent.get().getFirst(), parent.get().getSecond(), "parent"));
  }

  @Nonnull
  private Map<Urn, Set<String>> readFieldPaths(
      @Nonnull final Set<Urn> datasetUrns,
      @Nonnull final OperationFingerprint operationContext,
      @Nonnull final RetrieverContext retrieverContext) {
    final Map<Urn, Map<String, Aspect>> aspects =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObjects(
                operationContext, datasetUrns, Set.of(SCHEMA_METADATA_ASPECT_NAME));

    final Map<Urn, Set<String>> fieldPathsByDataset = new HashMap<>();
    aspects.forEach(
        (datasetUrn, aspectsByName) -> {
          final Aspect aspect = aspectsByName.get(SCHEMA_METADATA_ASPECT_NAME);
          if (aspect == null) {
            return;
          }
          final SchemaMetadata schema = new SchemaMetadata(aspect.data());
          if (!schema.hasFields()) {
            return;
          }
          fieldPathsByDataset.put(
              datasetUrn,
              schema.getFields().stream()
                  .map(SchemaField::getFieldPath)
                  .collect(Collectors.toSet()));
        });
    return fieldPathsByDataset;
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
