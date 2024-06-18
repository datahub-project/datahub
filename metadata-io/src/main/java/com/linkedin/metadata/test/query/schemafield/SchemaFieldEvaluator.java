package com.linkedin.metadata.test.query.schemafield;

import static com.linkedin.metadata.test.query.schemafield.SchemaFieldUtils.SCHEMA_FIELDS_LENGTH_PROPERTY;
import static com.linkedin.metadata.test.query.schemafield.SchemaFieldUtils.SCHEMA_FIELDS_PROPERTY;
import static com.linkedin.metadata.test.query.schemafield.SchemaFieldUtils.isSchemaFieldsQuery;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.query.BaseQueryEvaluator;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Evaluator that supports resolving `schemaFields` and `schemaFields.length` queries, e.g. those
 * which are defined against the 'resolved' schema fields model for a dataset.
 *
 * <p>Note that the ONLY supports schema fields for the immediate entity, and not related entities.
 * For example, we do not support predicates on the schema fields attached to related datasets (yet)
 */
@Slf4j
@RequiredArgsConstructor
public class SchemaFieldEvaluator extends BaseQueryEvaluator {

  private final EntityService<?> entityService;

  @Override
  public boolean isEligible(@Nonnull final String entityType, @Nonnull final TestQuery query) {
    if (!entityType.equals(Constants.DATASET_ENTITY_NAME)) {
      return false;
    }
    return isSchemaFieldsQuery(query);
  }

  @Override
  @Nonnull
  public ValidationResult validateQuery(
      @Nonnull final String entityType, @Nonnull final TestQuery query)
      throws IllegalArgumentException {
    return new ValidationResult(isEligible(entityType, query), Collections.emptyList());
  }

  @Override
  @Nonnull
  public Map<Urn, Map<TestQuery, TestQueryResponse>> evaluate(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityType,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<TestQuery> queries) {
    final Map<Urn, Map<TestQuery, TestQueryResponse>> result = new HashMap<>();
    for (TestQuery query : queries) {
      try {
        final Set<String> aspectSpecNames =
            ImmutableSet.of(
                Constants.SCHEMA_METADATA_ASPECT_NAME,
                Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);

        entityService
            .getEntitiesV2(opContext, entityType, urns, aspectSpecNames)
            .forEach(
                (urn, response) -> {
                  result.putIfAbsent(urn, new HashMap<>());
                  try {
                    result
                        .get(urn)
                        .put(
                            query,
                            buildQueryResponse(
                                query,
                                extractSchemaMetadata(response),
                                extractEditableSchemaMetadata(response)));
                  } catch (RuntimeException e) {
                    log.error(
                        "RuntimeException for urn: {} for query {}. Skipping running test for urn",
                        urn,
                        query,
                        e);
                  }
                });
      } catch (URISyntaxException e) {
        log.error("Error while fetching aspects for urns {}", urns, e);
        throw new RuntimeException(String.format("Error while fetching aspects for urns %s", urns));
      }
    }
    return result;
  }

  private TestQueryResponse buildQueryResponse(
      @Nonnull final TestQuery query,
      @Nullable final SchemaMetadata schemaMetadata,
      @Nullable final EditableSchemaMetadata editableSchemaMetadata) {
    if (schemaMetadata == null) {
      // Schema metadata aspect MUST be present to proceed.
      return TestQueryResponse.empty();
    }

    final List<String> results = new ArrayList<>();

    // Case 1: Schema Field Length Query
    if (SCHEMA_FIELDS_LENGTH_PROPERTY.equals(query.getQuery())) {
      results.add(String.valueOf(schemaMetadata.getFields().size()));
    }

    // Case 2: Schema Field Query
    if (SCHEMA_FIELDS_PROPERTY.equals(query.getQuery())) {
      results.addAll(
          schemaMetadata.getFields().stream()
              .map(field -> buildSerializedSchemaField(field, editableSchemaMetadata))
              .collect(Collectors.toList()));
    }

    return new TestQueryResponse(results);
  }

  @Nullable
  private SchemaMetadata extractSchemaMetadata(@Nullable final EntityResponse entityResponse) {
    if (entityResponse != null
        && entityResponse.getAspects().containsKey(Constants.SCHEMA_METADATA_ASPECT_NAME)) {
      return new SchemaMetadata(
          entityResponse.getAspects().get(Constants.SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  @Nullable
  private EditableSchemaMetadata extractEditableSchemaMetadata(
      @Nullable final EntityResponse entityResponse) {
    if (entityResponse != null
        && entityResponse
            .getAspects()
            .containsKey(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME)) {
      return new EditableSchemaMetadata(
          entityResponse
              .getAspects()
              .get(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME)
              .getValue()
              .data());
    }
    return null;
  }

  @Nullable
  private String getEditableDescription(
      @Nonnull final String fieldPath,
      @Nullable final EditableSchemaMetadata editableSchemaMetadata) {
    if (editableSchemaMetadata == null) {
      return null;
    }
    return editableSchemaMetadata.getEditableSchemaFieldInfo().stream()
        .filter(
            field ->
                field.getFieldPath().equals(fieldPath)
                    && field.getDescription(GetMode.NULL) != null)
        .map(EditableSchemaFieldInfo::getDescription)
        .findFirst()
        .orElse(null);
  }

  @Nonnull
  private String buildSerializedSchemaField(
      @Nonnull final SchemaField field,
      @Nullable final EditableSchemaMetadata editableSchemaMetadata) {
    return SchemaFieldUtils.serializeSchemaField(
        new com.linkedin.metadata.test.query.schemafield.SchemaField(
            field.getFieldPath(),
            field.getDescription(),
            getEditableDescription(field.getFieldPath(), editableSchemaMetadata)));
  }
}
