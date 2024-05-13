package com.linkedin.metadata.test.query;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Evaluator that supports resolving `structuredProperties.urn:li:structuredProperty:test` queries,
 * e.g. those which are defined against structured property values for an entity.
 *
 * <p>Note that the ONLY supports structured properties for the immediate entity, and not related
 * entities. For example, we do not support predicates on the structured properties attached to
 * related domains, glossary terms, containers, and more (yet)
 *
 * <p>Also notice that you MUST reference the property by full urn, using the "structuredProperties"
 * prefix: structuredProperties.urn:li:structuredProperty:test
 */
@Slf4j
@RequiredArgsConstructor
public class StructuredPropertyEvaluator extends BaseQueryEvaluator {

  private final EntityService<?> entityService;

  @Override
  public boolean isEligible(@Nonnull final String entityType, @Nonnull final TestQuery query) {
    return structuredPropertyCheck(query);
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
            ImmutableSet.of(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME);

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
                            buildQueryResponse(query, extractStructuredProperties(response)));
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
      @Nonnull final TestQuery query, @Nullable final StructuredProperties structuredProperties) {
    if (structuredProperties == null) {
      return TestQueryResponse.empty();
    }
    // Remove the "structuredProperties." prefix to get a reference to a specific property.
    final String propertyReference = query.getQuery().replaceFirst("^structuredProperties\\.", "");
    final Urn propertyUrn =
        propertyReference.startsWith(
                String.format("urn:li:%s:", Constants.STRUCTURED_PROPERTY_ENTITY_NAME))
            ? UrnUtils.getUrn(propertyReference)
            : UrnUtils.getUrn(
                String.format(
                    "urn:li:%s:%s", Constants.STRUCTURED_PROPERTY_ENTITY_NAME, propertyReference));
    final List<String> values = new ArrayList<>();
    // Attempt to extract the string-ified version of the property that can be used for comparison.
    for (StructuredPropertyValueAssignment prop : structuredProperties.getProperties()) {
      if (prop.getPropertyUrn().equals(propertyUrn)) {
        // Found the property!
        for (PrimitivePropertyValue value : prop.getValues()) {
          if (value.isDouble()) {
            values.add(value.getDouble().toString());
          } else if (value.isString()) {
            values.add(value.getString());
          } else {
            log.warn(
                String.format(
                    "Test query matched unsupported structured property type %s",
                    value.memberType().getType().toString()));
            // Simply continue.
          }
        }
      }
    }
    return new TestQueryResponse(values);
  }

  @Nullable
  private StructuredProperties extractStructuredProperties(
      @Nullable final EntityResponse entityResponse) {
    if (entityResponse != null
        && entityResponse.getAspects().containsKey(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME)) {
      return new StructuredProperties(
          entityResponse
              .getAspects()
              .get(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME)
              .getValue()
              .data());
    }
    return null;
  }

  static boolean structuredPropertyCheck(@Nonnull final TestQuery query) {
    if (query.getQueryParts().size() > 0) {
      return query.getQuery().matches("^structuredProperties\\.urn:li:structuredProperty:.+$");
    }
    return false;
  }
}
